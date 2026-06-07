/*
 * Tests for BasicTieredStore — the PSRAM-front + optional flash-persist store,
 * focused on the incremental (resumable) compaction path that the synchronous
 * FileStore tests don't exercise:
 *
 *   - heavy overwrite churn triggers an incremental compaction (begin-on-
 *     threshold, pumped a few records per put) and the store stays correct;
 *   - writes issued WHILE the persist tier is compacting are buffered (the
 *     active segment is closed) and replayed on completion — visible in the
 *     front immediately, durable after a reboot;
 *   - the host-loop nudge (compact_step()) converges a compaction when the
 *     write feed goes quiet;
 *   - everything survives a reinit ("reboot") from the same filesystem.
 *
 * A small per-slice budget makes a compaction span many slices so the
 * incremental machinery is genuinely interleaved with writes.
 */

#define USTORE_COMPACT_STEP_RECORDS 2

#include <unity.h>
#include <microStore/TieredStore.h>

#include <cstring>
#include <cstdio>
#include <string>
#include <vector>

/* ---- Minimal RAM filesystem (shared shape with test_file_store) ---- */

struct RamFile {
    std::vector<uint8_t> data;
    size_t pos = 0;
    bool open = false;
    bool write_mode = false;
    bool append_mode = false;
};

static RamFile g_files[16];
static char    g_names[16][64];
static int     g_nfiles = 0;

static int find_file(const char* name) {
    for (int i = 0; i < g_nfiles; i++)
        if (g_names[i][0] != '\0' && strcmp(g_names[i], name) == 0) return i;
    return -1;
}
static int alloc_slot() {
    for (int i = 0; i < g_nfiles; i++)
        if (g_names[i][0] == '\0') return i;
    if (g_nfiles >= 16) return -1;
    return g_nfiles++;
}

class RamFileImpl : public microStore::FileImpl {
    int _idx;
    bool _closed = false;
public:
    RamFileImpl(int idx) : microStore::FileImpl(), _idx(idx) {}
    virtual ~RamFileImpl() { if (!_closed) close(); }
protected:
    virtual const char* name()  const override { return g_names[_idx]; }
    virtual size_t      size()  const override { return g_files[_idx].data.size(); }
    virtual void close() override { g_files[_idx].open = false; _closed = true; }
    virtual int read() override {
        RamFile& f = g_files[_idx];
        if (f.pos >= f.data.size()) return EOF;
        return f.data[f.pos++];
    }
    virtual size_t write(uint8_t ch) override {
        RamFile& f = g_files[_idx];
        if (f.append_mode) f.pos = f.data.size();
        if (f.pos >= f.data.size()) f.data.resize(f.pos + 1);
        f.data[f.pos++] = ch;
        return 1;
    }
    virtual size_t read(uint8_t* buffer, size_t size) override {
        RamFile& f = g_files[_idx];
        size_t avail = f.data.size() - f.pos;
        size_t n = (size < avail) ? size : avail;
        memcpy(buffer, f.data.data() + f.pos, n);
        f.pos += n;
        return n;
    }
    virtual size_t write(const uint8_t* buffer, size_t size) override {
        RamFile& f = g_files[_idx];
        if (f.append_mode) f.pos = f.data.size();
        size_t need = f.pos + size;
        if (f.data.size() < need) f.data.resize(need);
        memcpy(f.data.data() + f.pos, buffer, size);
        f.pos += size;
        return size;
    }
    virtual int  available() override { return (int)(g_files[_idx].data.size() - g_files[_idx].pos); }
    virtual int  peek()      override { RamFile& f=g_files[_idx]; return (f.pos<f.data.size())?f.data[f.pos]:EOF; }
    virtual size_t tell()    override { return g_files[_idx].pos; }
    virtual long seek(uint32_t pos, microStore::SeekMode mode) override {
        RamFile& f = g_files[_idx];
        size_t new_pos;
        switch (mode) {
            case microStore::SeekModeEnd: new_pos = (size_t)((long)f.data.size() + (long)pos); break;
            case microStore::SeekModeCur: new_pos = (size_t)((long)f.pos + (long)pos); break;
            default:                      new_pos = (size_t)pos; break;
        }
        f.pos = new_pos;
        return (long)new_pos;
    }
    virtual void flush() override {}
    virtual bool isValid() const override { return !_closed; }
};

class RamFileSystemImpl : public microStore::FileSystemImpl {
protected:
    virtual microStore::File open(const char* path, microStore::File::Mode mode, const bool create = false) override {
        bool wr = (mode == microStore::File::ModeWrite || mode == microStore::File::ModeReadWrite);
        bool ap = (mode == microStore::File::ModeAppend || mode == microStore::File::ModeReadAppend);
        int idx = find_file(path);
        if (wr) {
            if (idx < 0) { idx = alloc_slot(); if (idx < 0) return {}; strncpy(g_names[idx], path, 63); g_names[idx][63] = '\0'; }
            g_files[idx].data.clear(); g_files[idx].pos = 0; g_files[idx].open = true;
            g_files[idx].write_mode = true; g_files[idx].append_mode = false;
        } else if (ap) {
            if (idx < 0) {
                idx = alloc_slot(); if (idx < 0) return {};
                strncpy(g_names[idx], path, 63); g_names[idx][63] = '\0';
                g_files[idx].data.clear(); g_files[idx].pos = 0;
            }
            g_files[idx].open = true; g_files[idx].write_mode = true; g_files[idx].append_mode = true;
            g_files[idx].pos = g_files[idx].data.size();
        } else {
            if (idx < 0) return {};
            g_files[idx].pos = 0; g_files[idx].open = true;
            g_files[idx].write_mode = false; g_files[idx].append_mode = false;
        }
        (void)create;
        return microStore::File(new RamFileImpl(idx));
    }
    virtual bool exists(const char* path) override { return find_file(path) >= 0; }
    virtual bool remove(const char* path) override {
        int idx = find_file(path);
        if (idx < 0) return false;
        g_files[idx].data.clear(); g_files[idx].pos = 0;
        g_files[idx].open = false; g_files[idx].write_mode = false; g_files[idx].append_mode = false;
        g_names[idx][0] = '\0';
        while (g_nfiles > 0 && g_names[g_nfiles - 1][0] == '\0') g_nfiles--;
        return true;
    }
    virtual bool rename(const char* src, const char* dst) override {
        int si = find_file(src); if (si < 0) return false;
        int di = find_file(dst);
        if (di >= 0) {
            g_files[di].data.clear(); g_files[di].pos = 0;
            g_files[di].open = false; g_files[di].write_mode = false; g_files[di].append_mode = false;
            g_names[di][0] = '\0';
            while (g_nfiles > 0 && g_names[g_nfiles - 1][0] == '\0') g_nfiles--;
        }
        strncpy(g_names[si], dst, 63); g_names[si][63] = '\0';
        return true;
    }
    virtual bool mkdir(const char* path) override { (void)path; return true; }
    virtual bool rmdir(const char* path) override { (void)path; return true; }
    virtual bool isDirectory(const char* path) override { (void)path; return false; }
    virtual std::list<std::string> listDirectory(const char* path, Callbacks::DirectoryListing cb = nullptr) override {
        (void)path; (void)cb; return {};
    }
    virtual size_t storageSize()      override { return 0; }
    virtual size_t storageAvailable() override { return 0; }
};

static void reset_ram_fs() {
    for (int i = 0; i < 16; i++) {
        g_files[i].data.clear(); g_files[i].pos = 0;
        g_files[i].open = false; g_files[i].write_mode = false; g_files[i].append_mode = false;
        g_names[i][0] = '\0';
    }
    g_nfiles = 0;
}
static microStore::FileSystem make_ram_fs() { return microStore::FileSystem{new RamFileSystemImpl()}; }

/* ---- Key/value helpers ---- */

static std::vector<uint8_t> K(int i) {
    char b[16]; snprintf(b, sizeof(b), "k%03d", i);
    return std::vector<uint8_t>((uint8_t*)b, (uint8_t*)b + strlen(b));
}
static std::vector<uint8_t> V(int i, int round) {
    char b[24]; snprintf(b, sizeof(b), "v%03d_%03d", i, round);
    return std::vector<uint8_t>((uint8_t*)b, (uint8_t*)b + strlen(b));
}
static bool veq(const std::vector<uint8_t>& a, const std::vector<uint8_t>& b) {
    return a.size() == b.size() && (a.empty() || memcmp(a.data(), b.data(), a.size()) == 0);
}

// Assert every key 0..n_keys-1 reads back its expected round-value through a store.
static void assert_all(microStore::TieredStore& s, int n_keys, const std::vector<int>& round) {
    for (int i = 0; i < n_keys; i++) {
        std::vector<uint8_t> out;
        TEST_ASSERT_TRUE_MESSAGE(s.get(K(i), out), "key missing");
        TEST_ASSERT_TRUE_MESSAGE(veq(out, V(i, round[i])), "stale/wrong value");
    }
}

/* ===========================================================================
 * Tests
 * =========================================================================== */

// Heavy overwrite churn must trigger at least one incremental compaction and
// leave every live key correct — both live (front) and after a reboot (persist).
void test_tiered_incremental_churn_integrity() {
    reset_ram_fs();
    const int N = 8;

    microStore::TieredStore store(2048, 4);
    auto fs = make_ram_fs();
    TEST_ASSERT_TRUE(store.init(fs, "/p", /*persist=*/true, /*clear=*/true));

    std::vector<int> round(N, 0);
    for (int i = 0; i < N; i++) store.put(K(i), V(i, 0));

    // Overwrite the whole working set many times. Each overwrite makes the prior
    // record dead; once dead% crosses the threshold an incremental compaction
    // begins and is pumped by these very puts.
    bool saw_compacting = false;
    for (int r = 1; r <= 60; r++) {
        for (int i = 0; i < N; i++) {
            store.put(K(i), V(i, r));
            round[i] = r;
            if (store.compacting()) saw_compacting = true;
        }
    }
    // Make sure no compaction is left half-done.
    while (store.compacting()) store.compact_step();

    TEST_ASSERT_TRUE_MESSAGE(saw_compacting, "expected an incremental compaction to run");
    TEST_ASSERT_TRUE_MESSAGE(store.stats().compacts >= 1, "expected >=1 completed compaction");
    TEST_ASSERT_EQUAL_UINT32(N, store.size());
    assert_all(store, N, round);

    // Reboot: a fresh store warmed from the same filesystem must see the latest
    // value for every key (durability through the compacted persist tier).
    microStore::TieredStore reboot(2048, 4);
    auto fs2 = make_ram_fs();
    TEST_ASSERT_TRUE(reboot.init(fs2, "/p", /*persist=*/true, /*clear=*/false));
    TEST_ASSERT_EQUAL_UINT32(N, reboot.size());
    assert_all(reboot, N, round);
}

// Writes issued while the persist tier is compacting must be buffered (the
// active segment is closed) and replayed on completion: visible in the front
// right away, durable after a reboot.
void test_tiered_buffer_during_compaction() {
    reset_ram_fs();
    const int BASE = 6;

    microStore::TieredStore store(2048, 4);
    auto fs = make_ram_fs();
    TEST_ASSERT_TRUE(store.init(fs, "/p", /*persist=*/true, /*clear=*/true));

    std::vector<int> round(BASE, 0);
    for (int i = 0; i < BASE; i++) store.put(K(i), V(i, 0));

    // Drive churn until a compaction is in flight, then STOP churning.
    int r = 0;
    while (!store.compacting() && r < 200) {
        r++;
        for (int i = 0; i < BASE; i++) { store.put(K(i), V(i, r)); round[i] = r; }
    }
    TEST_ASSERT_TRUE_MESSAGE(store.compacting(), "could not get the store into a compaction");

    // While compacting, insert brand-new keys. Each is visible in the front
    // immediately even though the persist tier's active segment is closed.
    const int EXTRA = 5;
    int inserted = 0;
    for (int j = 0; j < EXTRA && store.compacting(); j++) {
        int key = 100 + j;
        store.put(K(key), V(key, 1));
        std::vector<uint8_t> out;
        TEST_ASSERT_TRUE_MESSAGE(store.get(K(key), out), "buffered key not visible in front");
        TEST_ASSERT_TRUE(veq(out, V(key, 1)));
        inserted++;
    }
    TEST_ASSERT_TRUE_MESSAGE(inserted > 0, "compaction finished too fast to test buffering");

    // Converge any remaining compaction via the quiet-feed nudge.
    while (store.compacting()) store.compact_step();

    // Reboot and confirm the buffered inserts were replayed into the persist tier.
    microStore::TieredStore reboot(2048, 4);
    auto fs2 = make_ram_fs();
    TEST_ASSERT_TRUE(reboot.init(fs2, "/p", /*persist=*/true, /*clear=*/false));
    for (int i = 0; i < BASE; i++) {
        std::vector<uint8_t> out;
        TEST_ASSERT_TRUE(reboot.get(K(i), out));
        TEST_ASSERT_TRUE(veq(out, V(i, round[i])));
    }
    for (int j = 0; j < inserted; j++) {
        int key = 100 + j;
        std::vector<uint8_t> out;
        TEST_ASSERT_TRUE_MESSAGE(reboot.get(K(key), out), "buffered insert not durable");
        TEST_ASSERT_TRUE(veq(out, V(key, 1)));
    }
}

// A remove issued during a compaction must be buffered and applied: gone from
// the front immediately and absent after a reboot.
void test_tiered_remove_during_compaction() {
    reset_ram_fs();
    const int BASE = 6;

    microStore::TieredStore store(2048, 4);
    auto fs = make_ram_fs();
    TEST_ASSERT_TRUE(store.init(fs, "/p", /*persist=*/true, /*clear=*/true));

    std::vector<int> round(BASE, 0);
    for (int i = 0; i < BASE; i++) store.put(K(i), V(i, 0));

    int r = 0;
    while (!store.compacting() && r < 200) {
        r++;
        for (int i = 0; i < BASE; i++) { store.put(K(i), V(i, r)); round[i] = r; }
    }
    TEST_ASSERT_TRUE_MESSAGE(store.compacting(), "could not get the store into a compaction");

    store.remove(K(0));
    std::vector<uint8_t> out;
    TEST_ASSERT_FALSE_MESSAGE(store.get(K(0), out), "removed key still in front");

    while (store.compacting()) store.compact_step();

    microStore::TieredStore reboot(2048, 4);
    auto fs2 = make_ram_fs();
    TEST_ASSERT_TRUE(reboot.init(fs2, "/p", /*persist=*/true, /*clear=*/false));
    TEST_ASSERT_FALSE_MESSAGE(reboot.get(K(0), out), "removed key resurrected after reboot");
    for (int i = 1; i < BASE; i++) {
        std::vector<uint8_t> v;
        TEST_ASSERT_TRUE(reboot.get(K(i), v));
        TEST_ASSERT_TRUE(veq(v, V(i, round[i])));
    }
}

// With persistence disabled the store is front-only: writes never compact, and
// nothing survives a reboot. (Guards the degrade-to-front-only path.)
void test_tiered_front_only_no_persist() {
    reset_ram_fs();
    const int N = 8;

    microStore::TieredStore store(2048, 4);
    auto fs = make_ram_fs();
    TEST_ASSERT_TRUE(store.init(fs, "/p", /*persist=*/false, /*clear=*/true));
    TEST_ASSERT_FALSE(store.persist_enabled());

    std::vector<int> round(N, 0);
    for (int r = 0; r <= 40; r++)
        for (int i = 0; i < N; i++) { store.put(K(i), V(i, r)); round[i] = r; }

    TEST_ASSERT_FALSE(store.compacting());
    TEST_ASSERT_EQUAL_UINT32(N, store.size());
    assert_all(store, N, round);

    microStore::TieredStore reboot(2048, 4);
    auto fs2 = make_ram_fs();
    TEST_ASSERT_TRUE(reboot.init(fs2, "/p", /*persist=*/false, /*clear=*/false));
    TEST_ASSERT_EQUAL_UINT32(0u, reboot.size());   // front-only: cold after reboot
}

// max_recs set BEFORE init() (the usual case — callers configure the cap at
// construction, before the persist tier exists) must still bound the persist
// tier, not just the front. Regression for the path-store bug where the front
// capped at 500 but the flash tier grew unbounded.
void test_tiered_max_recs_bounds_persist() {
    reset_ram_fs();
    const uint32_t CAP = 20;

    microStore::TieredStore store(2048, 4);
    store.set_max_recs(CAP);                 // BEFORE init
    auto fs = make_ram_fs();
    TEST_ASSERT_TRUE(store.init(fs, "/p", /*persist=*/true, /*clear=*/true));

    for (int i = 0; i < 100; i++) store.put(K(i), V(i, 0));
    while (store.compacting()) store.compact_step();

    auto s = store.stats();
    TEST_ASSERT_TRUE_MESSAGE(s.live_recs    <= CAP, "front exceeded max_recs");
    TEST_ASSERT_TRUE_MESSAGE(s.persist_recs <= CAP, "persist tier exceeded max_recs");

    // And the bound holds across a reboot (the boot-time prune also sees it).
    microStore::TieredStore reboot(2048, 4);
    reboot.set_max_recs(CAP);
    auto fs2 = make_ram_fs();
    TEST_ASSERT_TRUE(reboot.init(fs2, "/p", /*persist=*/true, /*clear=*/false));
    TEST_ASSERT_TRUE_MESSAGE(reboot.stats().persist_recs <= CAP, "persist tier exceeded max_recs after reboot");
}

/* ---- Main ---- */

void setUp()    {}
void tearDown() {}

int main() {
    UNITY_BEGIN();
    RUN_TEST(test_tiered_incremental_churn_integrity);
    RUN_TEST(test_tiered_buffer_during_compaction);
    RUN_TEST(test_tiered_remove_during_compaction);
    RUN_TEST(test_tiered_front_only_no_persist);
    RUN_TEST(test_tiered_max_recs_bounds_persist);
    return UNITY_END();
}
