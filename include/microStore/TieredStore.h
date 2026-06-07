/*
 * Copyright (c) 2026 Chad Attermann
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

#pragma once

#include "HeapStore.h"
#include "FileStore.h"
#include "FileSystem.h"

#include <vector>
#include <cstdint>
#include <memory>
#include <utility>

// Max writes buffered while the persist tier compacts before the buffer is
// drained synchronously (safety valve). Self-driving put()s keep the live buffer
// near records/USTORE_COMPACT_STEP_RECORDS, so this cap is rarely approached.
#ifndef USTORE_TIERED_PENDING_MAX
#define USTORE_TIERED_PENDING_MAX 256
#endif

namespace microStore {

// Two-tier key/value store: a BasicHeapStore "front" (always present, in
// RAM/PSRAM — absorbs every read and all high-frequency churn) backed by an
// optional BasicFileStore "persist" tier (log-structured, per-record) for
// durability across reboots.
//
//   get()       -> front only (fast; the hot path never touches flash).
//   put()       -> front always; persist tier too when enabled. Use for real
//                  content changes.
//   put_front() -> front only, never the persist tier. Use for volatile
//                  touches (TTL / last-used refresh) so the durable tier only
//                  sees genuine changes — keeps the write rate low and avoids
//                  compaction churn.
//   remove()    -> both tiers.
//   init()      -> brings up the front (always); if persist_enabled, brings up
//                  the file tier on the *injected* filesystem (internal flash,
//                  SD, ...) and bulk-loads it into the front.
//
// Drop-in for the Store template parameter of TypedStore: it exposes the same
// std::vector<uint8_t> put/get/remove/exists/size + a begin()/end() iterator
// (delegated to the front) whose *it yields {key, value}.
template <typename Allocator = std::allocator<uint8_t>>
class BasicTieredStore
{
    using HeapStoreT = BasicHeapStore<Allocator>;
    using FileStoreT = BasicFileStore<Allocator>;

public:

    BasicTieredStore(uint32_t segment_size = USTORE_DEFAULT_SEGMENT_SIZE,
                     uint8_t  segment_count = USTORE_DEFAULT_SEGMENT_COUNT)
        : _persist(segment_size, segment_count) {}

    explicit BasicTieredStore(const Allocator& alloc,
                              uint32_t segment_size = USTORE_DEFAULT_SEGMENT_SIZE,
                              uint8_t  segment_count = USTORE_DEFAULT_SEGMENT_COUNT)
        : _front(alloc), _persist(alloc, segment_size, segment_count) {}

    // Bring up the front (always) and, if persist_enabled, the file tier on the
    // injected filesystem, then bulk-load the file tier into the front. If the
    // filesystem is unavailable the store degrades to front-only (no durability)
    // rather than failing — callers stay functional with an empty cold cache.
    bool init(FileSystem& filesystem, const char* prefix, bool persist_enabled,
              bool clearOnInit = false, uint32_t segment_size = 0, uint8_t segment_count = 0)
    {
        _front.init(clearOnInit);
        _persist_enabled = persist_enabled;
        if (!_persist_enabled) return true;

        // Re-apply any eviction policy set before init(): set_max_recs/set_ttl_secs
        // are commonly called at construction (before the persist tier exists), so
        // they only reached the front. Apply them to the persist tier BEFORE its
        // init() so the boot-time prune bounds it too — otherwise the front caps
        // but the flash tier grows unbounded.
        _persist.set_max_recs(_policy_max_recs);
        _persist.set_ttl_secs(_policy_ttl);

        if (!_persist.init(filesystem, prefix, clearOnInit, segment_size, segment_count)) {
            _persist_enabled = false;   // degrade to front-only
            return true;
        }
        // Drive the persist tier's compaction incrementally: it begins on a
        // threshold and we pump it a few records at a time (put()/compact_step()),
        // buffering writes meanwhile, instead of taking a synchronous O(records)
        // freeze on the loop. The front keeps serving reads throughout.
        _persist.set_incremental(true);

        // Warm the front from the durable tier (front-only puts: no re-persist).
        for (auto it = _persist.begin(); it != _persist.end(); ++it) {
            const auto& e = *it;   // operator* lazy-loads the value
            _front.put(e.key.data(), (uint8_t)e.key.size(),
                       e.value.data(), (uint16_t)e.value.size(), e.ttl, e.timestamp);
        }
        return true;
    }

    inline bool isValid() const { return true; }   // the front is always valid
    inline operator bool() const { return true; }
    inline bool persist_enabled() const { return _persist_enabled; }

    /* -------- PUT (front + persist) -------- */

    bool put(const uint8_t* key, uint8_t key_len, const void* data, uint16_t len,
             uint32_t ttl = 0, uint32_t ts = microStore::time())
    {
        _front.put(key, key_len, data, len, ttl, ts);
        if (_persist_enabled) {
            if (_persist.compacting())
                // Active segment is closed while compacting — buffer and replay
                // when it finishes (the front already holds the latest value).
                buffer_op_(false, key, key_len, static_cast<const uint8_t*>(data), len, ttl, ts);
            else
                _persist.put(key, key_len, static_cast<const uint8_t*>(data), len, ttl, ts);
            drive_compaction_();
        }
        return true;
    }
    inline bool put(const std::vector<uint8_t>& key, const std::vector<uint8_t>& data,
                    uint32_t ttl = 0, uint32_t ts = microStore::time())
    {
        return put(key.data(), (uint8_t)key.size(), data.data(), (uint16_t)data.size(), ttl, ts);
    }

    /* -------- PUT_FRONT (front only — volatile touch) -------- */

    bool put_front(const uint8_t* key, uint8_t key_len, const void* data, uint16_t len,
                   uint32_t ttl = 0, uint32_t ts = microStore::time())
    {
        return _front.put(key, key_len, data, len, ttl, ts);
    }
    inline bool put_front(const std::vector<uint8_t>& key, const std::vector<uint8_t>& data,
                          uint32_t ttl = 0, uint32_t ts = microStore::time())
    {
        return _front.put(key.data(), (uint8_t)key.size(), data.data(), (uint16_t)data.size(), ttl, ts);
    }

    /* -------- GET (front only) -------- */

    bool get(const uint8_t* key, uint8_t key_len, void* out, uint16_t* size)
    {
        return _front.get(key, key_len, out, size);
    }
    inline bool get(const std::vector<uint8_t>& key, std::vector<uint8_t>& out)
    {
        return _front.get(key, out);
    }

    /* -------- REMOVE (both tiers) -------- */

    bool remove(const uint8_t* key, uint8_t key_len)
    {
        bool r = _front.remove(key, key_len);
        if (_persist_enabled) {
            if (_persist.compacting())
                buffer_op_(true, key, key_len, nullptr, 0, 0, 0);
            else
                _persist.remove(key, key_len);
            drive_compaction_();
        }
        return r;
    }
    inline bool remove(const std::vector<uint8_t>& key)
    {
        return remove(key.data(), (uint8_t)key.size());
    }

    /* -------- EXISTS / SIZE / CLEAR / POLICY (front authoritative) -------- */

    bool exists(const uint8_t* key, uint8_t key_len) { return _front.exists(key, key_len); }
    inline bool exists(const std::vector<uint8_t>& key) { return _front.exists(key); }

    inline size_t size() { return _front.size(); }

    void clear() { _front.clear(); if (_persist_enabled) { _persist.clear(); _pending.clear(); } }

    /* -------- INCREMENTAL COMPACTION DRIVE (host loop nudge) -------- */

    // Is the persist tier mid-compaction (writes are being buffered)?
    inline bool compacting() { return _persist_enabled && _persist.compacting(); }

    // Advance an in-flight persist-tier compaction by one slice and replay the
    // write buffer if it finishes. put()/remove() already self-drive under load;
    // call this from the host loop so a compaction still converges when the write
    // feed goes quiet. No-op when nothing is compacting.
    inline void compact_step() { if (_persist_enabled) drive_compaction_(); }

    // Diagnostic dump — only the persist tier has an on-disk layout to describe.
    void dumpInfo(bool detailed = true) { if (_persist_enabled) _persist.dumpInfo(detailed); }

    // The policy is remembered so init() can apply it to the persist tier even
    // when it was set before that tier was brought up (the usual case).
    inline void set_ttl_secs(uint32_t ttl_s)  { _policy_ttl = ttl_s; _front.set_ttl_secs(ttl_s);  if (_persist_enabled) _persist.set_ttl_secs(ttl_s); }
    inline void set_max_recs(uint32_t max_recs){ _policy_max_recs = max_recs; _front.set_max_recs(max_recs); if (_persist_enabled) _persist.set_max_recs(max_recs); }

    /* -------- ITERATION (delegated to the front) -------- */

    using iterator = typename HeapStoreT::iterator;
    iterator begin() { return _front.begin(); }
    iterator end()   { return _front.end(); }

    /* -------- STATS (front live picture + persist write counters) -------- */

    struct Stats {
        uint32_t puts;                // persist-tier successful puts
        uint32_t removes;             // persist-tier removes
        uint32_t compacts;            // persist-tier compactions
        uint64_t bytes_written;       // persist-tier payload bytes
        uint32_t live_recs;           // front live record count
        uint32_t persist_recs;        // persist-tier live record count (durable set)
        uint32_t dead_since_compact;  // persist-tier dead records since compaction
        uint32_t put_fails;           // persist-tier put() rejects (size/flush)
        uint32_t pending;             // writes buffered during an in-flight compaction
        bool     persisted;           // is the durable tier active?
    };
    Stats stats() {
        Stats s{};
        s.live_recs = (uint32_t)_front.size();
        s.persisted = _persist_enabled;
        s.pending   = (uint32_t)_pending.size();
        if (_persist_enabled) {
            auto ps = _persist.stats();
            s.puts               = ps.puts;
            s.removes            = ps.removes;
            s.compacts           = ps.compacts;
            s.bytes_written      = ps.bytes_written;
            s.persist_recs       = ps.live_recs;
            s.dead_since_compact = ps.dead_since_compact;
            s.put_fails          = ps.put_fails;
        }
        return s;
    }

private:

    /* -------- WRITE BUFFER (replayed after a compaction finishes) -------- */

    // While the persist tier compacts, its active segment is closed, so put()s
    // and remove()s can't reach it. We record them here (the front already has
    // the value, so reads are correct) and replay them in order once compaction
    // completes. Buffer storage uses the store's own Allocator (PSRAM for paths),
    // keeping it off internal SRAM.
    template<typename T>
    using rebind_alloc_ = typename std::allocator_traits<Allocator>::template rebind_alloc<T>;
    using ByteVec = std::vector<uint8_t, rebind_alloc_<uint8_t>>;
    struct PendingOp {
        bool     is_remove;
        uint32_t ttl;
        uint32_t ts;
        ByteVec  key;
        ByteVec  value;
    };

    void buffer_op_(bool is_remove, const uint8_t* key, uint8_t key_len,
                    const uint8_t* data, uint16_t len, uint32_t ttl, uint32_t ts)
    {
        PendingOp op;
        op.is_remove = is_remove;
        op.ttl = ttl;
        op.ts  = ts;
        op.key.assign(key, key + key_len);
        if (!is_remove && len > 0) op.value.assign(data, data + len);
        _pending.push_back(std::move(op));

        // Safety valve: should the buffer outgrow its cap (compaction making no
        // progress — e.g. a quiet pump loop), finish it synchronously so memory
        // stays bounded. Self-driving puts keep the buffer near R/step, so this
        // only fires in pathological cases.
        if (_pending.size() > USTORE_TIERED_PENDING_MAX) {
            while (_persist.compacting()) _persist.compact_step(SIZE_MAX);
            flush_pending_();
        }
    }

    // Pump the persist-tier compaction one slice; when it finishes, replay the
    // buffered writes into the now-open store.
    void drive_compaction_()
    {
        if (!_persist.compacting()) return;
        _persist.compact_step(USTORE_COMPACT_STEP_RECORDS);
        if (!_persist.compacting()) flush_pending_();
    }

    void flush_pending_()
    {
        size_t i = 0;
        for (; i < _pending.size(); i++) {
            if (_persist.compacting()) break;   // a replayed put re-triggered compaction
            PendingOp& op = _pending[i];
            if (op.is_remove)
                _persist.remove(op.key.data(), (uint8_t)op.key.size());
            else
                _persist.put(op.key.data(), (uint8_t)op.key.size(),
                             op.value.data(), (uint16_t)op.value.size(), op.ttl, op.ts);
        }
        _pending.erase(_pending.begin(), _pending.begin() + (long)i);
    }

    HeapStoreT _front;
    FileStoreT _persist;
    bool       _persist_enabled = false;
    uint32_t   _policy_max_recs = 0;   // remembered so init() applies it to the persist tier
    uint32_t   _policy_ttl      = 0;
    std::vector<PendingOp, rebind_alloc_<PendingOp>> _pending;
};

using TieredStore = BasicTieredStore<>;

} // namespace microStore
