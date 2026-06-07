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

#include "File.h"
#include "FileSystem.h"
#include "Utility.h"

#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <stdio.h>
#include <algorithm>
#include <vector>
#include <unordered_map>

namespace microStore {

/* ---------------- CONFIG ---------------- */

// Per-operation trace logging (get/put/compact/rotate, with key hashes and
// sizes). Investigation-only and very high volume under a heavy feed — at
// 115200 baud with a serial monitor attached it can back up the USB-CDC
// write and stall the main loop. Compiled out unless the build defines
// USTORE_VERBOSE_LOG.
#if defined(USTORE_VERBOSE_LOG)
#define USTORE_LOG(...) printf(__VA_ARGS__)
#else
#define USTORE_LOG(...) ((void)0)
#endif

#ifndef USTORE_DEFAULT_SEGMENT_COUNT
#define USTORE_DEFAULT_SEGMENT_COUNT 8
#endif

#ifndef USTORE_DEFAULT_SEGMENT_SIZE
#define USTORE_DEFAULT_SEGMENT_SIZE 65536
#endif

#ifndef USTORE_WRITE_BUFFER_SIZE
//#define USTORE_WRITE_BUFFER_SIZE 4096
#define USTORE_WRITE_BUFFER_SIZE 512
#endif

#ifndef USTORE_MAX_KEY_LEN
#define USTORE_MAX_KEY_LEN 64
#endif

#ifndef USTORE_MAX_VALUE_LEN
#define USTORE_MAX_VALUE_LEN 1024
#endif

#ifndef USTORE_MAX_FILENAME_LEN
#define USTORE_MAX_FILENAME_LEN 64
#endif

#ifndef USTORE_COMPACT_RETRY_MS
//#define USTORE_COMPACT_RETRY_MS 5000   // ms between compact() retries after failure
#define USTORE_COMPACT_RETRY_MS 60000   // ms between compact() retries after failure
#endif

#ifndef USTORE_COMPACT_THRESHOLD
// % dead records (relative to total live + dead since last compact) that
// triggers auto-compact. 0 = auto-compact disabled.
//
// 80% (was 25%) chosen after the previous compaction fix landed: with
// 25% the auto-compact path fired on essentially every announce churn
// pair (2 live + 1 new dead = 33% → compact), and each compact()
// invocation walks every segment + rebuilds the index synchronously
// while holding rns_lock. On the firmware this starved AsyncTCP and
// the radio task, leading to periodic reboots roughly every 60 s of
// announce traffic. 80% lets dead records breathe — compaction runs
// only when there are ~4x more dead than live records, which keeps
// the on-disk footprint reasonable without taking out the system on
// every announce.
#define USTORE_COMPACT_THRESHOLD 80
#endif

#ifndef USTORE_DEFAULT_TTL_SECS
#define USTORE_DEFAULT_TTL_SECS 0
#endif

// Records copied per incremental compaction slice (one compact_step pump). Each
// slice runs under the caller's lock, so this bounds the per-step stall; the
// lock is released between slices, letting the radio/main loop run. Smaller =
// shorter stalls but more pumps (and a larger TieredStore write buffer) to drain
// a compaction. Only used when set_incremental(true).
#ifndef USTORE_COMPACT_STEP_RECORDS
#define USTORE_COMPACT_STEP_RECORDS 16
#endif

#ifndef USTORE_DEFAULT_MAX_RECS
#define USTORE_DEFAULT_MAX_RECS 0
#endif

/* ---------------- CONSTANTS ---------------- */

static const uint32_t MAGIC_RECORD  = 0xC0DEC0DE;
static const uint32_t MAGIC_COMMIT  = 0xFACEB00C;
static const uint16_t FLAG_DELETE   = 1;
static const uint32_t JOURNAL_MAGIC = 0x4B564A4E;

enum JournalState { JOURNAL_NONE = 0, JOURNAL_COMPACTING = 1, JOURNAL_COMMIT = 2 };

#pragma pack(push,1)
struct Journal {
	uint32_t magic;
	uint32_t state;           // JOURNAL_NONE / COMPACTING / COMMIT
	uint32_t next_seg;        // segments 0..next_seg-1 are committed to compact.tmp
	uint32_t tmp_valid_size;  // byte count of compact.tmp that is durably valid
};
#pragma pack(pop)


/* ---------------- RECORD STRUCTURES ---------------- */

#pragma pack(push,1)

struct RecordHeader
{
	uint32_t magic;       // framing sentinel — MUST be first
	uint8_t  flags;       // record type (normal/delete) — affects how the rest of the header is interpreted
	uint8_t  key_len;     // length of the key
	uint16_t length;      // lenght of the value
	uint32_t timestamp;   // timestamp at record insertion
	uint32_t ttl;         // per-record TTL in seconds; 0 = use global policy
	uint32_t crc;         // integrity check — MUST be last
};

struct RecordCommit
{
	uint32_t magic;
};

#pragma pack(pop)

/* ---------------- STORAGE ENGINE ---------------- */

template<typename Allocator = std::allocator<uint8_t>>
class BasicFileStore
{
public:

	using allocator_type = Allocator;

	BasicFileStore(uint32_t segment_size = USTORE_DEFAULT_SEGMENT_SIZE, uint8_t segment_count = USTORE_DEFAULT_SEGMENT_COUNT) : BasicFileStore(Allocator{}, segment_size, segment_count) {}
	explicit BasicFileStore(const Allocator& alloc, uint32_t segment_size = USTORE_DEFAULT_SEGMENT_SIZE, uint8_t segment_count = USTORE_DEFAULT_SEGMENT_COUNT)
		: _alloc(alloc), _segment_size(segment_size), _segment_count(segment_count), _index(map_alloc_type(_alloc)) { write_buf_pos = 0; }

	~BasicFileStore()
	{
		if (isValid())
			flush_buffer();
	}

	inline allocator_type get_allocator() const { return _alloc; }

	inline bool isValid() const { if (!_filesystem) return false; return true; }
	inline operator bool() const { return isValid(); }

	bool init(FileSystem& filesystem, const char* prefix, bool clearOnInit = false, uint32_t segment_size = 0, uint8_t segment_count = 0)
	{
		if (segment_size > 0) _segment_size = segment_size;
		if (segment_count > 0) _segment_count = segment_count;
		USTORE_LOG("[ustore] init: Initializing FileStore with prefix=%s, segment_size=%lu, segment_count=%u\n", prefix, _segment_size, _segment_count);

		_filesystem = filesystem;
		strncpy(base_prefix,prefix,sizeof(base_prefix));

		if (clearOnInit) {
			clear();
		}

		recover_if_needed();

		if (!load_index())
			rebuild_index_from_segments();

		// Enforce max_recs at boot: prune excess entries and rewrite the
		// persistent index so evicted keys don't reappear on the next reboot.
		size_t pruned = prune_index_to_max_recs_();
		if (pruned > 0) {
			char iname[USTORE_MAX_FILENAME_LEN]; index_name(iname);
			_filesystem.remove(iname);
			write_index_bulk();
		}

		open_index_for_append();

		// Resume at the highest segment that exists on flash, not always seg 0.
		// Without this, after reset current_segment stays 0 and new writes
		// overwrite previously-live segments, losing all their records.
		uint32_t resume_seg = 0;
		for (uint32_t i = 0; i < _segment_count; i++)
		{
			char name[USTORE_MAX_FILENAME_LEN];
			segment_name(i, name);
			File f = _filesystem.open(name, File::ModeRead);
			if (f) { f.close(); resume_seg = i; }
		}

		open_segment(resume_seg);

		return true;
	}

	void clear()
	{
        if (!isValid()) {
			USTORE_LOG("[ustore] clear: store is invalid, skipping!\n");
			return;
		}

		char name[USTORE_MAX_FILENAME_LEN];

		if (index_file) index_file.close();

		for(uint32_t i = 0; i < _segment_count; i++)
		{
			segment_name(i,name);
			//USTORE_LOG("[ustore] clear: removing segment file: %s\n", name);
			_filesystem.remove(name);
		}

		index_name(name);
		//USTORE_LOG("[ustore] clear: removing index file: %s\n", name);
		_filesystem.remove(name);

		_index.clear();
		_dead_since_compact = 0;
		current_segment=0;
		current_offset=0;
		write_buf_pos=0;

		open_index_for_append();
		open_segment(0);
	}

	/* -------- PUT -------- */

	bool put(const uint8_t* key, uint8_t key_len, const uint8_t* data, uint16_t len, uint32_t ttl = 0, uint32_t ts = microStore::time())
	{
        if (!isValid()) return false;

		if (key_len > USTORE_MAX_KEY_LEN) {
			USTORE_LOG("[ustore] put: failed due to excessive key length: %u\n", key_len);
			_stat_put_fails++;
			return false;
		}
		if (len > USTORE_MAX_VALUE_LEN) {
			USTORE_LOG("[ustore] put: failed due to excessive data length: %u\n", len);
			_stat_put_fails++;
			return false;
		}

		// CBA Don't fail to put just because rotation fails
		//if (!rotate_segment_if_needed(len))
		//    return false;
		rotate_segment_if_needed(len);

		RecordHeader hdr;

		hdr.magic = MAGIC_RECORD;
		hdr.key_len = key_len;
		hdr.timestamp = ts;
		hdr.ttl = ttl;
		hdr.length = len;
		hdr.flags = 0;

		hdr.crc = crc32(0,(uint8_t*)&hdr,sizeof(hdr)-4);
		hdr.crc = crc32(hdr.crc,key,key_len);
		hdr.crc = crc32(hdr.crc,(uint8_t*)data,len);

		uint32_t offset = current_offset;

		append_buffer((uint8_t*)&hdr, sizeof(hdr));
		append_buffer(key, key_len);
		append_buffer(data, len);

		RecordCommit c;
		c.magic = MAGIC_COMMIT;

		append_buffer((uint8_t*)&c, sizeof(c));

		// ensure data is on flash before committing index entry
		// if flush fails then fail put without writing index entry
		if (!flush_buffer()) {
			_stat_put_fails++;
			return false;
		}

		// If an existing record was updated then increment _dead_since_compact
		if (index_find(key, key_len)) _dead_since_compact++;

		index_insert(key, key_len, current_segment, offset, ts, ttl);

		// Enforce max_recs: evict the oldest record(s) from the in-memory index
		// when a new key pushes the count over the limit. Orphaned disk records
		// will be reclaimed at the next compaction.
		if (policy_max_recs > 0 && _index.size() > policy_max_recs)
			prune_index_to_max_recs_();

		persist_index_entry(key, key_len, current_segment, offset, ts, ttl);
USTORE_LOG("[ustore] put: key %s offset %u\n", bin_str(key, key_len), offset);

		current_offset += sizeof(hdr)+key_len+len+sizeof(c);

		_stat_puts++;
		_stat_bytes += len;
		compact_if_threshold();

USTORE_LOG("[ustore] put: wrote key %s with data length %u\n", bin_str(key, key_len), len);
//USTORE_LOG("[ustore] put: %s\n", bin_str((uint8_t*)data, len));
		return true;
	}

	inline bool put(const char* key, const uint8_t* data, uint16_t len, uint32_t ttl = 0, uint32_t ts = microStore::time())
	{
		return put((const uint8_t*)key, (uint8_t)strlen(key), data, len, ttl, ts);
	}

	inline bool put(const char* key, const char* data, uint32_t ttl = 0, uint32_t ts = microStore::time())
	{
		return put((const uint8_t*)key, (uint8_t)strlen(key), (const uint8_t*)data, (uint16_t)strlen(data), ttl, ts);
	}

	inline bool put(const std::vector<uint8_t>& key, const uint8_t* data, uint16_t len, uint32_t ttl = 0, uint32_t ts = microStore::time())
	{
		return put(key.data(), (uint8_t)key.size(), data, len, ttl, ts);
	}

	inline bool put(const std::vector<uint8_t>& key, const std::vector<uint8_t>& data, uint32_t ttl = 0, uint32_t ts = microStore::time())
	{
		return put(key.data(), (uint8_t)key.size(), data.data(), (uint16_t)data.size(), ttl, ts);
	}

	inline bool put(const char* key, const std::string& data, uint32_t ttl = 0, uint32_t ts = microStore::time())
	{
		return put((const uint8_t*)key, (uint8_t)strlen(key), (const uint8_t*)data.c_str(), (uint16_t)data.length(), ttl, ts);
	}

	inline bool put(const std::string& key, const std::string& data, uint32_t ttl = 0, uint32_t ts = microStore::time())
	{
		return put((const uint8_t*)key.c_str(), (uint8_t)key.length(), (const uint8_t*)data.c_str(), (uint16_t)data.length(), ttl, ts);
	}

	/* -------- GET -------- */

	bool get(const uint8_t* key, uint8_t key_len, uint8_t* out, uint16_t* size)
	{
        if (!isValid()) return false;

USTORE_LOG("[ustore] get: fetching key %s with data size %u\n", bin_str(key, key_len), *size);
		if (key_len > USTORE_MAX_KEY_LEN) {
			USTORE_LOG("[ustore] get: failed due to excessive key length: %u\n", key_len);
			return false;
		}

		flush_buffer();

		IndexValue* e = index_find(key, key_len);
		if (!e) {
			USTORE_LOG("[ustore] get: key %s not found in index\n", bin_str(key, key_len));
			return false;
		}
//USTORE_LOG("[ustore] get: key %s offset %lu\n", bin_str(key, key_len), e->offset);

		if (is_ttl_expired_(e->timestamp, e->ttl)) {
			index_remove(key, key_len);
			USTORE_LOG("[ustore] get: key %s expired by TTL\n", bin_str(key, key_len));
			return false;
		}

		char name[USTORE_MAX_FILENAME_LEN];
		segment_name(e->segment, name);

		File f = _filesystem.open(name, File::ModeRead);
		if (!f) {
			USTORE_LOG("[ustore] get: key %s failed to open file %s\n", bin_str(key, key_len), name);
			return false;
		}

		f.seek(e->offset, SeekModeSet);

		RecordHeader hdr;

		//if (f.read(&hdr, sizeof(hdr)) != sizeof(hdr)) {
		size_t len = f.read(&hdr, sizeof(hdr));
//USTORE_LOG("[ustore] get: key %s read header of size %u\n", bin_str(key, key_len), len);
		if (len != sizeof(hdr)) {
			USTORE_LOG("[ustore] get: key %s header read failed\n", bin_str(key, key_len));
			f.close();
			return false;
		}

		if (hdr.magic != MAGIC_RECORD ||
			hdr.key_len > USTORE_MAX_KEY_LEN ||
			hdr.length > USTORE_MAX_VALUE_LEN)
		{
			USTORE_LOG("[ustore] get: key %s has corrupted record\n", bin_str(key, key_len));
			f.close();
			return false;
		}

		// CBA If this is only a size request then skip reading value
		if (out != nullptr) {
			f.seek((long)(e->offset+sizeof(hdr)+hdr.key_len), SeekModeSet);
			size_t read = std::min(hdr.length, *size);
			if (f.read(out, read) != read) {
				USTORE_LOG("[ustore] get: key %s value read failed\n", bin_str(key, key_len));
				f.close();
				return false;
			}
		}

		*size = hdr.length;

		f.close();

USTORE_LOG("[ustore] get: returning key %s with data length %u\n", bin_str(key, key_len), *size);
//USTORE_LOG("[ustore] get: %s\n", bin_str((uint8_t*)out, *size));
		return true;
	}

	inline bool get(const char* key, uint8_t* out, uint16_t* size)
	{
		return get((const uint8_t*)key, (uint8_t)strlen(key), out, size);
	}

	inline bool get(const char* key, char* out, uint16_t* size)
	{
		return get((const uint8_t*)key, (uint8_t)strlen(key), (uint8_t*)out, size);
	}

	inline bool get(const std::vector<uint8_t>& key, uint8_t* out, uint16_t* size)
	{
		return get(key.data(), (uint8_t)key.size(), out, size);
	}

	inline bool get(const std::vector<uint8_t>& key, std::vector<uint8_t>& out)
	{
		uint16_t size = USTORE_MAX_VALUE_LEN;
		out.resize(size);
		if (!get(key.data(), (uint8_t)key.size(), out.data(), &size)) {
			return false;
		}
		out.resize(size);
		return true;
	}

	inline bool get(const char* key, std::string& out)
	{
		uint16_t size = USTORE_MAX_VALUE_LEN;
		out.resize(size);
		// C++17 supported
		//if (!get(key.data(), (uint8_t)key.size(), out.data(), &size)) {
		// C++14 supported
		if (!get((const uint8_t*)key, (uint8_t)strlen(key), (uint8_t*)&out[0], &size)) {
			return false;
		}
		out.resize(size);
		return true;
	}

	inline bool get(const std::string& key, std::string& out)
	{
		uint16_t size = USTORE_MAX_VALUE_LEN;
		out.resize(size);
		// C++17 supported
		//if (!get(key.data(), (uint8_t)key.size(), out.data(), &size)) {
		// C++14 supported
		if (!get((const uint8_t*)key.c_str(), (uint8_t)key.length(), (uint8_t*)&out[0], &size)) {
			return false;
		}
		out.resize(size);
		return true;
	}

	/* -------- REMOVE -------- */

	bool remove(const uint8_t* key, uint8_t key_len)
	{
        if (!isValid()) return false;

		if(key_len > USTORE_MAX_KEY_LEN) return false;

		RecordHeader hdr;
		hdr.magic = MAGIC_RECORD;
		hdr.key_len = key_len;
		hdr.timestamp = 0;
		hdr.length = 0;
		hdr.flags = FLAG_DELETE;
		hdr.crc = crc32(0, (uint8_t*)&hdr, sizeof(hdr)-4);
		hdr.crc = crc32(hdr.crc, key, key_len);

		append_buffer((uint8_t*)&hdr, sizeof(hdr));
		append_buffer(key, key_len);

		RecordCommit c;
		c.magic=MAGIC_COMMIT;

		append_buffer((uint8_t*)&c, sizeof(c));

		flush_buffer();  // ensure tombstone is on flash before committing index entry

		// If an existing record was removed then increment _dead_since_compact
		if (index_find(key, key_len)) _dead_since_compact++;

		index_remove(key, key_len);

		persist_index_entry(key, key_len, 0xFFFFFFFF, 0);

		current_offset += sizeof(RecordHeader) + key_len + sizeof(RecordCommit);

		_stat_removes++;
		compact_if_threshold();

		return true;
	}

	inline bool remove(const char* key)
	{
		return remove((const uint8_t*)key, (uint8_t)strlen(key));
	}

	inline bool remove(const std::vector<uint8_t>& key)
	{
		return remove(key.data(), (uint8_t)key.size());
	}

	/* -------- EXISTS -------- */

	bool exists(const uint8_t* key, uint8_t key_len)
	{
        if (!isValid()) return false;
		if(key_len > USTORE_MAX_KEY_LEN) return false;
		IndexValue* e = index_find(key, key_len);
		if (!e) return false;
		if (is_ttl_expired_(e->timestamp, e->ttl)) { index_remove(key, key_len); return false; }
		return true;
	}

	inline bool exists(const char* key)
	{
		return exists((const uint8_t*)key, (uint8_t)strlen(key));
	}

	inline bool exists(const std::vector<uint8_t>& key)
	{
		return exists(key.data(), (uint8_t)key.size());
	}

	/* -------- SIZE -------- */

	inline size_t size()
	{
        if (!isValid()) return 0;
		return _index.size();
	}

	/* -------- STATS (diagnostics) -------- */

	// Lifetime counters since boot, for write-rate / compaction-frequency
	// diagnostics. Cheap; not persisted.
	struct Stats {
		uint32_t puts;            // successful put() calls
		uint32_t removes;         // successful remove() calls
		uint32_t compacts;        // completed compactions
		uint64_t bytes_written;   // payload bytes appended by put()
		uint32_t live_recs;       // current live record count (index size)
		uint32_t dead_since_compact;  // dead records accrued since last compaction
		uint32_t put_fails;       // put() calls that returned false (size/flush rejects)
	};
	inline Stats stats()
	{
		Stats s;
		s.puts               = _stat_puts;
		s.removes            = _stat_removes;
		s.compacts           = _stat_compacts;
		s.bytes_written      = _stat_bytes;
		s.live_recs          = (uint32_t)(isValid() ? _index.size() : 0);
		s.dead_since_compact = _dead_since_compact;
		s.put_fails          = _stat_put_fails;
		return s;
	}

	/* -------- POLICY -------- */

	inline void set_ttl_secs(uint32_t ttl_s)
	{
		policy_ttl_secs    = ttl_s;
	}

	inline void set_max_recs(uint32_t max_recs)
	{
		policy_max_recs = max_recs;
	}

	/* -------- DUMP INFO -------- */

	void dumpInfo(bool detailed = true)
	{
		flush_buffer();

		uint32_t total_file_size = 0;
		uint32_t total_entries = 0;
		uint32_t total_tomb_entries = 0;
		uint32_t total_tomb_bytes = 0;
		uint32_t total_dead_entries = 0;
		uint32_t total_dead_bytes = 0;

		for(uint32_t seg = 0; seg < _segment_count; seg++) {
			char name[USTORE_MAX_FILENAME_LEN];
			segment_name(seg, name);

			File f = _filesystem.open(name, File::ModeRead);
			if (!f) break;  // segments are contiguous; first missing one ends the scan

			uint32_t file_size = 0;
			uint32_t entries = 0;
			uint32_t tomb_entries = 0;
			uint32_t tomb_bytes = 0;
			uint32_t dead_entries = 0;
			uint32_t dead_bytes = 0;

			{
				f.seek(0, SeekModeEnd);
				file_size = (uint32_t)f.tell();
				f.seek(0, SeekModeSet);

				while(true) {
					uint32_t offset = (uint32_t)f.tell();

					RecordHeader hdr;
					if(f.read(&hdr, sizeof(hdr)) != sizeof(hdr)) break;

					if(hdr.magic != MAGIC_RECORD) break;

					if(hdr.key_len > USTORE_MAX_KEY_LEN) break;

					uint8_t key[USTORE_MAX_KEY_LEN];
					if(f.read(key, hdr.key_len) != hdr.key_len) break;

					f.seek((long)(offset + sizeof(hdr) + hdr.key_len + hdr.length), SeekModeSet);

					RecordCommit c;
					if(f.read(&c, sizeof(c)) != sizeof(c)) break;

					if(c.magic != MAGIC_COMMIT) break;

					uint32_t record_size = sizeof(RecordHeader) + hdr.key_len + hdr.length + sizeof(RecordCommit);

					if (hdr.flags & FLAG_DELETE) {
						tomb_entries++;
						tomb_bytes += record_size;
					}
					else {
						IndexValue* iv = index_find(key, hdr.key_len);
						bool live = (iv && iv->segment == seg && iv->offset == offset);
						if(live) {
							entries++;
						}
						else {
							dead_entries++;
							dead_bytes += record_size;
						}
					}
				}

				f.close();
			}

			total_file_size += file_size;
			total_entries += entries;
			total_tomb_entries += tomb_entries;
			total_tomb_bytes += tomb_bytes;
			total_dead_entries += dead_entries;
			total_dead_bytes += dead_bytes;

			if (detailed) {
				bool active = (seg == current_segment);
				printf("%s%s:\n", name, active ? " (ACTIVE)" : "");
				printf("  Bytes       : %8u\n", (unsigned)file_size);
				printf("  Entries     : %8u\n", (unsigned)entries);
				printf("  Tomb bytes  : %8u\n", (unsigned)tomb_bytes);
				printf("  Tomb entries: %8u\n", (unsigned)tomb_entries);
				printf("  Dead bytes  : %8u\n", (unsigned)dead_bytes);
				printf("  Dead entries: %8u\n", (unsigned)dead_entries);
			}
		}

		printf("TOTAL:\n");
		printf("  Bytes       : %8u\n", (unsigned)total_file_size);
		printf("  Entries     : %8u\n", (unsigned)total_entries);
		printf("  Tomb bytes  : %8u\n", (unsigned)total_tomb_bytes);
		printf("  Tomb entries: %8u\n", (unsigned)total_tomb_entries);
		printf("  Dead bytes  : %8u\n", (unsigned)total_dead_bytes);
		printf("  Dead entries: %8u\n", (unsigned)total_dead_entries);
	}

private:

	/* -------- INDEX TYPES (hoisted so iterator can reference them) -------- */

	struct VectorHash {
		template<typename Alloc>
		size_t operator()(const std::vector<uint8_t, Alloc>& v) const {
			uint32_t h = 2166136261u;
			for(uint8_t b : v) { h ^= b; h *= 16777619u; }
			return h;
		}
	};

	struct IndexValue {
		uint32_t segment;
		uint32_t offset;
		uint32_t timestamp;
		uint32_t ttl;
	};

	template<typename T>
	using rebind_alloc = typename std::allocator_traits<Allocator>::template rebind_alloc<T>;

	using byte_alloc_type = rebind_alloc<uint8_t>;
	using KeyType         = std::vector<uint8_t, byte_alloc_type>;
	using MapPairType     = std::pair<const KeyType, IndexValue>;
	using map_alloc_type  = rebind_alloc<MapPairType>;
	using IndexMap = std::unordered_map<
		KeyType, IndexValue, VectorHash, std::equal_to<KeyType>, map_alloc_type>;

	KeyType make_key(const uint8_t* key, uint8_t key_len) const {
		return KeyType(key, key + key_len, byte_alloc_type(_alloc));
	}

public:

	/* -------- ENTRY -------- */

	struct Entry {
		std::vector<uint8_t> key;
		std::vector<uint8_t> value;
		uint32_t timestamp;
		uint32_t ttl;
	};

	/* -------- ITERATOR -------- */

	// Forward iterator over all live key-value records.
	// Walks the in-memory index; key and timestamp are available with zero disk
	// I/O. The value field is lazy-loaded on first dereference (*it or it->)
	// and cached until operator++ advances to the next record.
	// WARNING: Mutating the store during iteration (put/remove/clear/compact) is
	// undefined behaviour — unordered_map iterator invalidation rules apply.
	class iterator {
	public:
		using iterator_category = std::forward_iterator_tag;
		using value_type        = Entry;
		using difference_type   = std::ptrdiff_t;
		using pointer           = const Entry*;
		using reference         = const Entry&;

		// Default constructor — produces a singular (non-dereferenceable) iterator.
		iterator() : store_(nullptr), value_loaded_(false) {}

		// operator*  — full dereference; loads the value from disk on first call
		//              for this position. Use this when you need entry.value.
		// operator-> — metadata-only; returns key and timestamp (already in memory)
		//              without touching disk. entry.value will be empty unless
		//              operator* was already called for this position.
		reference operator*()  const { if (!value_loaded_) load_value(); return current_; }
		pointer   operator->() const { return &current_; }

		iterator& operator++() {
			++pos_;
			load_meta();
			return *this;
		}

		iterator operator++(int) {
			iterator tmp = *this;
			++(*this);
			return tmp;
		}

		bool operator==(const iterator& o) const { return pos_ == o.pos_; }
		bool operator!=(const iterator& o) const { return pos_ != o.pos_; }

	private:
		friend class BasicFileStore<Allocator>;

		using idx_iter = typename IndexMap::iterator;

		iterator(BasicFileStore* store, idx_iter pos, idx_iter end)
			: store_(store), pos_(pos), end_(end), value_loaded_(false)
		{
			load_meta();
		}

		// Populate key and timestamp from the in-memory index. Zero disk I/O.
		// Resets value_loaded_ so the next dereference re-reads from disk.
		void load_meta() {
			if (pos_ == end_) return;
			iv_                = pos_->second;
			current_.key.assign(pos_->first.begin(), pos_->first.end());
			current_.timestamp = iv_.timestamp;
			current_.ttl       = iv_.ttl;
			current_.value.clear();
			value_loaded_      = false;
		}

		// Read the value from disk into current_.value. Called lazily by
		// operator* / operator->. Marked const so it can mutate mutable members.
		void load_value() const {
			char name[USTORE_MAX_FILENAME_LEN];
			store_->segment_name(iv_.segment, name);

			File f = store_->_filesystem.open(name, File::ModeRead);
			value_loaded_ = true;
			if (!f) { current_.value.clear(); return; }

			f.seek((long)iv_.offset, SeekModeSet);
			RecordHeader hdr;
			// Defensive: a stale/corrupt index entry can point at a non-record
			// offset (e.g. after an interrupted compaction). Reject anything that
			// isn't a sane record header BEFORE resize(hdr.length) — otherwise a
			// garbage length triggers a multi-megabyte allocation / OOM crash on
			// the warm-load that iterates the whole store.
			if (f.read(&hdr, sizeof(hdr)) != sizeof(hdr) ||
			    hdr.magic != MAGIC_RECORD ||
			    hdr.key_len > USTORE_MAX_KEY_LEN ||
			    hdr.length  > USTORE_MAX_VALUE_LEN) {
				current_.value.clear();
				f.close();
				return;
			}
			f.seek((long)(iv_.offset + sizeof(hdr) + hdr.key_len), SeekModeSet);
			current_.value.resize(hdr.length);
			if (hdr.length > 0)
				f.read(current_.value.data(), hdr.length);
			f.close();
		}

		BasicFileStore*      store_;
		idx_iter             pos_;
		idx_iter             end_;
		IndexValue           iv_;
		mutable bool         value_loaded_;
		mutable Entry        current_;
	};

	/* -------- BEGIN / END -------- */

	iterator begin() {
		flush_buffer();   // ensure pending writes are visible before iterating
		return iterator(this, _index.begin(), _index.end());
	}

	iterator end() {
		return iterator(this, _index.end(), _index.end());
	}

private:

	/* -------- BUFFERED WRITES -------- */

	void append_buffer(const uint8_t* data, size_t len)
	{
		const uint8_t* p=(const uint8_t*)data;

		while(len)
		{
			size_t space = USTORE_WRITE_BUFFER_SIZE - write_buf_pos;
			size_t n = (len < space) ? len : space;

			memcpy(write_buf+write_buf_pos, p, n);

			write_buf_pos += n;
			p += n;
			len -= n;

			if(write_buf_pos == USTORE_WRITE_BUFFER_SIZE)
				flush_buffer();
		}
	}

	bool flush_buffer()
	{
		if (!active_file) {
			USTORE_LOG("[ustore] ERROR: Active file is not valid, failed to flush buffer\n");
			// CBA Must reset buffer pos to avoid an infinite flushing loop
			write_buf_pos = 0;
			return false;
		}
		active_file.write(write_buf, write_buf_pos);
		active_file.flush();
		write_buf_pos = 0;
		return true;
	}

	/* -------- INDEX -------- */

	IndexValue* index_find(const uint8_t* key, uint8_t key_len)
	{
		auto it = _index.find(make_key(key, key_len));
		return (it != _index.end()) ? &it->second : nullptr;
	}

	void index_insert(const uint8_t* key, uint8_t key_len, uint32_t seg, uint32_t off, uint32_t ts = 0, uint32_t ttl = 0)
	{
		IndexValue& iv = _index[make_key(key, key_len)];
		iv.segment   = seg;
		iv.offset    = off;
		iv.timestamp = ts;
		iv.ttl       = ttl;
	}

	void index_remove(const uint8_t* key, uint8_t key_len)
	{
		_index.erase(make_key(key, key_len));
	}

	/* -------- POLICY HELPERS -------- */

	bool is_ttl_expired_(uint32_t ts, uint32_t record_ttl) const
	{
		uint32_t effective_ttl = (record_ttl > 0) ? record_ttl : policy_ttl_secs;
		return effective_ttl > 0 && microStore::time() > ts && (microStore::time() - ts) >= effective_ttl;
	}

	// Evict the oldest records (by timestamp) until _index.size() <= policy_max_recs.
	// Returns the number of entries evicted.
	// When to_evict == 1 (the common put() path), a simple O(n) scan is used to
	// avoid heap allocation.  When to_evict > 1 (init / compact), a partial sort
	// is used to evict all excess entries in a single pass.
	size_t prune_index_to_max_recs_()
	{
		if (policy_max_recs == 0 || _index.size() <= policy_max_recs) return 0;

		size_t to_evict = _index.size() - policy_max_recs;

		if (to_evict == 1) {
			// Fast path: single linear scan for the oldest entry.
			auto oldest = _index.begin();
			for (auto it = _index.begin(); it != _index.end(); ++it)
				if (it->second.timestamp < oldest->second.timestamp) oldest = it;
			_index.erase(oldest);
		} else {
			// Bulk path: collect (timestamp, key) pairs, partial-sort, then erase.
			using KTSPair = std::pair<uint32_t, KeyType>;
			using KTSAlloc = rebind_alloc<KTSPair>;
			KTSAlloc kts_alloc(_alloc);
			std::vector<KTSPair, KTSAlloc> candidates(kts_alloc);
			candidates.reserve(_index.size());
			for (auto& kv : _index)
				candidates.push_back(std::make_pair(kv.second.timestamp, kv.first));
			std::partial_sort(candidates.begin(), candidates.begin() + (long)to_evict, candidates.end(),
				[](const KTSPair& a, const KTSPair& b){ return a.first < b.first; });
			for (size_t i = 0; i < to_evict; i++) {
				_index.erase(candidates[i].second);
			}
		}
		// Record(s) evicted so increment _dead_since_compact
		_dead_since_compact += to_evict;
USTORE_LOG("[ustore] Evicted %lu records to policy_max_recs\n", to_evict);

		return to_evict;
	}

	/* -------- INDEX FILE -------- */

	bool persist_index_entry(const uint8_t* key, uint8_t key_len, uint32_t seg, uint32_t off, uint32_t ts = 0, uint32_t ttl = 0)
	{
		if (!index_file) {
			USTORE_LOG("[ustore] ERROR: Index file is not valid\n");
			return false;
		}

		index_file.write(&key_len, 1);
		index_file.write(key, key_len);
		index_file.write(&seg, 4);
		index_file.write(&off, 4);
		index_file.write(&ts, 4);
		index_file.write(&ttl, 4);
		index_file.flush();  // explicit fsync — guarantees entry reaches flash

		return true;
	}

	bool write_index_bulk()
	{
		char name[USTORE_MAX_FILENAME_LEN];
		index_name(name);

		File f = _filesystem.open(name, File::ModeAppend);
		if (!f) return false;

		for (auto& kv : _index)
		{
			uint8_t  key_len = (uint8_t)kv.first.size();
			uint32_t seg     = kv.second.segment;
			uint32_t off     = kv.second.offset;
			uint32_t ts      = kv.second.timestamp;

			uint32_t ttl     = kv.second.ttl;

			f.write(&key_len, 1);
			f.write(kv.first.data(), key_len);
			f.write(&seg, 4);
			f.write(&off, 4);
			f.write(&ts, 4);
			f.write(&ttl, 4);
		}

		f.flush();
		f.close();
		return true;
	}

	bool load_index()
	{
		char name[USTORE_MAX_FILENAME_LEN];
		index_name(name);

		File f = _filesystem.open(name, File::ModeRead);
		if (!f) return false;

		while(true)
		{
			uint8_t key_len;
			uint8_t key[USTORE_MAX_KEY_LEN];
			uint32_t seg;
			uint32_t off;

			if(f.read(&key_len, 1) != 1) break;
			if(key_len > USTORE_MAX_KEY_LEN) break;
			if(f.read(key, key_len) != key_len) break;
			if(f.read(&seg, 4) != 4) break;
			if(f.read(&off, 4) != 4) break;
			uint32_t ts = 0;
			if(f.read(&ts, 4) != 4) break;
			uint32_t ttl = 0;
			if(f.read(&ttl, 4) != 4) break;

			// seg==0xFFFFFFFF is a deletion sentinel written by remove()
			if(seg==0xFFFFFFFF)
				index_remove(key, key_len);
			else
				index_insert(key, key_len, seg, off, ts, ttl);
		}

		f.close();

		return true;
	}

	/* -------- INDEX FILE OPEN HELPER -------- */

	void open_index_for_append()
	{
		char name[USTORE_MAX_FILENAME_LEN];
		index_name(name);
		index_file = _filesystem.open(name, File::ModeAppend);
	}

	const char* char_str(const uint8_t* key, size_t len)
	{
		static char str[USTORE_MAX_VALUE_LEN+1];
		int n = 0;
		for (int i = 0; i < len && i < USTORE_MAX_VALUE_LEN; ++i) {
			str[n] = key[i];
			n++;
		}
		str[n] = 0;
		return str;
	}

	const char* bin_str(const uint8_t* key, size_t len)
	{
		static char str[USTORE_MAX_VALUE_LEN*2+1];
		int n = 0;
		for (int i = 0; i < len && i < USTORE_MAX_VALUE_LEN; ++i) {
			//if (key[i] > 31 && key[i] < 127) {
			//	str[n++] = key[i];
			//}
			//else {
				snprintf(str+n, sizeof(str)-n, "%02x", key[i]);
				n += 2;
			//}
		}
		str[n] = 0;
		return str;
	}

	/* -------- SEGMENTS -------- */

	void segment_name(uint32_t id,char* out)
	{
		snprintf(out, USTORE_MAX_FILENAME_LEN, "%s_%u.dat", base_prefix,id);
	}

	void index_name(char* out)
	{
		snprintf(out, USTORE_MAX_FILENAME_LEN, "%s_index.dat", base_prefix);
	}

	void journal_name(char* out)
	{
		snprintf(out, USTORE_MAX_FILENAME_LEN, "%s_journal.dat", base_prefix);
	}

	bool open_segment(uint32_t id)
	{
		if (active_file) active_file.close();

		char name[USTORE_MAX_FILENAME_LEN];
		segment_name(id,name);

USTORE_LOG("[ustore] Opening active file: %s\n", name);
		active_file = _filesystem.open(name, File::ModeReadAppend);
		if (!active_file) {
			USTORE_LOG("[ustore] ERROR: Failed to open active file: %s\n", name);
			return false;
		}
		current_segment=id;
		active_file.seek(0,SeekModeEnd);
		current_offset=active_file.tell();
		return true;
	}

	bool rotate_segment_if_needed(uint32_t write_size)
	{
		if (current_offset + write_size + sizeof(RecordHeader) + sizeof(RecordCommit) < _segment_size)
			return true;

USTORE_LOG("[ustore] Rotating segment...\n");
		flush_buffer();

		if (active_file) active_file.close();

		current_segment++;

		if (current_segment >= _segment_count) {
			if (compact_in_cooldown &&
				(microStore::millis() - compact_cooldown_start_ms) < USTORE_COMPACT_RETRY_MS)
			{
				USTORE_LOG("[ustore] Compact skipped: cooldown active\n");
				current_segment = _segment_count - 1;
				open_segment(current_segment);
				return false;
			}
			if (!compact()) {
				compact_in_cooldown       = true;
				compact_cooldown_start_ms = microStore::millis();
				current_segment = _segment_count - 1;
				open_segment(current_segment);
				return false;
			}
			compact_in_cooldown = false;
			current_segment = 1;  // seg0 = compacted data; fresh writes start at seg1
		}

		open_segment(current_segment);
		return true;
	}

	/* -------- JOURNAL HELPERS -------- */

	void write_journal(uint32_t state, uint32_t next_seg = 0, uint32_t tmp_valid_size = 0)
	{
		char name[USTORE_MAX_FILENAME_LEN]; journal_name(name);
		File f = _filesystem.open(name, File::ModeWrite);
		if (!f) return;
		Journal j;
		j.magic          = JOURNAL_MAGIC;
		j.state          = state;
		j.next_seg       = next_seg;
		j.tmp_valid_size = tmp_valid_size;
		f.write(&j, sizeof(j));
		f.flush();
		f.close();
	}

	void clear_journal()
	{
		char name[USTORE_MAX_FILENAME_LEN]; journal_name(name);
		_filesystem.remove(name);
	}

	void compact_if_threshold()
	{
#if USTORE_COMPACT_THRESHOLD > 0
		uint32_t total = (uint32_t)_index.size() + _dead_since_compact;
		if (total > 0 && _dead_since_compact * 100 / total >= USTORE_COMPACT_THRESHOLD) {
			// Respect the same back-off the rotate path uses, so a failing
			// compact() isn't retried on every put(). active_file stays open
			// on this path (compact() is never entered), so the store keeps
			// working.
			if (compact_in_cooldown &&
				(microStore::millis() - compact_cooldown_start_ms) < USTORE_COMPACT_RETRY_MS)
			{
				return;
			}
			USTORE_LOG("[ustore] Compaction triggered by deleted threshold\n");
			if (_incremental) {
				// Begin a resumable compaction and return — the host pumps
				// compact_step() between loop ticks while the front tier absorbs
				// reads and buffers writes. compact_begin() closes the active
				// segment; compact_finalize()/compact_abort() reopen one. Avoiding
				// the synchronous O(records) freeze here is the entire point.
				if (!compacting()) compact_begin();
				return;
			}
			if (compact()) {
				// seg0 now holds the compacted data; fresh writes start at
				// seg1, mirroring rotate_segment_if_needed().
				compact_in_cooldown = false;
				current_segment = 1;
				open_segment(current_segment);
			} else {
				// compact() closed active_file at its top and then failed.
				// Without reopening, the next flush_buffer() sees !active_file
				// and silently drops the write. Restore the open-active-file
				// invariant and back off, exactly as the rotate path does.
				compact_in_cooldown       = true;
				compact_cooldown_start_ms = microStore::millis();
				open_segment(current_segment);
			}
		}
#endif
	}

	void recover_if_needed()
	{
		char name[USTORE_MAX_FILENAME_LEN]; journal_name(name);
		File f = _filesystem.open(name, File::ModeRead);
		if (!f) return;

		Journal j;
		size_t n = f.read(&j, sizeof(j));
		f.close();

		if (n != sizeof(j) || j.magic != JOURNAL_MAGIC) {
			_filesystem.remove(name);
			return;
		}

		char tmp_name[USTORE_MAX_FILENAME_LEN]; snprintf(tmp_name, sizeof(tmp_name), "%s_compact.tmp", base_prefix);

		if (j.state == JOURNAL_COMMIT) {
			// Tmp file is complete — finish the swap.
			finalize_compaction();
		} else if (j.next_seg == 0) {
			// COMPACTING, no source segments deleted yet — tmp may be partial, discard it.
			// All original segments are intact; normal boot proceeds.
			_filesystem.remove(tmp_name);
		} else {
			// COMPACTING with next_seg > 0: source segments 0..next_seg-1 have been deleted.
			// compact.tmp holds their live records up to byte tmp_valid_size (bytes beyond
			// that are either extra valid records from a flushed-but-not-journaled segment,
			// or a partial write stopped by the crash — both are handled harmlessly by the
			// segment scan in rebuild_index_from_segments()).
			//
			// Recovery strategy: consolidate compact.tmp into segment_0.dat so the
			// surviving segments next_seg..7 plus seg0 hold every live record, then
			// force a full index rebuild from those segments.
			//
			// The on-disk index still maps every key to its PRE-compaction
			// segment/offset, which is now stale (seg0 is the compacted data) and
			// would make the boot warm-load read garbage. Drop it UNCONDITIONALLY
			// — before the rename — so the index is never trusted across an
			// interrupted compaction, even on a retry where tmp was already renamed
			// by an earlier boot.
			char iname[USTORE_MAX_FILENAME_LEN]; index_name(iname);
			_filesystem.remove(iname);

			char seg0[USTORE_MAX_FILENAME_LEN]; segment_name(0, seg0);
			if (_filesystem.exists(tmp_name)) {
				if (!_filesystem.rename(tmp_name, seg0)) {
					// Genuine filesystem error — keep the journal and retry next
					// boot. The index is already gone, so this boot still rebuilds
					// (minus the un-renamed tmp records, recovered on the retry).
					return;
				}
			}
			// else: a previous boot already renamed tmp→seg0 — nothing left to
			// consolidate; just clear the journal below.
		}

		_filesystem.remove(name);  // clear journal
	}

	/* -------- FINALIZE COMPACTION -------- */

	void finalize_compaction()
	{
		char tmp_name[USTORE_MAX_FILENAME_LEN]; snprintf(tmp_name, sizeof(tmp_name), "%s_compact.tmp", base_prefix);
		char seg0[USTORE_MAX_FILENAME_LEN];     segment_name(0, seg0);

		// Remove all existing segments, then rename tmp → seg0.
		for (uint32_t i = 0; i < _segment_count; i++) {
			char sname[USTORE_MAX_FILENAME_LEN]; segment_name(i, sname);
			_filesystem.remove(sname);
		}
		if (!_filesystem.rename(tmp_name, seg0)) {
			_filesystem.remove(tmp_name);
			return;
		}

		// Rebuild in-memory index by scanning seg0.
		_index.clear();
		File f = _filesystem.open(seg0, File::ModeRead);
		if (f) {
			uint32_t scan_offset = 0;
			uint8_t key_buf[USTORE_MAX_KEY_LEN];
			while (true) {
				RecordHeader hdr;
				if (f.read(&hdr, sizeof(hdr)) != sizeof(hdr)) break;
				if (hdr.magic != MAGIC_RECORD || hdr.key_len == 0 ||
					hdr.key_len > USTORE_MAX_KEY_LEN || hdr.length > USTORE_MAX_VALUE_LEN) break;
				if (f.read(key_buf, hdr.key_len) != hdr.key_len) break;
				f.seek((long)(scan_offset + sizeof(hdr) + hdr.key_len + hdr.length), SeekModeSet);
				RecordCommit c;
				if (f.read(&c, sizeof(c)) != sizeof(c)) break;
				if (c.magic != MAGIC_COMMIT) break;
				if (!(hdr.flags & FLAG_DELETE))
					index_insert(key_buf, hdr.key_len, 0, scan_offset, hdr.timestamp, hdr.ttl);
				scan_offset += sizeof(hdr) + hdr.key_len + hdr.length + sizeof(c);
			}
			f.close();
		}

		// Rebuild persistent index from the now-correct in-memory index.
		// Use write_index_bulk() — single flush for all entries — not persist_index_entry()
		// which flushes after every entry and would cause N × fsync delays on flash.
		char iname[USTORE_MAX_FILENAME_LEN]; index_name(iname);
		if (index_file) index_file.close();
		_filesystem.remove(iname);
		write_index_bulk();
		open_index_for_append();

		current_segment = 0;
		// current_offset is set by open_segment() which the caller will invoke next.
	}

	/* -------- INCREMENTAL (RESUMABLE) COMPACTION ENGINE -------- */

	// Begin a compaction: close the active segment, journal COMPACTING, open
	// compact.tmp, evict to max_recs, and seed the step cursors. Afterwards there
	// is NO open active segment — compact_step() must run to a finalize or abort,
	// both of which reopen one. On a tmp-open failure the store stays writable.
	void compact_begin()
	{
USTORE_LOG("[ustore] Compacting storage (begin)...\n");
		// Close any open handle on the active segment before unlinking source
		// files. Without this, LittleFS refuses to unlink the current segment
		// ("Has open FD") and every compaction silently frees nothing — dead
		// bytes then accumulate until the low-memory watchdog reboots the device.
		flush_buffer();
		if (active_file) active_file.close();

		_compacting         = true;
		_compact_failed     = false;
		_compact_dirty      = false;
		_compact_seg        = 0;
		_compact_rec        = 0;
		_compact_out_off    = 0;
		_compact_committed  = 0;
		_compact_seg_loaded = false;
		_compact_offsets.clear();

		// Phase 1: journal COMPACTING (next_seg=0 — no source segments deleted yet).
		write_journal(JOURNAL_COMPACTING, 0, 0);

		char tmp_name[USTORE_MAX_FILENAME_LEN]; snprintf(tmp_name, sizeof(tmp_name), "%s_compact.tmp", base_prefix);
USTORE_LOG("[ustore] Opening tmp file: %s\n", tmp_name);
		_compact_outf = _filesystem.open(tmp_name, File::ModeWrite);
		if (!_compact_outf) {
			// Can't open the scratch file — abort cleanly and stay writable.
			clear_journal();
			_compact_failed = true;
			_compacting     = false;
			open_segment(current_segment);
			return;
		}

		// Evict excess (max_recs) before enumerating live records, so over-cap
		// entries are dropped from the compacted output.
		prune_index_to_max_recs_();
	}

	// Snapshot the live record offsets of the current source segment from the
	// in-memory index (sorted for sequential reads) and open it for reading.
	// TTL-expired entries are dropped from the index here rather than copied — so
	// their stale segment/offset cannot survive into the compacted store.
	void compact_load_segment()
	{
		_compact_offsets.clear();
		using KeyVec = std::vector<KeyType, rebind_alloc<KeyType>>;
		KeyVec expired{ rebind_alloc<KeyType>(_alloc) };
		for (auto& kv : _index) {
			if (kv.second.segment != _compact_seg) continue;
			if (is_ttl_expired_(kv.second.timestamp, kv.second.ttl)) { expired.push_back(kv.first); continue; }
			_compact_offsets.push_back(kv.second.offset);
		}
		for (auto& k : expired) _index.erase(k);   // safe: iteration finished
		std::sort(_compact_offsets.begin(), _compact_offsets.end());

		char src_name[USTORE_MAX_FILENAME_LEN]; segment_name(_compact_seg, src_name);
		_compact_src        = _filesystem.open(src_name, File::ModeRead);  // may be invalid (empty/missing segment)
		_compact_rec        = 0;
		_compact_seg_loaded = true;
	}

	// Copy one live record from the source segment into compact.tmp and repoint
	// its index entry at the new (segment 0) offset — building the post-compaction
	// index inline, so finalize is a rename + index-write with no O(records)
	// re-scan. Returns false only on a fatal write error (out of space), which
	// aborts the compaction. A corrupt/unreadable source record is skipped and
	// flags _compact_dirty, forcing finalize to fall back to the authoritative
	// re-scan so no stale index entry survives.
	bool compact_copy_one()
	{
		// Static scratch (compact is not re-entrant) — keep 1 KB off the stack.
		static uint8_t key_buf[USTORE_MAX_KEY_LEN];
		static uint8_t val_buf[USTORE_MAX_VALUE_LEN];

		uint32_t off = _compact_offsets[_compact_rec++];
		yield_now();
		if (!_compact_src) { _compact_dirty = true; return true; }

		_compact_src.seek((long)off, SeekModeSet);
		RecordHeader hdr;
		if (_compact_src.read(&hdr, sizeof(hdr)) != sizeof(hdr) ||
			hdr.magic != MAGIC_RECORD || hdr.key_len > USTORE_MAX_KEY_LEN || hdr.length > USTORE_MAX_VALUE_LEN) {
			_compact_dirty = true; return true;
		}
		if (_compact_src.read(key_buf, hdr.key_len) != hdr.key_len) { _compact_dirty = true; return true; }
		if (hdr.length > 0 && _compact_src.read(val_buf, hdr.length) != hdr.length) { _compact_dirty = true; return true; }

		RecordCommit c; c.magic = MAGIC_COMMIT;
		uint32_t reclen  = sizeof(hdr) + hdr.key_len + hdr.length + sizeof(c);
		size_t   written = 0;
		written += _compact_outf.write(&hdr,    sizeof(hdr));
		written += _compact_outf.write(key_buf, hdr.key_len);
		if (hdr.length > 0) written += _compact_outf.write(val_buf, hdr.length);
		written += _compact_outf.write(&c, sizeof(c));
		if (written != reclen) return false;   // out of space — abort

		// Inline index build: repoint this key at its new home in seg0.
		IndexValue* iv = index_find(key_buf, hdr.key_len);
		if (iv) { iv->segment = 0; iv->offset = _compact_out_off; }
		else    { _compact_dirty = true; }   // key vanished mid-compaction — be safe
		_compact_out_off += reclen;
		return true;
	}

	// Done with the current source segment: flush compact.tmp, journal progress
	// (next_seg = seg+1) BEFORE deleting the source (crash-safe ordering — a crash
	// between the two is reconciled on boot), delete the source, advance.
	bool compact_close_segment()
	{
		if (_compact_src) _compact_src.close();
		_compact_outf.flush();

		write_journal(JOURNAL_COMPACTING, _compact_seg + 1, _compact_out_off);

		char src_name[USTORE_MAX_FILENAME_LEN]; segment_name(_compact_seg, src_name);
		_filesystem.remove(src_name);   // no-op if the segment had no records / didn't exist
		_compact_committed++;

		_compact_seg++;
		_compact_seg_loaded = false;
		return true;
	}

	// All source segments copied: commit, swap compact.tmp into seg0, persist the
	// index, and reopen a writable active segment (seg1 — seg0 holds compacted
	// data, mirroring rotate_segment_if_needed()).
	void compact_finalize()
	{
		_compact_outf.flush();
		_compact_outf.close();

		// Phase 4: commit — past this point recover_if_needed() finalizes on boot.
		write_journal(JOURNAL_COMMIT);

		if (_compact_dirty) {
			// A record was skipped (corruption) — the inline index can't be
			// trusted. Fall back to the authoritative rename + full seg0 re-scan.
			finalize_compaction();
		} else {
			// Fast path: the index already points every live key at its seg0
			// offset. Swap the file in and persist the in-memory index as-is —
			// no O(records) re-scan.
			char tmp_name[USTORE_MAX_FILENAME_LEN]; snprintf(tmp_name, sizeof(tmp_name), "%s_compact.tmp", base_prefix);
			char seg0[USTORE_MAX_FILENAME_LEN];     segment_name(0, seg0);
			for (uint32_t i = 0; i < _segment_count; i++) {
				char sname[USTORE_MAX_FILENAME_LEN]; segment_name(i, sname);
				_filesystem.remove(sname);
			}
			if (_filesystem.rename(tmp_name, seg0)) {
				char iname[USTORE_MAX_FILENAME_LEN]; index_name(iname);
				if (index_file) index_file.close();
				_filesystem.remove(iname);
				write_index_bulk();
				open_index_for_append();
			} else {
				// Rename failed — recover by re-scan (the index may now be stale).
				_filesystem.remove(tmp_name);
				finalize_compaction();
			}
		}

		clear_journal();
		_dead_since_compact = 0;
		_stat_compacts++;
		_compacting     = false;
		compact_in_cooldown = false;

		current_segment = 1;
		open_segment(1);
	}

	// Abort an in-flight compaction (write error / open failure). Preserve data
	// per the journal protocol and reopen a writable active segment.
	void compact_abort()
	{
		if (_compact_src)  _compact_src.close();
		if (_compact_outf) _compact_outf.close();

		char tmp_name[USTORE_MAX_FILENAME_LEN]; snprintf(tmp_name, sizeof(tmp_name), "%s_compact.tmp", base_prefix);
		if (_compact_committed == 0) {
			// No source segment deleted yet — discard the scratch file entirely.
			_filesystem.remove(tmp_name);
			clear_journal();
		}
		// else: source segments 0.._compact_committed-1 are gone and live only in
		// compact.tmp; leave compact.tmp + the journal so recover_if_needed()
		// finishes the swap on the next boot.

		_compact_failed     = true;
		_compacting         = false;
		// Back off so a failing compaction isn't retried on every put().
		compact_in_cooldown       = true;
		compact_cooldown_start_ms = microStore::millis();

		// The in-memory index now mixes repointed (seg0) and not-yet-processed
		// entries; rebuild it from what is actually on disk so reads stay correct
		// until the boot-time recovery consolidates compact.tmp.
		if (_compact_committed > 0)
			rebuild_index_from_segments();

		current_segment = _segment_count - 1;
		open_segment(current_segment);
	}

	/* -------- INDEX REBUILD FROM LOG -------- */

	// Called when the index file is missing. Scans all segment files in write
	// order to reconstruct the in-memory index, then persists it so future
	// boots are fast.  Uses the same record-walk logic as finalize_compaction().
	void rebuild_index_from_segments()
	{
		USTORE_LOG("[ustore] Index missing — rebuilding from segment files...\n");
		_index.clear();

		for (uint32_t seg = 0; seg < _segment_count; seg++)
		{
			char sname[USTORE_MAX_FILENAME_LEN];
			segment_name(seg, sname);
			File f = _filesystem.open(sname, File::ModeRead);
			if (!f) continue;

			uint32_t scan_offset = 0;
			uint8_t  key_buf[USTORE_MAX_KEY_LEN];
			while (true)
			{
				RecordHeader hdr;
				if (f.read(&hdr, sizeof(hdr)) != sizeof(hdr)) break;
				if (hdr.magic != MAGIC_RECORD || hdr.key_len == 0 ||
					hdr.key_len > USTORE_MAX_KEY_LEN || hdr.length > USTORE_MAX_VALUE_LEN) break;
				if (f.read(key_buf, hdr.key_len) != hdr.key_len) break;
				f.seek((long)(scan_offset + sizeof(hdr) + hdr.key_len + hdr.length), SeekModeSet);
				RecordCommit c;
				if (f.read(&c, sizeof(c)) != sizeof(c)) break;
				if (c.magic != MAGIC_COMMIT) break;
				if (hdr.flags & FLAG_DELETE)
					index_remove(key_buf, hdr.key_len);
				else
					index_insert(key_buf, hdr.key_len, seg, scan_offset, hdr.timestamp, hdr.ttl);
				scan_offset += sizeof(hdr) + hdr.key_len + hdr.length + sizeof(c);
			}
			f.close();
		}

		// Persist the rebuilt index so the next boot uses the fast path.
		char iname[USTORE_MAX_FILENAME_LEN]; index_name(iname);
		if (index_file) index_file.close();
		_filesystem.remove(iname);
		write_index_bulk();
	}

	/* -------- COMPACTION -------- */

public:
	// Synchronous compaction: begin a resumable compaction (if one is not already
	// in flight) and drain it to completion in this call. The work itself lives in
	// the resumable engine (compact_begin + compact_step + compact_finalize);
	// passing SIZE_MAX runs every slice back-to-back without yielding the caller.
	// This is the path standalone FileStores and the segments-full rotate take.
	// The incremental path (set_incremental(true)) instead begins the compaction
	// and lets the host drive compact_step() a few records at a time between loop
	// ticks, so the old single ~O(records) freeze is sliced into bounded steps.
	bool compact()
	{
		if (!_compacting) compact_begin();
		while (!compact_step(SIZE_MAX)) { /* drain */ }
		return !_compact_failed;
	}

	// Opt this store into incremental compaction: compact_if_threshold() begins a
	// resumable compaction and returns instead of draining, leaving the host to
	// pump compact_step(). Only safe when a front tier (e.g. BasicTieredStore)
	// absorbs reads and buffers writes while the active segment is closed.
	inline void set_incremental(bool enabled) { _incremental = enabled; }

	// Is a resumable compaction in flight (spanning multiple compact_step calls)?
	inline bool compacting() const { return _compacting; }

	// Advance an in-flight compaction by up to `max_records` record copies.
	// Returns true when the store is no longer compacting (finished, aborted,
	// or was already idle). Drive this from a cooperative loop — releasing the
	// caller's lock between calls — to slice the old single ~O(records) freeze
	// into bounded steps. Passing SIZE_MAX runs it to completion in one call.
	bool compact_step(size_t max_records)
	{
		if (!_compacting) return true;
		size_t done = 0;
		while (done < max_records) {
			yield_now();
			if (!_compact_seg_loaded) {
				if (_compact_seg >= _segment_count) { compact_finalize(); return true; }
				compact_load_segment();
			}
			if (_compact_rec >= _compact_offsets.size()) {
				if (!compact_close_segment()) { compact_abort(); return true; }
				continue;
			}
			if (!compact_copy_one()) { compact_abort(); return true; }
			done++;
		}
		return false;   // more to do
	}

private:

	FileSystem _filesystem;

	char base_prefix[32];
	uint32_t _segment_size = USTORE_DEFAULT_SEGMENT_SIZE;
	uint8_t _segment_count = USTORE_DEFAULT_SEGMENT_COUNT;

	File active_file;
	File index_file;

	uint32_t current_segment = 0;
	uint32_t current_offset = 0;

	bool compact_in_cooldown = false;
	uint32_t compact_cooldown_start_ms = 0;
	uint32_t _dead_since_compact = 0;

	// ---- Incremental (resumable) compaction state ----
	// When _compacting is set, a compaction is in flight across multiple
	// compact_step() calls. Each step copies a bounded number of live records
	// from the source segments into compact.tmp, building _compact_index inline
	// (so finalize is a rename + index-swap, with no O(records) re-scan).
	// Between steps the caller releases its lock, so the radio/main loop run —
	// turning the old single ~18 s freeze into many sub-second slices.
	// The on-disk protocol (per-segment journal + delete) is unchanged, so
	// recover_if_needed() still recovers a partially-compacted store on boot.
	bool     _incremental         = false; // begin-and-yield (host pumps compact_step) vs synchronous drain
	bool     _compacting          = false;
	uint32_t _compact_seg         = 0;   // source segment currently being copied
	size_t   _compact_rec         = 0;   // next record index within _compact_offsets
	File     _compact_outf;              // compact.tmp, held open across steps
	uint32_t _compact_out_off     = 0;   // write cursor within compact.tmp
	bool     _compact_seg_loaded  = false;
	File     _compact_src;               // current source-segment read handle, held across steps
	uint32_t _compact_committed   = 0;   // source segments committed to compact.tmp so far
	bool     _compact_failed      = false;
	bool     _compact_dirty       = false; // a record was skipped — finalize must re-scan, not trust the inline index
	std::vector<uint32_t> _compact_offsets;  // live offsets of the current source segment

	uint32_t policy_ttl_secs = USTORE_DEFAULT_TTL_SECS; // 0 = TTL disabled (seconds)
	uint32_t policy_max_recs = USTORE_DEFAULT_MAX_RECS; // 0 = max-records disabled

	// Lightweight lifetime counters for diagnostics (write rate / compaction
	// frequency). Cheap to maintain; read via the stats() accessors. Not
	// persisted — they reset to 0 on each boot, which is what we want for
	// rate sampling (delta over a window / since-boot).
	uint32_t _stat_puts      = 0;   // successful put() calls
	uint32_t _stat_removes   = 0;   // successful remove() calls
	uint32_t _stat_compacts  = 0;   // completed compactions
	uint64_t _stat_bytes     = 0;   // payload bytes appended by put()
	uint32_t _stat_put_fails = 0;   // put() calls that returned false

	uint8_t write_buf[USTORE_WRITE_BUFFER_SIZE];
	size_t write_buf_pos;

	Allocator _alloc;
	IndexMap _index;
};


using FileStore = BasicFileStore<>;

}
