// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// String Adaptive Hash Map (SAHA) for aggregation.
//
// Based on the SAHA paper: "SAHA: A String Adaptive Hash Table for Analytical Databases"
// (Zheng et al., 2020). Dispatches string keys by length to specialized sub-hash-tables,
// using integer representations for short strings to enable fast integer hashing/comparison.
//
// Architecture:
//   len 0-2:   S0 - Direct array lookup (65536 entries, O(1), no hashing)
//   len 3-8:   S1 - phmap<Key8(uint64_t), Value>
//   len 9-16:  S2 - phmap<Key16(int128_t), Value>
//   len 17-24: S3 - phmap<Key24, Value>
//   len >24:   L  - phmap<Slice, Value> (pointer-based, same as baseline)
//
// SAHAMultiMap provides a self-contained hash map API (emplace/find/lazy_emplace)
// comparable to phmap::flat_hash_map, making it easy to benchmark independently.

#pragma once

#include <cstdint>
#include <cstring>
#include <vector>

#include "base/phmap/phmap.h"
#include "column/binary_column.h"
#include "column/column_hash.h"
#include "column/nullable_column.h"
#include "exec/aggregate/agg_hash_map.h"
#include "gutil/strings/fastmem.h"
#include "runtime/mem_pool.h"

namespace starrocks {

// ============================================================================
// Key types for SAHA sub-tables
// ============================================================================

// Key8: uint64_t encoding for strings of length 3-8.
// Low bytes hold string content; highest byte holds the length for uniqueness.
using Key8 = uint64_t;

// Key16: int128_t encoding for strings of length 9-16.
// Low 64 bits = first 8 bytes; high 64 bits = tail bytes + length in top byte.
using Key16 = int128_t;

// Key24: 24-byte struct for strings of length 17-24.
struct Key24 {
    uint64_t v[3] = {0, 0, 0};
    bool operator==(const Key24& o) const { return v[0] == o.v[0] && v[1] == o.v[1] && v[2] == o.v[2]; }
    bool operator!=(const Key24& o) const { return !(*this == o); }
};

// ============================================================================
// Hash functors - all use hardware CRC32 for maximum throughput
// ============================================================================

// CRC-based hash for Key8 (uint64_t). Faster than StdHashWithSeed which uses std::hash.
template <PhmapSeed seed>
struct Key8CrcHash {
    std::size_t operator()(Key8 key) const {
        return phmap_mix_with_seed<sizeof(size_t), seed>()(crc_hash_uint64(key, seed));
    }
};

// CRC-based hash for Key16 (int128_t). Uses hardware CRC instead of hash_combine.
template <PhmapSeed seed>
struct Key16CrcHash {
    std::size_t operator()(Key16 value) const {
        uint64_t lo = static_cast<uint64_t>(value);
        uint64_t hi = static_cast<uint64_t>(static_cast<uint128_t>(value) >> 64);
        return phmap_mix_with_seed<sizeof(size_t), seed>()(crc_hash_uint128(lo, hi, seed));
    }
};

// CRC-based hash for Key24 (3 x uint64_t).
template <PhmapSeed seed>
struct Key24Hash {
    std::size_t operator()(const Key24& key) const {
        uint64_t h = crc_hash_uint64(key.v[0], seed);
        h = crc_hash_uint64(key.v[1], h);
        h = crc_hash_uint64(key.v[2], h);
        return phmap_mix_with_seed<sizeof(size_t), seed>()(h);
    }
};

// ============================================================================
// Key conversion functions
// ============================================================================

ALWAYS_INLINE inline uint16_t slice_to_s0_key(const Slice& s) {
    uint16_t key = 0;
    if (s.size >= 1) key = static_cast<uint8_t>(s.data[0]);
    if (s.size >= 2) key = (key << 8) | static_cast<uint8_t>(s.data[1]);
    return key;
}

// SAHA memory loading optimization: always load exactly 8 bytes, then shift
// out garbage bits. For the tail chunk (where length < 8), we load 8 bytes
// ending at the string's end position: memcpy(&n, data + size - 8, 8), then
// right-shift to remove leading garbage. This avoids variable-length memcpy.
// The string must have at least 8 bytes of readable memory at data+size-8,
// which is guaranteed by StarRocks' column memory layout (padding exists).

ALWAYS_INLINE inline Key8 slice_to_key8(const Slice& s) {
    // len 3-8: load 8 bytes from the end, shift right to clear leading garbage
    uint64_t n = 0;
    memcpy(&n, s.data + s.size - 8, 8);
    int tail_bits = (8 - s.size) * 8;
    n >>= tail_bits;
    // Encode length in highest byte for uniqueness
    n |= (static_cast<uint64_t>(s.size) << 56);
    return n;
}

ALWAYS_INLINE inline Key16 slice_to_key16(const Slice& s) {
    // len 9-16: first 8 bytes loaded directly; last 8 bytes from end, shifted
    uint64_t lo = 0, hi = 0;
    memcpy(&lo, s.data, 8);
    memcpy(&hi, s.data + s.size - 8, 8);
    int tail_bits = (16 - s.size) * 8;
    hi >>= tail_bits;
    hi |= (static_cast<uint64_t>(s.size) << 56);
    return (static_cast<int128_t>(hi) << 64) | lo;
}

ALWAYS_INLINE inline Key24 slice_to_key24(const Slice& s) {
    // len 17-24: first 16 bytes loaded directly; last 8 bytes from end, shifted
    Key24 key;
    memcpy(&key.v[0], s.data, 8);
    memcpy(&key.v[1], s.data + 8, 8);
    memcpy(&key.v[2], s.data + s.size - 8, 8);
    int tail_bits = (24 - s.size) * 8;
    key.v[2] >>= tail_bits;
    key.v[2] |= (static_cast<uint64_t>(s.size) << 56);
    return key;
}

// ============================================================================
// S0Array: Direct-mapped array for strings of length 0-2 (65536 entries)
// ============================================================================

template <typename Value>
struct S0Array {
    Value* _slots = nullptr;
    size_t _size = 0;

    S0Array() = default;
    ~S0Array() { delete[] _slots; }
    S0Array(const S0Array&) = delete;
    S0Array& operator=(const S0Array&) = delete;
    S0Array(S0Array&& o) noexcept : _slots(o._slots), _size(o._size) {
        o._slots = nullptr;
        o._size = 0;
    }

    static constexpr size_t kCapacity = 65536;

    void ensure_init() {
        if (UNLIKELY(_slots == nullptr)) {
            _slots = new Value[kCapacity]();
        }
    }

    bool initialized() const { return _slots != nullptr; }

    // Returns pointer to the value slot. Sets *inserted=true if new.
    ALWAYS_INLINE Value* emplace(uint16_t key, bool* inserted) {
        ensure_init();
        Value& slot = _slots[key];
        if (slot == Value{}) {
            *inserted = true;
            _size++;
        } else {
            *inserted = false;
        }
        return &slot;
    }

    ALWAYS_INLINE Value* find(uint16_t key) {
        if (!_slots) return nullptr;
        Value& slot = _slots[key];
        return (slot != Value{}) ? &slot : nullptr;
    }

    size_t size() const { return _size; }
    size_t capacity() const { return _slots ? kCapacity : 0; }

    void clear() {
        if (_slots) {
            memset(_slots, 0, sizeof(Value) * kCapacity);
            _size = 0;
        }
    }

    // Iterate all non-empty entries
    template <typename F>
    void for_each_value(F&& f) {
        if (!_slots) return;
        for (size_t i = 0; i < kCapacity; i++) {
            if (_slots[i] != Value{}) {
                f(_slots[i]);
            }
        }
    }
};

// ============================================================================
// SAHAMultiMap: String Adaptive Hash Map with phmap-like API
// ============================================================================
//
// Usage (standalone, for benchmarking):
//   SAHAMultiMap<PhmapSeed1> map;
//   auto [vp, inserted] = map.emplace(some_slice);
//   if (inserted) *vp = my_value;
//   auto* found = map.find(some_slice);
//
// Usage (with lazy construction):
//   map.lazy_emplace(key, [&](AggDataPtr& val) {
//       val = allocate_something();
//   });

template <PhmapSeed seed>
class SAHAMultiMap {
public:
    using key_type = Slice;
    using mapped_type = AggDataPtr;

    using S1Map = phmap::flat_hash_map<Key8, mapped_type, Key8CrcHash<seed>>;
    using S2Map = phmap::flat_hash_map<Key16, mapped_type, Key16CrcHash<seed>>;
    using S3Map = phmap::flat_hash_map<Key24, mapped_type, Key24Hash<seed>>;
    using LMap = phmap::flat_hash_map<Slice, mapped_type, SliceHashWithSeed<seed>, SliceEqual>;

    // ====================================================================
    // Core API: emplace / lazy_emplace / find
    // ====================================================================

    ALWAYS_INLINE std::pair<mapped_type*, bool> emplace(const Slice& key) {
        bool inserted = false;
        mapped_type* vp = _dispatch_emplace(key, &inserted);
        return {vp, inserted};
    }

    template <typename F>
    ALWAYS_INLINE mapped_type* lazy_emplace(const Slice& key, F&& f) {
        return _dispatch_lazy_emplace(key, std::forward<F>(f));
    }

    ALWAYS_INLINE mapped_type* find(const Slice& key) { return _dispatch_find(key); }

    ALWAYS_INLINE const mapped_type* find(const Slice& key) const {
        return const_cast<SAHAMultiMap*>(this)->find(key);
    }

    // ====================================================================
    // Capacity / size
    // ====================================================================

    size_t size() const { return _s0.size() + _s1.size() + _s2.size() + _s3.size() + _long.size(); }

    size_t capacity() const {
        return _s0.capacity() + _s1.capacity() + _s2.capacity() + _s3.capacity() + _long.capacity();
    }

    size_t bucket_count() const {
        return _s0.capacity() + _s1.bucket_count() + _s2.bucket_count() + _s3.bucket_count() + _long.bucket_count();
    }

    size_t dump_bound() const {
        return _s0.capacity() + _s1.capacity() + _s2.capacity() + _s3.capacity() + _long.capacity();
    }

    void clear() {
        _s0.clear();
        _s1.clear();
        _s2.clear();
        _s3.clear();
        _long.clear();
    }

    void reserve(size_t n) {
        _s1.reserve(n / 2);
        _long.reserve(n / 4);
    }

    // ====================================================================
    // Iteration: visit all values (for convert_to_two_level etc.)
    // ====================================================================

    template <typename F>
    void for_each_value(F&& f) {
        _s0.for_each_value(f);
        for (auto& [_, v] : _s1) f(v);
        for (auto& [_, v] : _s2) f(v);
        for (auto& [_, v] : _s3) f(v);
        for (auto& [_, v] : _long) f(v);
    }

    // Iterate the long-string sub-table (for direct transfer to two-level map)
    LMap& long_map() { return _long; }

private:
    // ====================================================================
    // Dispatch: route key to the correct sub-table by string length
    // ====================================================================

    // Dispatch using switch with case ranges (GCC/Clang extension).
    // The compiler generates a jump table for lengths 0-24, and a single
    // bounds-check + jump for lengths >24 (the default case).
    // Dispatch using switch with case ranges. Each sub-table uses its own
    // optimized hash function (CRC-based for integer keys, SliceHash for long).
    // Route strings with '\0' to _long (Slice-based) to avoid key collisions in S0-S3.
    ALWAYS_INLINE bool _use_short_path(const Slice& key) {
        return key.size <= 24 && memchr(key.data, '\0', key.size) == nullptr;
    }

    ALWAYS_INLINE mapped_type* _dispatch_emplace(const Slice& key, bool* inserted) {
        if (LIKELY(_use_short_path(key))) {
            switch (key.size) {
            case 0 ... 2:
                return _emplace_s0(key, inserted);
            case 3 ... 8:
                return _emplace_phmap(_s1, slice_to_key8(key), inserted);
            case 9 ... 16:
                return _emplace_phmap(_s2, slice_to_key16(key), inserted);
            default: // 17-24
                return _emplace_phmap(_s3, slice_to_key24(key), inserted);
            }
        }
        return _emplace_phmap(_long, key, inserted);
    }

    template <typename F>
    ALWAYS_INLINE mapped_type* _dispatch_lazy_emplace(const Slice& key, F&& f) {
        if (LIKELY(_use_short_path(key))) {
            switch (key.size) {
            case 0 ... 2:
                return _lazy_emplace_s0(key, std::forward<F>(f));
            case 3 ... 8:
                return _lazy_emplace_phmap(_s1, slice_to_key8(key), std::forward<F>(f));
            case 9 ... 16:
                return _lazy_emplace_phmap(_s2, slice_to_key16(key), std::forward<F>(f));
            default: // 17-24
                return _lazy_emplace_phmap(_s3, slice_to_key24(key), std::forward<F>(f));
            }
        }
        auto iter = _long.lazy_emplace(key, [&](const auto& ctor) {
            mapped_type val{};
            f(val);
            ctor(key, val);
        });
        return &iter->second;
    }

    ALWAYS_INLINE mapped_type* _dispatch_find(const Slice& key) {
        if (LIKELY(_use_short_path(key))) {
            switch (key.size) {
            case 0 ... 2:
                return _s0.find(slice_to_s0_key(key));
            case 3 ... 8:
                return _find_phmap(_s1, slice_to_key8(key));
            case 9 ... 16:
                return _find_phmap(_s2, slice_to_key16(key));
            default: // 17-24
                return _find_phmap(_s3, slice_to_key24(key));
            }
        }
        auto it = _long.find(key);
        return (it != _long.end()) ? &it->second : nullptr;
    }

    // ====================================================================
    // Per-sub-table emplace / find
    // ====================================================================

    ALWAYS_INLINE mapped_type* _emplace_s0(const Slice& key, bool* inserted) {
        uint16_t sk = slice_to_s0_key(key);
        return _s0.emplace(sk, inserted);
    }

    template <typename Map, typename K>
    ALWAYS_INLINE mapped_type* _emplace_phmap(Map& map, const K& sub_key, bool* inserted) {
        auto iter = map.lazy_emplace(sub_key, [&](const auto& ctor) {
            *inserted = true;
            ctor(sub_key, mapped_type{});
        });
        return &iter->second;
    }

    template <typename F>
    ALWAYS_INLINE mapped_type* _lazy_emplace_s0(const Slice& key, F&& f) {
        uint16_t sk = slice_to_s0_key(key);
        _s0.ensure_init();
        auto& slot = _s0._slots[sk];
        if (slot == mapped_type{}) {
            _s0._size++;
            f(slot);
        }
        return &slot;
    }

    template <typename Map, typename K, typename F>
    ALWAYS_INLINE mapped_type* _lazy_emplace_phmap(Map& map, const K& sub_key, F&& f) {
        auto iter = map.lazy_emplace(sub_key, [&](const auto& ctor) {
            mapped_type val{};
            f(val);
            ctor(sub_key, val);
        });
        return &iter->second;
    }

    template <typename Map, typename K>
    ALWAYS_INLINE mapped_type* _find_phmap(Map& map, const K& sub_key) {
        auto it = map.find(sub_key);
        return (it != map.end()) ? &it->second : nullptr;
    }

    // ====================================================================
    // Sub-tables
    // ====================================================================

    // Sub-tables are public for direct access by AggHashMapWithOneStringKeyAdaptive's
    // per-bucket processors and for benchmarking.
public:
    S0Array<mapped_type> _s0;
    S1Map _s1;
    S2Map _s2;
    S3Map _s3;
    LMap _long;
};

// ============================================================================
// AggHashMapWithOneStringKeyAdaptive: SAHA-based aggregation hash map
// ============================================================================

template <PhmapSeed seed, bool is_nullable>
struct AggHashMapWithOneStringKeyAdaptive
        : public AggHashMapWithKey<SAHAMultiMap<seed>, AggHashMapWithOneStringKeyAdaptive<seed, is_nullable>> {
    using Self = AggHashMapWithOneStringKeyAdaptive<seed, is_nullable>;
    using Base = AggHashMapWithKey<SAHAMultiMap<seed>, Self>;
    using KeyType = Slice;
    using ResultVector = Buffer<Slice>;

    template <class... Args>
    AggHashMapWithOneStringKeyAdaptive(Args&&... args) : Base(std::forward<Args>(args)...) {}

    AggDataPtr get_null_key_data() { return null_key_data; }
    void set_null_key_data(AggDataPtr data) { null_key_data = data; }

    // ========================================================================
    // compute_agg_states: entry point
    // ========================================================================

    template <AllocFunc<Self> Func, typename HTBuildOp>
    void compute_agg_states(size_t chunk_size, const Columns& key_columns, MemPool* pool, Func&& allocate_func,
                            Buffer<AggDataPtr>* agg_states, ExtraAggParam* extra) {
        const auto* key_column = key_columns[0].get();
        if constexpr (is_nullable) {
            compute_agg_states_nullable<Func, HTBuildOp>(chunk_size, key_column, pool,
                                                          std::forward<Func>(allocate_func), agg_states, extra);
        } else {
            compute_agg_states_non_nullable<Func, HTBuildOp>(chunk_size, key_column, pool,
                                                              std::forward<Func>(allocate_func), agg_states, extra);
        }
    }

    template <AllocFunc<Self> Func, typename HTBuildOp>
    ALWAYS_NOINLINE void compute_agg_states_non_nullable(size_t chunk_size, const Column* key_column, MemPool* pool,
                                                         Func&& allocate_func, Buffer<AggDataPtr>* agg_states,
                                                         ExtraAggParam* extra) {
        DCHECK(key_column->is_binary());
        const auto* column = down_cast<const BinaryColumn*>(key_column);
        _dispatch_loop<Func, HTBuildOp>(column->size(), column, pool, std::forward<Func>(allocate_func), agg_states,
                                        extra);
    }

    template <AllocFunc<Self> Func, typename HTBuildOp>
    ALWAYS_NOINLINE void compute_agg_states_nullable(size_t chunk_size, const Column* key_column, MemPool* pool,
                                                     Func&& allocate_func, Buffer<AggDataPtr>* agg_states,
                                                     ExtraAggParam* extra) {
        if (key_column->only_null()) {
            if (null_key_data == nullptr) {
                null_key_data = allocate_func(nullptr);
            }
            for (size_t i = 0; i < chunk_size; i++) {
                (*agg_states)[i] = null_key_data;
            }
        } else {
            DCHECK(key_column->is_nullable());
            const auto* nullable_column = down_cast<const NullableColumn*>(key_column);
            const auto* data_column = down_cast<const BinaryColumn*>(nullable_column->data_column().get());

            if (!nullable_column->has_null()) {
                compute_agg_states_non_nullable<Func, HTBuildOp>(chunk_size, data_column, pool,
                                                                  std::forward<Func>(allocate_func), agg_states, extra);
            } else {
                _dispatch_loop_nullable<Func, HTBuildOp>(chunk_size, nullable_column, data_column, pool,
                                                         std::forward<Func>(allocate_func), agg_states, extra);
            }
        }
    }

    // ========================================================================
    // Core dispatch loops with batch classification
    // ========================================================================
    //
    // Instead of per-row switch dispatch (which bloats the hot loop with all
    // sub-table code paths), we classify rows by string length first, then
    // process each bucket in a tight loop touching only one sub-table.
    //
    // Benefits:
    // 1. Each inner loop has only ONE sub-table's code → no icache bloat
    // 2. Branch predictor sees uniform patterns within each bucket
    // 3. Same sub-table accessed consecutively → better dcache locality

    static constexpr uint8_t kBucketS0 = 0;
    static constexpr uint8_t kBucketS1 = 1;
    static constexpr uint8_t kBucketS2 = 2;
    static constexpr uint8_t kBucketS3 = 3;
    static constexpr uint8_t kBucketL = 4;

    // Classify a string key into the appropriate sub-table bucket.
    // Strings containing '\0' are routed to L (Slice-based sub-map) because S0-S3
    // use integer key representations where '\0' bytes are indistinguishable from
    // zero-padding, which would cause key collisions between strings of different lengths.
    static ALWAYS_INLINE uint8_t _classify(const Slice& s) {
        if (s.size > 24) return kBucketL;
        if (UNLIKELY(memchr(s.data, '\0', s.size) != nullptr)) return kBucketL;
        if (s.size <= 2) return kBucketS0;
        if (s.size <= 8) return kBucketS1;
        if (s.size <= 16) return kBucketS2;
        return kBucketS3;
    }

    template <AllocFunc<Self> Func, typename HTBuildOp>
    ALWAYS_NOINLINE void _dispatch_loop(size_t num_rows, const BinaryColumn* column, MemPool* pool,
                                        Func&& allocate_func, Buffer<AggDataPtr>* agg_states, ExtraAggParam* extra) {
        if constexpr (HTBuildOp::process_limit) {
            _dispatch_loop_perrow<Func, HTBuildOp>(num_rows, column, pool, std::forward<Func>(allocate_func),
                                                   agg_states, extra);
        } else {
            _dispatch_loop_batched<Func, HTBuildOp>(num_rows, column, pool, std::forward<Func>(allocate_func),
                                                    agg_states, extra);
        }
    }

    template <AllocFunc<Self> Func, typename HTBuildOp>
    ALWAYS_NOINLINE void _dispatch_loop_nullable(size_t chunk_size, const NullableColumn* nullable_column,
                                                 const BinaryColumn* data_column, MemPool* pool, Func&& allocate_func,
                                                 Buffer<AggDataPtr>* agg_states, ExtraAggParam* extra) {
        if constexpr (HTBuildOp::process_limit) {
            _dispatch_loop_nullable_perrow<Func, HTBuildOp>(chunk_size, nullable_column, data_column, pool,
                                                            std::forward<Func>(allocate_func), agg_states, extra);
        } else {
            _dispatch_loop_nullable_batched<Func, HTBuildOp>(chunk_size, nullable_column, data_column, pool,
                                                             std::forward<Func>(allocate_func), agg_states, extra);
        }
    }

    // ---- Batched path: classify then process per-bucket ----

    template <AllocFunc<Self> Func, typename HTBuildOp>
    ALWAYS_NOINLINE void _dispatch_loop_batched(size_t num_rows, const BinaryColumn* column, MemPool* pool,
                                                Func&& allocate_func, Buffer<AggDataPtr>* agg_states,
                                                ExtraAggParam* extra) {
        // Phase 1: classify rows into buckets.
        // Must NOT alias agg_states->data(): Phase 2 writes 8-byte AggDataPtr values which would
        // corrupt bucket bytes at positions [i*8, i*8+7], causing later processors to see garbage
        // bucket classifications and leave some agg_states[j] as null -> SIGSEGV.
        _bucket_buf.resize(num_rows);
        auto* __restrict buckets = _bucket_buf.data();
        for (size_t i = 0; i < num_rows; i++) {
            buckets[i] = _classify(column->get_slice(i));
        }

        // Phase 2: process each bucket in a tight loop
        _process_bucket_s0<Func, HTBuildOp>(num_rows, column, buckets, pool,
                                            std::forward<Func>(allocate_func), agg_states, extra);
        _process_bucket_s1<Func, HTBuildOp>(num_rows, column, buckets, pool,
                                            std::forward<Func>(allocate_func), agg_states, extra);
        _process_bucket_s2<Func, HTBuildOp>(num_rows, column, buckets, pool,
                                            std::forward<Func>(allocate_func), agg_states, extra);
        _process_bucket_s3<Func, HTBuildOp>(num_rows, column, buckets, pool,
                                            std::forward<Func>(allocate_func), agg_states, extra);
        _process_bucket_long<Func, HTBuildOp>(num_rows, column, buckets, pool,
                                              std::forward<Func>(allocate_func), agg_states, extra);
    }

    template <AllocFunc<Self> Func, typename HTBuildOp>
    ALWAYS_NOINLINE void _dispatch_loop_nullable_batched(size_t chunk_size, const NullableColumn* nullable_column,
                                                         const BinaryColumn* data_column, MemPool* pool,
                                                         Func&& allocate_func, Buffer<AggDataPtr>* agg_states,
                                                         ExtraAggParam* extra) {
        const auto& null_data = nullable_column->null_column_data();
        // Same aliasing issue as non-nullable path: use a separate buffer.
        _bucket_buf.resize(chunk_size);
        auto* __restrict buckets = _bucket_buf.data();
        for (size_t i = 0; i < chunk_size; i++) {
            buckets[i] = null_data[i] ? 0xFF : _classify(data_column->get_slice(i));
        }
        // Handle nulls
        for (size_t i = 0; i < chunk_size; i++) {
            if (buckets[i] == 0xFF) {
                if (UNLIKELY(null_key_data == nullptr)) {
                    null_key_data = allocate_func(nullptr);
                }
                (*agg_states)[i] = null_key_data;
            }
        }
        _process_bucket_s0<Func, HTBuildOp>(chunk_size, data_column, buckets, pool,
                                            std::forward<Func>(allocate_func), agg_states, extra);
        _process_bucket_s1<Func, HTBuildOp>(chunk_size, data_column, buckets, pool,
                                            std::forward<Func>(allocate_func), agg_states, extra);
        _process_bucket_s2<Func, HTBuildOp>(chunk_size, data_column, buckets, pool,
                                            std::forward<Func>(allocate_func), agg_states, extra);
        _process_bucket_s3<Func, HTBuildOp>(chunk_size, data_column, buckets, pool,
                                            std::forward<Func>(allocate_func), agg_states, extra);
        _process_bucket_long<Func, HTBuildOp>(chunk_size, data_column, buckets, pool,
                                              std::forward<Func>(allocate_func), agg_states, extra);
    }

    // ---- Per-row fallback path (for process_limit) ----

    template <AllocFunc<Self> Func, typename HTBuildOp>
    ALWAYS_NOINLINE void _dispatch_loop_perrow(size_t num_rows, const BinaryColumn* column, MemPool* pool,
                                               Func&& allocate_func, Buffer<AggDataPtr>* agg_states,
                                               ExtraAggParam* extra) {
        [[maybe_unused]] size_t hash_table_size = this->hash_map.size();
        auto* __restrict not_founds = extra->not_founds;
        for (size_t i = 0; i < num_rows; i++) {
            auto key = column->get_slice(i);
            _process_one_key<Func, HTBuildOp>(key, pool, std::forward<Func>(allocate_func), (*agg_states)[i],
                                              not_founds, i, hash_table_size, extra);
        }
    }

    template <AllocFunc<Self> Func, typename HTBuildOp>
    ALWAYS_NOINLINE void _dispatch_loop_nullable_perrow(size_t chunk_size, const NullableColumn* nullable_column,
                                                        const BinaryColumn* data_column, MemPool* pool,
                                                        Func&& allocate_func, Buffer<AggDataPtr>* agg_states,
                                                        ExtraAggParam* extra) {
        [[maybe_unused]] size_t hash_table_size = this->hash_map.size();
        auto* __restrict not_founds = extra->not_founds;
        const auto& null_data = nullable_column->null_column_data();
        for (size_t i = 0; i < chunk_size; i++) {
            if (null_data[i]) {
                if (UNLIKELY(null_key_data == nullptr)) {
                    null_key_data = allocate_func(nullptr);
                }
                (*agg_states)[i] = null_key_data;
            } else {
                auto key = data_column->get_slice(i);
                _process_one_key<Func, HTBuildOp>(key, pool, std::forward<Func>(allocate_func), (*agg_states)[i],
                                                  not_founds, i, hash_table_size, extra);
            }
        }
    }

    // ---- Long-only path: SAHA disabled, use _long for all strings (original behavior) ----

    // ========================================================================
    // Per-bucket processing: each is NOINLINE to isolate sub-table code
    // ========================================================================

    // S0 bucket: direct array lookup, no hashing
    template <AllocFunc<Self> Func, typename HTBuildOp>
    ALWAYS_NOINLINE void _process_bucket_s0(size_t n, const BinaryColumn* column, const uint8_t* __restrict buckets,
                                            MemPool* pool, Func&& allocate_func, Buffer<AggDataPtr>* agg_states,
                                            ExtraAggParam* extra) {
        for (size_t i = 0; i < n; i++) {
            if (buckets[i] != kBucketS0) continue;
            auto key = column->get_slice(i);
            uint16_t sk = slice_to_s0_key(key);
            this->hash_map._s0.ensure_init();
            auto& slot = this->hash_map._s0._slots[sk];
            _process_slot<Func, HTBuildOp>(slot, key, pool, std::forward<Func>(allocate_func),
                                           (*agg_states)[i], extra->not_founds, i);
        }
    }

    // S1 bucket: Key8 (uint64_t) sub-table
    template <AllocFunc<Self> Func, typename HTBuildOp>
    ALWAYS_NOINLINE void _process_bucket_s1(size_t n, const BinaryColumn* column, const uint8_t* __restrict buckets,
                                            MemPool* pool, Func&& allocate_func, Buffer<AggDataPtr>* agg_states,
                                            ExtraAggParam* extra) {
        for (size_t i = 0; i < n; i++) {
            if (buckets[i] != kBucketS1) continue;
            auto key = column->get_slice(i);
            _process_phmap_key<Func, HTBuildOp>(this->hash_map._s1, slice_to_key8(key), key, pool,
                                                std::forward<Func>(allocate_func), (*agg_states)[i],
                                                extra->not_founds, i);
        }
    }

    // S2 bucket: Key16 (int128_t) sub-table
    template <AllocFunc<Self> Func, typename HTBuildOp>
    ALWAYS_NOINLINE void _process_bucket_s2(size_t n, const BinaryColumn* column, const uint8_t* __restrict buckets,
                                            MemPool* pool, Func&& allocate_func, Buffer<AggDataPtr>* agg_states,
                                            ExtraAggParam* extra) {
        for (size_t i = 0; i < n; i++) {
            if (buckets[i] != kBucketS2) continue;
            auto key = column->get_slice(i);
            _process_phmap_key<Func, HTBuildOp>(this->hash_map._s2, slice_to_key16(key), key, pool,
                                                std::forward<Func>(allocate_func), (*agg_states)[i],
                                                extra->not_founds, i);
        }
    }

    // S3 bucket: Key24 sub-table
    template <AllocFunc<Self> Func, typename HTBuildOp>
    ALWAYS_NOINLINE void _process_bucket_s3(size_t n, const BinaryColumn* column, const uint8_t* __restrict buckets,
                                            MemPool* pool, Func&& allocate_func, Buffer<AggDataPtr>* agg_states,
                                            ExtraAggParam* extra) {
        for (size_t i = 0; i < n; i++) {
            if (buckets[i] != kBucketS3) continue;
            auto key = column->get_slice(i);
            _process_phmap_key<Func, HTBuildOp>(this->hash_map._s3, slice_to_key24(key), key, pool,
                                                std::forward<Func>(allocate_func), (*agg_states)[i],
                                                extra->not_founds, i);
        }
    }

    // Long bucket: Slice sub-table (same as baseline)
    template <AllocFunc<Self> Func, typename HTBuildOp>
    ALWAYS_NOINLINE void _process_bucket_long(size_t n, const BinaryColumn* column, const uint8_t* __restrict buckets,
                                              MemPool* pool, Func&& allocate_func, Buffer<AggDataPtr>* agg_states,
                                              ExtraAggParam* extra) {
        for (size_t i = 0; i < n; i++) {
            if (buckets[i] != kBucketL) continue;
            auto key = column->get_slice(i);
            _process_phmap_key<Func, HTBuildOp>(this->hash_map._long, key, key, pool,
                                                std::forward<Func>(allocate_func), (*agg_states)[i],
                                                extra->not_founds, i);
        }
    }

    // Common logic for S0 slot-based emplace
    template <AllocFunc<Self> Func, typename HTBuildOp>
    ALWAYS_INLINE void _process_slot(AggDataPtr& slot, const Slice& key, MemPool* pool, Func&& allocate_func,
                                     AggDataPtr& target_state, Filter* not_founds, size_t row_idx) {
        if constexpr (HTBuildOp::allocate) {
            if (slot == nullptr) {
                this->hash_map._s0._size++;
                if constexpr (HTBuildOp::fill_not_found) {
                    (*not_founds)[row_idx] = 1;
                }
                slot = _alloc_with_pool(key, pool, std::forward<Func>(allocate_func));
            }
            target_state = slot;
        } else if constexpr (HTBuildOp::fill_not_found) {
            if (slot != nullptr) {
                target_state = slot;
            } else {
                (*not_founds)[row_idx] = 1;
            }
        }
    }

    // Common logic for phmap sub-table emplace/find.
    // Only handles allocate and fill_not_found modes (process_limit uses perrow path).
    template <AllocFunc<Self> Func, typename HTBuildOp, typename Map, typename SubKey>
    ALWAYS_INLINE void _process_phmap_key(Map& map, const SubKey& sub_key, const Slice& key, MemPool* pool,
                                          Func&& allocate_func, AggDataPtr& target_state, Filter* not_founds,
                                          size_t row_idx) {
        if constexpr (HTBuildOp::allocate) {
            auto iter = map.lazy_emplace(sub_key, [&](const auto& ctor) {
                if constexpr (HTBuildOp::fill_not_found) {
                    (*not_founds)[row_idx] = 1;
                }
                AggDataPtr val = _alloc_with_pool(key, pool, std::forward<Func>(allocate_func));
                ctor(sub_key, val);
            });
            target_state = iter->second;
        } else if constexpr (HTBuildOp::fill_not_found) {
            auto it = map.find(sub_key);
            if (it != map.end()) {
                target_state = it->second;
            } else {
                (*not_founds)[row_idx] = 1;
            }
        }
    }

    // ========================================================================
    // Per-row processing using SAHAMultiMap API
    // ========================================================================

    template <AllocFunc<Self> Func, typename HTBuildOp>
    ALWAYS_INLINE void _process_one_key(const Slice& key, MemPool* pool, Func&& allocate_func,
                                        AggDataPtr& target_state, Filter* not_founds, size_t row_idx,
                                        [[maybe_unused]] size_t& hash_table_size, ExtraAggParam* extra) {
        if constexpr (HTBuildOp::process_limit) {
            if (hash_table_size < extra->limits) {
                auto* vp = this->hash_map.lazy_emplace(key, [&](AggDataPtr& val) {
                    hash_table_size++;
                    val = _alloc_with_pool(key, pool, std::forward<Func>(allocate_func));
                });
                target_state = *vp;
            } else {
                auto* vp = this->hash_map.find(key);
                if (vp) {
                    target_state = *vp;
                } else {
                    (*not_founds)[row_idx] = 1;
                }
            }
        } else if constexpr (HTBuildOp::allocate) {
            auto* vp = this->hash_map.lazy_emplace(key, [&](AggDataPtr& val) {
                if constexpr (HTBuildOp::fill_not_found) {
                    (*not_founds)[row_idx] = 1;
                }
                val = _alloc_with_pool(key, pool, std::forward<Func>(allocate_func));
            });
            target_state = *vp;
        } else if constexpr (HTBuildOp::fill_not_found) {
            auto* vp = this->hash_map.find(key);
            if (vp) {
                target_state = *vp;
            } else {
                (*not_founds)[row_idx] = 1;
            }
        }
    }

    template <AllocFunc<Self> Func>
    ALWAYS_INLINE AggDataPtr _alloc_with_pool(const Slice& key, MemPool* pool, Func&& allocate_func) {
        uint8_t* pos = pool->allocate_with_reserve(key.size, SLICE_MEMEQUAL_OVERFLOW_PADDING);
        strings::memcpy_inlined(pos, key.data, key.size);
        Slice pk{pos, key.size};
        return allocate_func(pk);
    }

    // ========================================================================
    // insert_keys_to_columns: output results
    // ========================================================================

    void insert_keys_to_columns(ResultVector& keys, MutableColumns& key_columns, size_t chunk_size) {
        if constexpr (is_nullable) {
            DCHECK(key_columns[0]->is_nullable());
            auto* nullable_column = down_cast<NullableColumn*>(key_columns[0].get());
            auto* column = down_cast<BinaryColumn*>(nullable_column->data_column_raw_ptr());
            keys.resize(chunk_size);
            column->append_strings(keys.data(), keys.size());
            nullable_column->null_column_data().resize(chunk_size);
        } else {
            DCHECK(!null_key_data);
            auto* column = down_cast<BinaryColumn*>(key_columns[0].get());
            keys.resize(chunk_size);
            column->append_strings(keys.data(), keys.size());
        }
    }

    static constexpr bool has_single_null_key = is_nullable;
    AggDataPtr null_key_data = nullptr;
    ResultVector results;

private:
    // Scratch buffer for bucket classification in batched dispatch.
    // Stored as a member to avoid per-chunk allocation (resize is amortized O(1)).
    std::vector<uint8_t> _bucket_buf;
};

} // namespace starrocks
