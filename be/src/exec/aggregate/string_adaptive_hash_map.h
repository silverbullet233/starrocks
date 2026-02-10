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

#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <utility>

#include "base/hash/hash.h"
#include "base/phmap/phmap.h"
#include "base/string/slice.h"
#include "column/column_hash.h"

namespace starrocks {

template <typename Mapped, PhmapSeed seed>
class StringAdaptiveHashMap {
public:
    using key_type = Slice;
    using mapped_type = Mapped;

    struct hasher {
        size_t operator()(const Slice& s) const { return SliceHashWithSeed<seed>()(s); }
    };

    using value_type = std::pair<key_type, mapped_type>;

private:
    using ShortKey2 = uint32_t;
    using ShortKey8 = uint64_t;

    struct ShortKey16 {
        uint64_t lo = 0;
        uint64_t hi = 0;

        bool operator==(const ShortKey16& rhs) const { return lo == rhs.lo && hi == rhs.hi; }
    };

    struct ShortKey24 {
        uint64_t lo = 0;
        uint64_t mid = 0;
        uint64_t hi = 0;

        bool operator==(const ShortKey24& rhs) const { return lo == rhs.lo && mid == rhs.mid && hi == rhs.hi; }
    };

    struct LongKeyWithHash {
        const uint8_t* data = nullptr;
        size_t size = 0;
        size_t hash = 0;
    };

    struct ShortKey2Hash {
        size_t operator()(ShortKey2 key) const {
            uint8_t bytes[2];
            uint16_t payload = static_cast<uint16_t>(key & 0xFFFFU);
            bytes[0] = static_cast<uint8_t>(payload & 0xFFU);
            bytes[1] = static_cast<uint8_t>((payload >> 8U) & 0xFFU);
            const size_t len = (key & (1U << 16U)) ? 1 : 2;
            return SliceHashWithSeed<seed>()(Slice(reinterpret_cast<const char*>(bytes), len));
        }
    };

    struct ShortKey8Hash {
        size_t operator()(ShortKey8 key) const {
            return SliceHashWithSeed<seed>()(Slice(reinterpret_cast<const char*>(&key), _inline_len_u64(key)));
        }
    };

    struct ShortKey16Hash {
        size_t operator()(const ShortKey16& key) const {
            const size_t len = key.hi != 0 ? (8 + _inline_len_u64(key.hi)) : _inline_len_u64(key.lo);
            return SliceHashWithSeed<seed>()(Slice(reinterpret_cast<const char*>(&key), len));
        }
    };

    struct ShortKey24Hash {
        size_t operator()(const ShortKey24& key) const {
            size_t len = 0;
            if (key.hi != 0) {
                len = 16 + _inline_len_u64(key.hi);
            } else if (key.mid != 0) {
                len = 8 + _inline_len_u64(key.mid);
            } else {
                len = _inline_len_u64(key.lo);
            }
            return SliceHashWithSeed<seed>()(Slice(reinterpret_cast<const char*>(&key), len));
        }
    };

    struct LongKeyHash {
        size_t operator()(const LongKeyWithHash& key) const { return key.hash; }
    };

    struct LongKeyEq {
        bool operator()(const LongKeyWithHash& lhs, const LongKeyWithHash& rhs) const {
            return lhs.hash == rhs.hash && lhs.size == rhs.size && std::memcmp(lhs.data, rhs.data, lhs.size) == 0;
        }
    };

    static size_t _inline_len_u64(uint64_t value) {
        if (value == 0) {
            return 0;
        }
        return (64U - static_cast<size_t>(__builtin_clzll(value)) + 7U) / 8U;
    }

    using ShortKey2Map = phmap::flat_hash_map<ShortKey2, mapped_type, ShortKey2Hash>;
    using ShortKey8Map = phmap::flat_hash_map<ShortKey8, mapped_type, ShortKey8Hash>;
    using ShortKey16Map = phmap::flat_hash_map<ShortKey16, mapped_type, ShortKey16Hash>;
    using ShortKey24Map = phmap::flat_hash_map<ShortKey24, mapped_type, ShortKey24Hash>;
    using LongKeyMap = phmap::flat_hash_map<LongKeyWithHash, mapped_type, LongKeyHash, LongKeyEq>;

    static key_type _decode_short2_key(const ShortKey2& key) {
        const size_t len = (key & (1U << 16U)) ? 1 : 2;
        return Slice(reinterpret_cast<const char*>(&key), len);
    }

    static key_type _decode_short8_key(const ShortKey8& key) {
        return Slice(reinterpret_cast<const char*>(&key), _inline_len_u64(key));
    }

    static key_type _decode_short16_key(const ShortKey16& key) {
        const size_t len = key.hi != 0 ? (8 + _inline_len_u64(key.hi)) : _inline_len_u64(key.lo);
        return Slice(reinterpret_cast<const char*>(&key), len);
    }

    static key_type _decode_short24_key(const ShortKey24& key) {
        size_t len = 0;
        if (key.hi != 0) {
            len = 16 + _inline_len_u64(key.hi);
        } else if (key.mid != 0) {
            len = 8 + _inline_len_u64(key.mid);
        } else {
            len = _inline_len_u64(key.lo);
        }
        return Slice(reinterpret_cast<const char*>(&key), len);
    }

    static key_type _decode_long_key(const LongKeyWithHash& key) {
        return Slice(reinterpret_cast<const char*>(key.data), key.size);
    }

public:
    class iterator {
    public:
        using difference_type = std::ptrdiff_t;
        using value_type = StringAdaptiveHashMap::value_type;
        using pointer = value_type*;
        using reference = value_type&;
        using iterator_category = std::forward_iterator_tag;

        iterator() = default;

        reference operator*() const { return *(operator->()); }

        pointer operator->() const {
            switch (_segment) {
            case Segment::kEmpty:
                return &_owner->_empty_value;
            case Segment::kShort2:
            case Segment::kShort8:
            case Segment::kShort16:
            case Segment::kShort24:
            case Segment::kLong:
                return const_cast<pointer>(&_entry);
            case Segment::kEnd:
                return nullptr;
            }
            return nullptr;
        }

        iterator& operator++() {
            _advance();
            return *this;
        }

        iterator operator++(int) {
            iterator tmp = *this;
            ++(*this);
            return tmp;
        }

        bool operator==(const iterator& rhs) const {
            if (_owner != rhs._owner || _segment != rhs._segment) {
                return false;
            }

            switch (_segment) {
            case Segment::kEmpty:
            case Segment::kEnd:
                return true;
            case Segment::kShort2:
            case Segment::kShort8:
            case Segment::kShort16:
            case Segment::kShort24:
            case Segment::kLong:
                return _mapped == rhs._mapped;
            }

            return false;
        }

        bool operator!=(const iterator& rhs) const { return !(*this == rhs); }

    private:
        friend class StringAdaptiveHashMap;

        enum class Segment {
            kEnd,
            kEmpty,
            kShort2,
            kShort8,
            kShort16,
            kShort24,
            kLong,
        };

        explicit iterator(StringAdaptiveHashMap* owner, Segment seg) : _owner(owner), _segment(seg) {}

        static iterator begin(StringAdaptiveHashMap* owner) {
            if (owner == nullptr) {
                return end(owner);
            }

            if (owner->_has_empty_value) {
                return from_empty(owner);
            }

            iterator it(owner, Segment::kEnd);
            if (it._to_short2_begin()) {
                return it;
            }
            if (it._to_short8_begin()) {
                return it;
            }
            if (it._to_short16_begin()) {
                return it;
            }
            if (it._to_short24_begin()) {
                return it;
            }
            if (it._to_long_begin()) {
                return it;
            }

            return end(owner);
        }

        static iterator end(StringAdaptiveHashMap* owner) { return iterator(owner, Segment::kEnd); }

        static iterator from_empty(StringAdaptiveHashMap* owner) { return iterator(owner, Segment::kEmpty); }

        static iterator from_short2(StringAdaptiveHashMap* owner, typename ShortKey2Map::iterator it) {
            iterator out(owner, Segment::kShort2);
            out._set_from_short2_iter(it);
            return out;
        }

        static iterator from_short8(StringAdaptiveHashMap* owner, typename ShortKey8Map::iterator it) {
            iterator out(owner, Segment::kShort8);
            out._set_from_short8_iter(it);
            return out;
        }

        static iterator from_short16(StringAdaptiveHashMap* owner, typename ShortKey16Map::iterator it) {
            iterator out(owner, Segment::kShort16);
            out._set_from_short16_iter(it);
            return out;
        }

        static iterator from_short24(StringAdaptiveHashMap* owner, typename ShortKey24Map::iterator it) {
            iterator out(owner, Segment::kShort24);
            out._set_from_short24_iter(it);
            return out;
        }

        static iterator from_long(StringAdaptiveHashMap* owner, typename LongKeyMap::iterator it) {
            iterator out(owner, Segment::kLong);
            out._set_from_long_iter(it);
            return out;
        }

        void _advance() {
            if (_owner == nullptr) {
                _segment = Segment::kEnd;
                _mapped = nullptr;
                return;
            }

            switch (_segment) {
            case Segment::kEmpty:
                _advance_empty();
                return;
            case Segment::kShort2:
                _advance_short2();
                return;
            case Segment::kShort8:
                _advance_short8();
                return;
            case Segment::kShort16:
                _advance_short16();
                return;
            case Segment::kShort24:
                _advance_short24();
                return;
            case Segment::kLong:
                _advance_long();
                return;
            case Segment::kEnd:
                return;
            }
        }

        void _advance_empty() {
            if (_to_short2_begin()) {
                return;
            }
            if (_to_short8_begin()) {
                return;
            }
            if (_to_short16_begin()) {
                return;
            }
            if (_to_short24_begin()) {
                return;
            }
            if (_to_long_begin()) {
                return;
            }
            _to_end();
        }

        void _advance_short2() {
            if (_mapped == nullptr) {
                _to_end();
                return;
            }

            const size_t hash = _owner->hash_function()(_key);
            auto it = _owner->_short2_map.find(_encode_short2(_key), hash);
            if (it == _owner->_short2_map.end()) {
                _to_end();
                return;
            }

            ++it;
            if (it != _owner->_short2_map.end()) {
                _set_from_short2_iter(it);
                return;
            }
            if (_to_short8_begin()) {
                return;
            }
            if (_to_short16_begin()) {
                return;
            }
            if (_to_short24_begin()) {
                return;
            }
            if (_to_long_begin()) {
                return;
            }
            _to_end();
        }

        void _advance_short8() {
            if (_mapped == nullptr) {
                _to_end();
                return;
            }

            const size_t hash = _owner->hash_function()(_key);
            auto it = _owner->_short8_map.find(_encode_short8(_key), hash);
            if (it == _owner->_short8_map.end()) {
                _to_end();
                return;
            }

            ++it;
            if (it != _owner->_short8_map.end()) {
                _set_from_short8_iter(it);
                return;
            }
            if (_to_short16_begin()) {
                return;
            }
            if (_to_short24_begin()) {
                return;
            }
            if (_to_long_begin()) {
                return;
            }
            _to_end();
        }

        void _advance_short16() {
            if (_mapped == nullptr) {
                _to_end();
                return;
            }

            const size_t hash = _owner->hash_function()(_key);
            auto it = _owner->_short16_map.find(_encode_short16(_key), hash);
            if (it == _owner->_short16_map.end()) {
                _to_end();
                return;
            }

            ++it;
            if (it != _owner->_short16_map.end()) {
                _set_from_short16_iter(it);
                return;
            }
            if (_to_short24_begin()) {
                return;
            }
            if (_to_long_begin()) {
                return;
            }
            _to_end();
        }

        void _advance_short24() {
            if (_mapped == nullptr) {
                _to_end();
                return;
            }

            const size_t hash = _owner->hash_function()(_key);
            auto it = _owner->_short24_map.find(_encode_short24(_key), hash);
            if (it == _owner->_short24_map.end()) {
                _to_end();
                return;
            }

            ++it;
            if (it != _owner->_short24_map.end()) {
                _set_from_short24_iter(it);
                return;
            }
            if (_to_long_begin()) {
                return;
            }
            _to_end();
        }

        void _advance_long() {
            if (_mapped == nullptr) {
                _to_end();
                return;
            }

            const size_t hash = _owner->hash_function()(_key);
            auto it = _owner->_long_map.find(LongKeyWithHash{_as_u8(_key.data), _key.size, hash});
            if (it == _owner->_long_map.end()) {
                _to_end();
                return;
            }

            ++it;
            if (it == _owner->_long_map.end()) {
                _to_end();
                return;
            }
            _set_from_long_iter(it);
        }

        bool _to_short2_begin() {
            auto it = _owner->_short2_map.begin();
            if (it == _owner->_short2_map.end()) {
                return false;
            }
            _set_from_short2_iter(it);
            return true;
        }

        bool _to_short8_begin() {
            auto it = _owner->_short8_map.begin();
            if (it == _owner->_short8_map.end()) {
                return false;
            }
            _set_from_short8_iter(it);
            return true;
        }

        bool _to_short16_begin() {
            auto it = _owner->_short16_map.begin();
            if (it == _owner->_short16_map.end()) {
                return false;
            }
            _set_from_short16_iter(it);
            return true;
        }

        bool _to_short24_begin() {
            auto it = _owner->_short24_map.begin();
            if (it == _owner->_short24_map.end()) {
                return false;
            }
            _set_from_short24_iter(it);
            return true;
        }

        bool _to_long_begin() {
            auto it = _owner->_long_map.begin();
            if (it == _owner->_long_map.end()) {
                return false;
            }
            _set_from_long_iter(it);
            return true;
        }

        void _to_end() {
            _segment = Segment::kEnd;
            _mapped = nullptr;
        }

        void _refresh_entry() {
            if (_mapped != nullptr) {
                _entry = value_type{_key, *_mapped};
            }
        }

        void _set_from_short2_iter(typename ShortKey2Map::iterator it) {
            _segment = Segment::kShort2;
            _mapped = &it->second;
            _key = _decode_short2_key(it->first);
            _refresh_entry();
        }

        void _set_from_short8_iter(typename ShortKey8Map::iterator it) {
            _segment = Segment::kShort8;
            _mapped = &it->second;
            _key = _decode_short8_key(it->first);
            _refresh_entry();
        }

        void _set_from_short16_iter(typename ShortKey16Map::iterator it) {
            _segment = Segment::kShort16;
            _mapped = &it->second;
            _key = _decode_short16_key(it->first);
            _refresh_entry();
        }

        void _set_from_short24_iter(typename ShortKey24Map::iterator it) {
            _segment = Segment::kShort24;
            _mapped = &it->second;
            _key = _decode_short24_key(it->first);
            _refresh_entry();
        }

        void _set_from_long_iter(typename LongKeyMap::iterator it) {
            _segment = Segment::kLong;
            _mapped = &it->second;
            _key = _decode_long_key(it->first);
            _refresh_entry();
        }

    private:
        StringAdaptiveHashMap* _owner = nullptr;
        Segment _segment = Segment::kEnd;
        mapped_type* _mapped = nullptr;
        key_type _key;
        value_type _entry;
    };

    StringAdaptiveHashMap() = default;
    ~StringAdaptiveHashMap() = default;

    hasher hash_function() const { return hasher(); }

    size_t size() const {
        size_t result = _has_empty_value ? 1 : 0;
        result += _map_size(_short2_map);
        result += _map_size(_short8_map);
        result += _map_size(_short16_map);
        result += _map_size(_short24_map);
        result += _map_size(_long_map);
        return result;
    }

    size_t capacity() const {
        size_t result = _map_capacity(_short2_map);
        result += _map_capacity(_short8_map);
        result += _map_capacity(_short16_map);
        result += _map_capacity(_short24_map);
        result += _map_capacity(_long_map);
        return result;
    }

    size_t bucket_count() const {
        size_t result = _map_bucket_count(_short2_map);
        result += _map_bucket_count(_short8_map);
        result += _map_bucket_count(_short16_map);
        result += _map_bucket_count(_short24_map);
        result += _map_bucket_count(_long_map);
        return result;
    }

    bool empty() const { return size() == 0; }

    void clear() {
        _has_empty_value = false;
        _short2_map.clear();
        _short8_map.clear();
        _short16_map.clear();
        _short24_map.clear();
        _long_map.clear();
    }

    void reserve(size_t n) {
        const size_t short2_cap = n / 8;
        const size_t short8_cap = n / 4;
        const size_t short16_cap = n / 4;
        const size_t short24_cap = n / 4;
        const size_t long_cap = n / 4;

        _short2_map.reserve(short2_cap);
        _short8_map.reserve(short8_cap);
        _short16_map.reserve(short16_cap);
        _short24_map.reserve(short24_cap);
        _long_map.reserve(long_cap);
    }

    size_t dump_bound() const {
        size_t result = _has_empty_value ? 1 : 0;
        result += _map_dump_bound(_short2_map);
        result += _map_dump_bound(_short8_map);
        result += _map_dump_bound(_short16_map);
        result += _map_dump_bound(_short24_map);
        result += _map_dump_bound(_long_map);
        return result;
    }

    template <typename F>
    iterator lazy_emplace(const key_type& key, F&& f) {
        return lazy_emplace_with_hash(key, hasher()(key), std::forward<F>(f));
    }

    template <typename F>
    iterator lazy_emplace_with_hash(const key_type& key, size_t hash, F&& f) {
        return _dispatch_by_key(
                key, [&]() { return _lazy_emplace_empty(std::forward<F>(f)); },
                [&](ShortKey2 short_key) {
                    auto it = _short2_map.lazy_emplace_with_hash(short_key, hash, [&](const auto& ctor) {
                        f([&](const key_type& inserted_key, const mapped_type& mapped) {
                            (void)inserted_key;
                            ctor(short_key, mapped);
                        });
                    });
                    return iterator::from_short2(this, it);
                },
                [&](ShortKey8 short_key) {
                    auto it = _short8_map.lazy_emplace_with_hash(short_key, hash, [&](const auto& ctor) {
                        f([&](const key_type& inserted_key, const mapped_type& mapped) {
                            (void)inserted_key;
                            ctor(short_key, mapped);
                        });
                    });
                    return iterator::from_short8(this, it);
                },
                [&](ShortKey16 short_key) {
                    auto it = _short16_map.lazy_emplace_with_hash(short_key, hash, [&](const auto& ctor) {
                        f([&](const key_type& inserted_key, const mapped_type& mapped) {
                            (void)inserted_key;
                            ctor(short_key, mapped);
                        });
                    });
                    return iterator::from_short16(this, it);
                },
                [&](ShortKey24 short_key) {
                    auto it = _short24_map.lazy_emplace_with_hash(short_key, hash, [&](const auto& ctor) {
                        f([&](const key_type& inserted_key, const mapped_type& mapped) {
                            (void)inserted_key;
                            ctor(short_key, mapped);
                        });
                    });
                    return iterator::from_short24(this, it);
                },
                [&]() { return _lazy_emplace_long(key, hash, std::forward<F>(f)); });
    }

    iterator find(const key_type& key) { return find(key, hasher()(key)); }

    iterator find(const key_type& key, size_t hash) {
        return _dispatch_by_key(
                key, [&]() { return _has_empty_value ? iterator::from_empty(this) : end(); },
                [&](ShortKey2 short_key) {
                    auto it = _short2_map.find(short_key, hash);
                    return it == _short2_map.end() ? end() : iterator::from_short2(this, it);
                },
                [&](ShortKey8 short_key) {
                    auto it = _short8_map.find(short_key, hash);
                    return it == _short8_map.end() ? end() : iterator::from_short8(this, it);
                },
                [&](ShortKey16 short_key) {
                    auto it = _short16_map.find(short_key, hash);
                    return it == _short16_map.end() ? end() : iterator::from_short16(this, it);
                },
                [&](ShortKey24 short_key) {
                    auto it = _short24_map.find(short_key, hash);
                    return it == _short24_map.end() ? end() : iterator::from_short24(this, it);
                },
                [&]() {
                    auto it = _long_map.find(LongKeyWithHash{_as_u8(key.data), key.size, hash});
                    return it == _long_map.end() ? end() : iterator::from_long(this, it);
                });
    }

    // V1: no-op hook for compatibility with existing Agg map fast path.
    void prefetch_hash(size_t hash) const { (void)hash; }

    std::pair<iterator, bool> emplace(const key_type& key, const mapped_type& mapped) {
        const size_t old_size = size();
        auto it = lazy_emplace(key, [&](const auto& ctor) { ctor(key, mapped); });
        return {it, size() != old_size};
    }

    iterator begin() { return iterator::begin(this); }

    iterator end() { return iterator::end(this); }

    template <typename InputIt>
    void insert(InputIt first, InputIt last) {
        const hasher h;
        for (; first != last; ++first) {
            const auto& kv = *first;
            lazy_emplace_with_hash(kv.first, h(kv.first), [&](const auto& ctor) { ctor(kv.first, kv.second); });
        }
    }

private:
    template <typename Map>
    static size_t _map_size(const Map& map) {
        if constexpr (requires(const Map& m) { m.size(); }) {
            return map.size();
        } else {
            return const_cast<Map&>(map).size();
        }
    }

    template <typename Map>
    static size_t _map_capacity(const Map& map) {
        if constexpr (requires(const Map& m) { m.capacity(); }) {
            return map.capacity();
        } else {
            return const_cast<Map&>(map).capacity();
        }
    }

    template <typename Map>
    static size_t _map_bucket_count(const Map& map) {
        if constexpr (requires(const Map& m) { m.bucket_count(); }) {
            return map.bucket_count();
        } else {
            return _map_capacity(map);
        }
    }

    template <typename Map>
    static size_t _map_dump_bound(const Map& map) {
        if constexpr (requires(const Map& m) { m.dump_bound(); }) {
            return map.dump_bound();
        } else {
            return const_cast<Map&>(map).dump_bound();
        }
    }

    template <typename F>
    iterator _lazy_emplace_empty(F&& f) {
        if (_has_empty_value) {
            return iterator::from_empty(this);
        }

        f([&](const key_type& inserted_key, const mapped_type& mapped) {
            _empty_value = value_type{inserted_key, mapped};
            _has_empty_value = true;
        });
        return _has_empty_value ? iterator::from_empty(this) : end();
    }

    template <typename F>
    iterator _lazy_emplace_long(const key_type& key, size_t hash, F&& f) {
        LongKeyWithHash long_key{_as_u8(key.data), key.size, hash};
        auto it = _long_map.lazy_emplace_with_hash(long_key, hash, [&](const auto& ctor) {
            f([&](const key_type& inserted_key, const mapped_type& mapped) {
                ctor(LongKeyWithHash{_as_u8(inserted_key.data), inserted_key.size, hash}, mapped);
            });
        });
        return iterator::from_long(this, it);
    }

    template <typename OnEmpty, typename OnShort2, typename OnShort8, typename OnShort16, typename OnShort24,
              typename OnLong>
    static decltype(auto) _dispatch_by_key(const key_type& key, OnEmpty&& on_empty, OnShort2&& on_short2,
                                           OnShort8&& on_short8, OnShort16&& on_short16, OnShort24&& on_short24,
                                           OnLong&& on_long) {
        if (key.size == 0) {
            return std::forward<OnEmpty>(on_empty)();
        }

        if (key.size <= 2) {
            return std::forward<OnShort2>(on_short2)(_encode_short2(key));
        }

        if (_can_use_inline_short_key(key)) {
            if (key.size <= 8) {
                return std::forward<OnShort8>(on_short8)(_encode_short8(key));
            }

            if (key.size <= 16) {
                return std::forward<OnShort16>(on_short16)(_encode_short16(key));
            }

            if (key.size <= 24) {
                return std::forward<OnShort24>(on_short24)(_encode_short24(key));
            }
        }

        return std::forward<OnLong>(on_long)();
    }

    static ShortKey2 _encode_short2(const key_type& key) {
        uint16_t payload = 0;
        std::memcpy(&payload, key.data, key.size);
        return key.size == 2 ? static_cast<ShortKey2>(payload) : (static_cast<ShortKey2>(1U << 16) | payload);
    }

    static bool _can_use_inline_short_key(const key_type& key) {
        return key.size > 0 && static_cast<uint8_t>(key.data[key.size - 1]) != 0;
    }

    static ShortKey8 _encode_short8(const key_type& key) {
        ShortKey8 out = 0;
        std::memcpy(&out, key.data, key.size);
        return out;
    }

    static ShortKey16 _encode_short16(const key_type& key) {
        ShortKey16 out;
        std::memcpy(&out.lo, key.data, 8);
        std::memcpy(&out.hi, key.data + 8, key.size - 8);
        return out;
    }

    static ShortKey24 _encode_short24(const key_type& key) {
        ShortKey24 out;
        std::memcpy(&out.lo, key.data, 8);
        std::memcpy(&out.mid, key.data + 8, 8);
        std::memcpy(&out.hi, key.data + 16, key.size - 16);
        return out;
    }

    static const uint8_t* _as_u8(const void* data) { return reinterpret_cast<const uint8_t*>(data); }

private:
    bool _has_empty_value = false;
    value_type _empty_value;

    ShortKey2Map _short2_map;
    ShortKey8Map _short8_map;
    ShortKey16Map _short16_map;
    ShortKey24Map _short24_map;
    LongKeyMap _long_map;
};

} // namespace starrocks
