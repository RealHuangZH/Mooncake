#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <list>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "tiered_cache/scheduler/stats_collector.h"
#include "utils.h"

namespace mooncake {

/**
 * @enum MultiLruBucket
 * @brief Frequency tier a key currently occupies. Ordered coldest-first so the
 *        enum value doubles as an index into the per-bucket LRU lists and as
 *        the natural eviction order.
 */
enum class MultiLruBucket : size_t {
    kCold = 0,
    kWarm = 1,
    kHot = 2,
    kVeryHot = 3,
};

/**
 * @class MultiLRUStatsCollector
 * @brief Frequency-aware stats collector backing the event-driven MultiLRU
 *        policy.
 *
 * Two cooperating structures:
 *  - A **TinyLFU** frequency estimator (4-row Count-Min Sketch with 4-bit
 *    saturating counters guarded by a Doorkeeper bloom filter). It is updated
 *    lock-free on every `RecordAccess`, so `EstimateFrequency` always reflects
 *    the latest hits — this is what the offload/onboard decisions read on the
 *    consumer threads. Counters age (halve) and the doorkeeper clears once
 *    `sample_window` accesses have accumulated, preventing saturation.
 *  - A **4-level MultiLRU** (one intrusive LRU list per frequency bucket) that
 *    orders keys for eviction (cold→very_hot, LRU-tail first within a bucket).
 *    It is maintained off the hot path: `RecordAccess` only appends the key to
 *    a sharded pending buffer; the aggregate lists are reconciled lazily under
 *    `aggregate_mutex_` whenever the single worker thread calls `GetSnapshot`
 *    or `EvictionCandidates`.
 *
 * Hot path (`RecordAccess`) therefore costs an atomic sketch bump plus a
 * sharded set insert — no global lock — satisfying the "zero blocking on the
 * hot path" requirement.
 */
class MultiLRUStatsCollector : public StatsCollector {
   public:
    static constexpr size_t kNumBuckets = 4;
    static constexpr size_t kSketchDepth = 4;       // CMS rows
    static constexpr uint8_t kCounterMax = 15;      // 4-bit saturating counter
    static constexpr size_t kDoorkeeperHashes = 2;  // bloom hash count

    struct Config {
        size_t shard_count = detail::DefaultStatsShardCount();
        size_t max_snapshot_keys = detail::DefaultSnapshotLimit();
        // Number of CMS counters per row. Rounded up to a power of two.
        size_t counter_width = 1u << 16;
        // Accesses recorded before the sketch ages (halves). 0 → derived as
        // 10 * counter_width.
        size_t sample_window = 0;
        // Doorkeeper bloom-filter size in bits. Rounded up to a power of two.
        // 0 → derived as 8 * counter_width.
        size_t doorkeeper_bits = 0;
        // Minimum estimated frequency for COLD/WARM/HOT/VERY_HOT respectively.
        std::array<uint64_t, kNumBuckets> thresholds = {1, 2, 4, 8};
    };

    MultiLRUStatsCollector() : MultiLRUStatsCollector(Config{}) {}

    explicit MultiLRUStatsCollector(const Config& config)
        : shards_(detail::NormalizeShardCount(config.shard_count)),
          shard_mask_(shards_.size() - 1),
          max_snapshot_keys_(
              detail::NormalizeSnapshotLimit(config.max_snapshot_keys)),
          counter_width_(NormalizePow2(config.counter_width)),
          counter_mask_(counter_width_ - 1),
          doorkeeper_words_(DoorkeeperWordCount(config.doorkeeper_bits,
                                                config.counter_width)),
          doorkeeper_bit_mask_(doorkeeper_words_ * 64 - 1),
          sample_window_(config.sample_window == 0 ? counter_width_ * 10
                                                   : config.sample_window),
          thresholds_(config.thresholds),
          sketch_(kSketchDepth * counter_width_),
          doorkeeper_(doorkeeper_words_) {
        for (auto& counter : sketch_) {
            counter.store(0, std::memory_order_relaxed);
        }
        for (auto& word : doorkeeper_) {
            word.store(0, std::memory_order_relaxed);
        }
    }

    MultiLRUStatsCollector(size_t shard_count, size_t max_snapshot_keys)
        : MultiLRUStatsCollector(Config{shard_count, max_snapshot_keys}) {}

    // --- StatsCollector interface ---

    void RecordAccess(std::string_view key) override {
        BumpFrequency(key);
        auto& shard = GetShard(key);
        std::lock_guard<std::mutex> lock(shard.mutex);
        shard.pending_updates.emplace(std::string(key));
    }

    AccessStats GetSnapshot() override {
        std::lock_guard<std::mutex> lock(aggregate_mutex_);
        ApplyPendingLocked();
        return BuildSnapshotLocked();
    }

    void RemoveKey(std::string_view key) override {
        auto& shard = GetShard(key);
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto it = shard.pending_updates.find(key);
        if (it != shard.pending_updates.end()) {
            shard.pending_updates.erase(it);
        }
        shard.pending_deletes.emplace_back(key);
    }

    // --- Frequency query interface (consumed by MultiLRUPolicy) ---

    /// TinyLFU estimate of how many times `key` has been seen this window.
    uint64_t EstimateFrequency(std::string_view key) const {
        HashPair h = Hash(key);
        uint64_t estimate = kCounterMax;
        for (size_t row = 0; row < kSketchDepth; ++row) {
            const size_t idx = CounterIndex(row, h);
            estimate = std::min<uint64_t>(
                estimate,
                sketch_[row * counter_width_ + idx].load(
                    std::memory_order_relaxed));
        }
        return estimate + (DoorkeeperContains(h) ? 1 : 0);
    }

    /// Frequency bucket `key` maps to, derived purely from its estimate.
    MultiLruBucket BucketOf(std::string_view key) const {
        return BucketForFrequency(EstimateFrequency(key));
    }

    MultiLruBucket BucketForFrequency(uint64_t frequency) const {
        if (frequency >= thresholds_[3]) return MultiLruBucket::kVeryHot;
        if (frequency >= thresholds_[2]) return MultiLruBucket::kHot;
        if (frequency >= thresholds_[1]) return MultiLruBucket::kWarm;
        return MultiLruBucket::kCold;
    }

    /**
     * @brief Up to `max_keys` eviction candidates in coldest-first order
     *        (COLD bucket first, LRU-tail first within each bucket).
     */
    std::vector<std::string> EvictionCandidates(size_t max_keys) {
        std::vector<std::string> candidates;
        if (max_keys == 0) return candidates;

        std::lock_guard<std::mutex> lock(aggregate_mutex_);
        ApplyPendingLocked();
        candidates.reserve(std::min(max_keys, nodes_.size()));
        for (size_t bucket = 0; bucket < kNumBuckets; ++bucket) {
            for (auto it = lru_lists_[bucket].rbegin();
                 it != lru_lists_[bucket].rend(); ++it) {
                candidates.push_back(*it);
                if (candidates.size() >= max_keys) {
                    return candidates;
                }
            }
        }
        return candidates;
    }

   private:
    struct HashPair {
        uint64_t a;
        uint64_t b;
    };

    struct alignas(64) Shard {
        std::mutex mutex;
        std::unordered_set<std::string, StringHash, std::equal_to<>>
            pending_updates;
        std::vector<std::string> pending_deletes;
    };

    // One node per tracked key. The string lives inside the per-bucket list;
    // the map points at it so a key can be relocated across buckets with a
    // single std::list::splice (which keeps the iterator valid).
    struct NodeRef {
        size_t bucket;
        std::list<std::string>::iterator it;
    };

    static size_t NormalizePow2(size_t value) {
        size_t normalized = 1;
        while (normalized < std::max<size_t>(1, value)) {
            normalized <<= 1;
        }
        return normalized;
    }

    // Doorkeeper bloom size in 64-bit words: at least one word, default 8 bits
    // per CMS counter, rounded up to a power of two.
    static size_t DoorkeeperWordCount(size_t doorkeeper_bits,
                                      size_t counter_width) {
        size_t bits = doorkeeper_bits;
        if (bits == 0) bits = counter_width * 8;
        return NormalizePow2(std::max<size_t>(64, bits)) / 64;
    }

    Shard& GetShard(std::string_view key) {
        return shards_[std::hash<std::string_view>{}(key) & shard_mask_];
    }

    static HashPair Hash(std::string_view key) {
        const uint64_t a = std::hash<std::string_view>{}(key);
        // Independent secondary hash (FNV-1a) for double hashing.
        uint64_t b = 1469598103934665603ull;
        for (char c : key) {
            b ^= static_cast<uint8_t>(c);
            b *= 1099511628211ull;
        }
        // Ensure b is odd so (a + row * b) cycles through distinct buckets.
        return {a, b | 1ull};
    }

    size_t CounterIndex(size_t row, HashPair h) const {
        return (h.a + row * h.b) & counter_mask_;
    }

    size_t DoorkeeperBit(size_t k, HashPair h) const {
        return (h.a + (kSketchDepth + k) * h.b) & doorkeeper_bit_mask_;
    }

    bool DoorkeeperContains(HashPair h) const {
        for (size_t k = 0; k < kDoorkeeperHashes; ++k) {
            const size_t bit = DoorkeeperBit(k, h);
            const uint64_t word =
                doorkeeper_[bit >> 6].load(std::memory_order_relaxed);
            if ((word & (1ull << (bit & 63))) == 0) {
                return false;
            }
        }
        return true;
    }

    // Sets the doorkeeper bits for `h`; returns whether all were already set
    // (i.e. the key had been seen at least once this window).
    bool DoorkeeperTestAndSet(HashPair h) {
        bool already_present = true;
        for (size_t k = 0; k < kDoorkeeperHashes; ++k) {
            const size_t bit = DoorkeeperBit(k, h);
            const uint64_t bit_value = 1ull << (bit & 63);
            const uint64_t prev = doorkeeper_[bit >> 6].fetch_or(
                bit_value, std::memory_order_relaxed);
            if ((prev & bit_value) == 0) {
                already_present = false;
            }
        }
        return already_present;
    }

    void IncrementCounter(size_t row, size_t idx) {
        std::atomic<uint8_t>& counter = sketch_[row * counter_width_ + idx];
        uint8_t cur = counter.load(std::memory_order_relaxed);
        while (cur < kCounterMax) {
            if (counter.compare_exchange_weak(cur, cur + 1,
                                              std::memory_order_relaxed)) {
                break;
            }
        }
    }

    void BumpFrequency(std::string_view key) {
        const HashPair h = Hash(key);
        // Doorkeeper gate: a key's first sighting this window only sets the
        // bloom bits; the main sketch is incremented from the second sighting
        // on. This keeps one-hit-wonders out of the sketch.
        if (DoorkeeperTestAndSet(h)) {
            for (size_t row = 0; row < kSketchDepth; ++row) {
                IncrementCounter(row, CounterIndex(row, h));
            }
        }

        const uint64_t samples =
            samples_since_reset_.fetch_add(1, std::memory_order_relaxed) + 1;
        if (samples >= sample_window_) {
            MaybeAge();
        }
    }

    // Halves every sketch counter and clears the doorkeeper. Run by whichever
    // thread first crosses the window boundary; others skip via try_lock.
    void MaybeAge() {
        std::unique_lock<std::mutex> lock(aging_mutex_, std::try_to_lock);
        if (!lock.owns_lock()) return;
        if (samples_since_reset_.load(std::memory_order_relaxed) <
            sample_window_) {
            return;  // Another thread already aged.
        }
        for (auto& counter : sketch_) {
            const uint8_t cur = counter.load(std::memory_order_relaxed);
            counter.store(static_cast<uint8_t>(cur >> 1),
                          std::memory_order_relaxed);
        }
        for (auto& word : doorkeeper_) {
            word.store(0, std::memory_order_relaxed);
        }
        samples_since_reset_.store(0, std::memory_order_relaxed);
    }

    // --- Aggregate MultiLRU maintenance (under aggregate_mutex_) ---

    void ApplyPendingLocked() {
        for (auto& shard : shards_) {
            std::unordered_set<std::string, StringHash, std::equal_to<>>
                pending_updates;
            std::vector<std::string> pending_deletes;
            {
                std::lock_guard<std::mutex> lock(shard.mutex);
                pending_updates.swap(shard.pending_updates);
                pending_deletes.swap(shard.pending_deletes);
            }

            // Deletes first, then updates: an access that follows a delete in
            // the same window resurrects the key (it was erased from
            // pending_updates by RemoveKey, then re-inserted by RecordAccess).
            for (const auto& key : pending_deletes) {
                RemoveNodeLocked(key);
            }
            for (const auto& key : pending_updates) {
                TouchNodeLocked(key);
            }
        }
    }

    void TouchNodeLocked(const std::string& key) {
        const size_t bucket = static_cast<size_t>(BucketOf(key));
        auto map_it = nodes_.find(key);
        if (map_it == nodes_.end()) {
            lru_lists_[bucket].push_front(key);
            nodes_.emplace(key, NodeRef{bucket, lru_lists_[bucket].begin()});
            return;
        }

        NodeRef& ref = map_it->second;
        if (ref.bucket == bucket) {
            lru_lists_[bucket].splice(lru_lists_[bucket].begin(),
                                      lru_lists_[bucket], ref.it);
        } else {
            lru_lists_[bucket].splice(lru_lists_[bucket].begin(),
                                      lru_lists_[ref.bucket], ref.it);
            ref.bucket = bucket;
        }
        // After splice ref.it remains valid and now sits at the bucket head.
    }

    void RemoveNodeLocked(const std::string& key) {
        auto map_it = nodes_.find(key);
        if (map_it == nodes_.end()) {
            return;
        }
        lru_lists_[map_it->second.bucket].erase(map_it->second.it);
        nodes_.erase(map_it);
    }

    AccessStats BuildSnapshotLocked() {
        AccessStats stats;
        stats.metric = AccessStatMetric::kFrequency;
        stats.hot_keys.reserve(std::min(max_snapshot_keys_, nodes_.size()));

        // Hottest-first: VERY_HOT bucket down to COLD, MRU-first within a
        // bucket. recent_heat_score carries the frequency estimate so
        // GetHotKeyStats (HA recovery prioritisation) sees the hottest keys.
        size_t rank = 1;
        for (size_t bucket = kNumBuckets; bucket-- > 0;) {
            for (const auto& key : lru_lists_[bucket]) {
                stats.hot_keys.push_back(AccessStatEntry{
                    key, static_cast<double>(EstimateFrequency(key)), rank++});
                if (stats.hot_keys.size() >= max_snapshot_keys_) {
                    return stats;
                }
            }
        }
        return stats;
    }

    // Ingest (sharded, hot path).
    std::vector<Shard> shards_;
    const size_t shard_mask_;
    const size_t max_snapshot_keys_;

    // TinyLFU sketch (lock-free).
    const size_t counter_width_;
    const size_t counter_mask_;
    const size_t doorkeeper_words_;
    const size_t doorkeeper_bit_mask_;
    const size_t sample_window_;
    const std::array<uint64_t, kNumBuckets> thresholds_;
    std::vector<std::atomic<uint8_t>> sketch_;
    std::vector<std::atomic<uint64_t>> doorkeeper_;
    std::atomic<uint64_t> samples_since_reset_{0};
    std::mutex aging_mutex_;

    // Aggregate MultiLRU (guarded by aggregate_mutex_, reconciled lazily by the
    // worker thread via GetSnapshot / EvictionCandidates).
    std::mutex aggregate_mutex_;
    std::array<std::list<std::string>, kNumBuckets> lru_lists_;
    std::unordered_map<std::string, NodeRef, StringHash, std::equal_to<>>
        nodes_;
};

}  // namespace mooncake
