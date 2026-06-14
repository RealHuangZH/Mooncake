#include <gtest/gtest.h>

#include <string>
#include <thread>
#include <vector>

#include "tiered_cache/scheduler/tinylfu_stats_collector.h"

namespace mooncake {
namespace {

const AccessStatEntry* FindKeyStats(const AccessStats& stats,
                                    const std::string& key) {
    for (const auto& item : stats.hot_keys) {
        if (item.key == key) {
            return &item;
        }
    }
    return nullptr;
}

MultiLRUStatsCollector::Config SmallConfig(size_t sample_window = 1'000'000) {
    MultiLRUStatsCollector::Config cfg;
    cfg.shard_count = 4;
    cfg.counter_width = 1024;
    cfg.sample_window = sample_window;
    cfg.thresholds = {1, 2, 4, 8};
    return cfg;
}

TEST(MultiLRUStatsCollectorTest, FrequencyIsMonotonicWithDoorkeeper) {
    MultiLRUStatsCollector c(SmallConfig());

    // Unseen key estimates zero.
    EXPECT_EQ(c.EstimateFrequency("k"), 0u);

    // The doorkeeper absorbs the first sighting: estimate becomes 1 without
    // touching the main sketch.
    c.RecordAccess("k");
    EXPECT_EQ(c.EstimateFrequency("k"), 1u);

    uint64_t prev = c.EstimateFrequency("k");
    for (int i = 0; i < 20; ++i) {
        c.RecordAccess("k");
        const uint64_t now = c.EstimateFrequency("k");
        EXPECT_GE(now, prev) << "frequency must be non-decreasing";
        prev = now;
    }
    EXPECT_GE(c.EstimateFrequency("k"), 8u) << "hot key saturates upward";
}

TEST(MultiLRUStatsCollectorTest, BucketBoundaries) {
    MultiLRUStatsCollector c(SmallConfig());
    EXPECT_EQ(c.BucketForFrequency(0), MultiLruBucket::kCold);
    EXPECT_EQ(c.BucketForFrequency(1), MultiLruBucket::kCold);
    EXPECT_EQ(c.BucketForFrequency(2), MultiLruBucket::kWarm);
    EXPECT_EQ(c.BucketForFrequency(3), MultiLruBucket::kWarm);
    EXPECT_EQ(c.BucketForFrequency(4), MultiLruBucket::kHot);
    EXPECT_EQ(c.BucketForFrequency(7), MultiLruBucket::kHot);
    EXPECT_EQ(c.BucketForFrequency(8), MultiLruBucket::kVeryHot);
    EXPECT_EQ(c.BucketForFrequency(100), MultiLruBucket::kVeryHot);

    for (int i = 0; i < 20; ++i) c.RecordAccess("hot");
    EXPECT_EQ(c.BucketOf("hot"), MultiLruBucket::kVeryHot);
}

TEST(MultiLRUStatsCollectorTest, AgingHalvesFrequency) {
    MultiLRUStatsCollector c(SmallConfig(/*sample_window=*/50));
    for (int i = 0; i < 10; ++i) c.RecordAccess("h");
    const uint64_t before = c.EstimateFrequency("h");
    EXPECT_GE(before, 8u);

    // Drive unrelated accesses past the sample window to trigger aging.
    for (int i = 0; i < 60; ++i) {
        c.RecordAccess("filler_" + std::to_string(i));
    }
    const uint64_t after = c.EstimateFrequency("h");
    EXPECT_LT(after, before) << "aging must reduce a stale key's frequency";
}

TEST(MultiLRUStatsCollectorTest, EvictionCandidatesOrderedColdFirst) {
    MultiLRUStatsCollector c(SmallConfig());
    c.RecordAccess("cold1");
    c.RecordAccess("cold2");
    for (int i = 0; i < 20; ++i) c.RecordAccess("hot");

    const auto candidates = c.EvictionCandidates(10);
    ASSERT_FALSE(candidates.empty());
    EXPECT_EQ(candidates.back(), "hot") << "very-hot key is evicted last";

    size_t hot_pos = candidates.size();
    for (size_t i = 0; i < candidates.size(); ++i) {
        if (candidates[i] == "hot") hot_pos = i;
    }
    for (size_t i = 0; i < candidates.size(); ++i) {
        if (candidates[i] == "cold1" || candidates[i] == "cold2") {
            EXPECT_LT(i, hot_pos) << "cold keys precede the hot key";
        }
    }
}

TEST(MultiLRUStatsCollectorTest, SnapshotIsFrequencyOrdered) {
    MultiLRUStatsCollector c(SmallConfig());
    c.RecordAccess("cold");
    for (int i = 0; i < 20; ++i) c.RecordAccess("hot");

    const auto snapshot = c.GetSnapshot();
    EXPECT_EQ(snapshot.metric, AccessStatMetric::kFrequency);
    ASSERT_GE(snapshot.hot_keys.size(), 2u);
    EXPECT_EQ(snapshot.hot_keys.front().key, "hot")
        << "hottest key reported first for HA recovery prioritisation";
    EXPECT_GT(snapshot.hot_keys.front().recent_heat_score, 0.0);
}

TEST(MultiLRUStatsCollectorTest, RemoveKeyDropsTracking) {
    MultiLRUStatsCollector c(SmallConfig());
    for (int i = 0; i < 5; ++i) c.RecordAccess("victim");
    ASSERT_NE(FindKeyStats(c.GetSnapshot(), "victim"), nullptr);

    c.RemoveKey("victim");
    EXPECT_EQ(FindKeyStats(c.GetSnapshot(), "victim"), nullptr);
}

TEST(MultiLRUStatsCollectorTest, ConcurrentRecordAccessIsSafe) {
    constexpr int kThreads = 8;
    constexpr int kAccessesPerThread = 5000;
    MultiLRUStatsCollector c;

    std::vector<std::thread> workers;
    workers.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        workers.emplace_back([&c]() {
            for (int i = 0; i < kAccessesPerThread; ++i) {
                c.RecordAccess("shared");
                c.RecordAccess("u" + std::to_string(i % 97));
            }
        });
    }
    for (auto& worker : workers) worker.join();

    EXPECT_GE(c.EstimateFrequency("shared"), 8u)
        << "heavily-shared key resolves as very hot";
    EXPECT_FALSE(c.EvictionCandidates(50).empty());
}

}  // namespace
}  // namespace mooncake
