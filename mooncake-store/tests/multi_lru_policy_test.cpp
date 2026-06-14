#include <gtest/gtest.h>

#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

#include "tiered_cache/scheduler/multi_lru_policy.h"
#include "tiered_cache/scheduler/tinylfu_stats_collector.h"

namespace mooncake {
namespace {

constexpr UUID kFast{1, 1};
constexpr UUID kSlow{2, 2};

class MultiLRUPolicyTest : public ::testing::Test {
   protected:
    void SetUp() override {
        MultiLRUStatsCollector::Config ccfg;
        ccfg.shard_count = 2;
        ccfg.counter_width = 512;
        ccfg.sample_window = 1'000'000;
        ccfg.thresholds = {1, 2, 4, 8};
        collector_ = std::make_unique<MultiLRUStatsCollector>(ccfg);

        MultiLRUPolicy::Config pcfg;
        pcfg.offload_freq_threshold = 1;
        pcfg.onboard_freq_threshold = 3;
        pcfg.onboard_dram_watermark = 0.80;
        policy_ = std::make_unique<MultiLRUPolicy>(pcfg);
        policy_->SetFastTier(kFast);
        policy_->SetSlowTier(kSlow);
        policy_->SetCollector(collector_.get());
        policy_->SetKeyContextProvider(
            [this](std::string_view key) -> std::optional<KeyContext> {
                auto it = residency_.find(std::string(key));
                if (it == residency_.end()) return std::nullopt;
                return it->second;
            });

        stats_[kFast] = TierStats{1000, 100};
        stats_[kSlow] = TierStats{1000, 100};
    }

    void Heat(const std::string& key, int times) {
        for (int i = 0; i < times; ++i) collector_->RecordAccess(key);
    }

    std::unique_ptr<MultiLRUStatsCollector> collector_;
    std::unique_ptr<MultiLRUPolicy> policy_;
    std::unordered_map<std::string, KeyContext> residency_;
    std::unordered_map<UUID, TierStats> stats_;
};

KeyContext MakeCtx(std::string key, std::vector<UUID> locations,
                   size_t size = 10) {
    KeyContext ctx;
    ctx.key = std::move(key);
    ctx.current_locations = std::move(locations);
    ctx.size_bytes = size;
    return ctx;
}

TEST_F(MultiLRUPolicyTest, OffloadsHotDramOnlyKey) {
    Heat("hot", 5);
    const auto decision = policy_->Offload(MakeCtx("hot", {kFast}), stats_);
    EXPECT_TRUE(decision.perform);
    EXPECT_EQ(decision.target_tier, kSlow);
}

TEST_F(MultiLRUPolicyTest, DoesNotOffloadColdKey) {
    Heat("cold", 1);  // doorkeeper only → estimate 1, not > threshold 1
    EXPECT_FALSE(policy_->Offload(MakeCtx("cold", {kFast}), stats_).perform);
}

TEST_F(MultiLRUPolicyTest, DoesNotOffloadKeyAlreadyOnSlowTier) {
    Heat("hot", 5);
    EXPECT_FALSE(
        policy_->Offload(MakeCtx("hot", {kFast, kSlow}), stats_).perform);
}

TEST_F(MultiLRUPolicyTest, DoesNotOffloadWhenSlowTierFull) {
    Heat("hot", 5);
    stats_[kSlow] = TierStats{1000, 1000};
    EXPECT_FALSE(policy_->Offload(MakeCtx("hot", {kFast}), stats_).perform);
}

TEST_F(MultiLRUPolicyTest, OnboardsHotSlowKey) {
    Heat("hot", 6);
    const auto decision = policy_->Onboard(MakeCtx("hot", {kSlow}), stats_);
    EXPECT_TRUE(decision.perform);
    EXPECT_EQ(decision.target_tier, kFast);
}

TEST_F(MultiLRUPolicyTest, DoesNotOnboardBelowFrequencyThreshold) {
    Heat("warm", 2);  // estimate ~2, not > threshold 3
    EXPECT_FALSE(policy_->Onboard(MakeCtx("warm", {kSlow}), stats_).perform);
}

TEST_F(MultiLRUPolicyTest, DoesNotOnboardWhenDramAboveWatermark) {
    Heat("hot", 6);
    stats_[kFast] = TierStats{1000, 900};  // 90% > 80% watermark
    EXPECT_FALSE(policy_->Onboard(MakeCtx("hot", {kSlow}), stats_).perform);
}

TEST_F(MultiLRUPolicyTest, DoesNotOnboardKeyAlreadyInDram) {
    Heat("hot", 6);
    EXPECT_FALSE(
        policy_->Onboard(MakeCtx("hot", {kSlow, kFast}), stats_).perform);
}

TEST_F(MultiLRUPolicyTest, EvictColdestFirstWithCorrectAction) {
    Heat("cold_dram_only", 1);
    Heat("cold_replicated", 1);
    Heat("hot", 20);
    residency_["cold_dram_only"] = MakeCtx("cold_dram_only", {kFast});
    residency_["cold_replicated"] = MakeCtx("cold_replicated", {kFast, kSlow});
    residency_["hot"] = MakeCtx("hot", {kFast});

    const auto actions = policy_->Evict(stats_, /*target_reclaim_bytes=*/25);
    ASSERT_FALSE(actions.empty());
    EXPECT_EQ(actions.back().key, "hot") << "very-hot key evicted last";

    for (const auto& action : actions) {
        if (action.key == "cold_dram_only") {
            EXPECT_EQ(action.type, SchedAction::Type::MIGRATE);
            EXPECT_EQ(action.target_tier_id, kSlow);
            EXPECT_EQ(action.source_tier_id, kFast);
        }
        if (action.key == "cold_replicated") {
            EXPECT_EQ(action.type, SchedAction::Type::EVICT);
            EXPECT_EQ(action.source_tier_id, kFast);
        }
    }
}

TEST_F(MultiLRUPolicyTest, EvictSkipsSlowOnlyKeys) {
    Heat("ssd_only", 3);
    residency_["ssd_only"] = MakeCtx("ssd_only", {kSlow});

    const auto actions = policy_->Evict(stats_, /*target_reclaim_bytes=*/1000);
    for (const auto& action : actions) {
        EXPECT_NE(action.key, "ssd_only")
            << "DRAM eviction must not touch slow-only keys";
    }
}

}  // namespace
}  // namespace mooncake
