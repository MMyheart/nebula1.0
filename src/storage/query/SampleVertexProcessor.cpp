/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "storage/query/SampleVertexProcessor.h"
#include "common/rand/random.h"
#include "storage/admin/RebuildSampleProcessor.h"

namespace nebula {
namespace storage {

void SampleVertexProcessor::process(const cpp2::SampleVertexRequest& req) {
    auto spaceId = req.get_space_id();
    auto& tagIds = req.get_tag_ids();
    auto count = req.get_count();
    if (tagIds.empty()) {
        LOG(ERROR) << "Sample Vertex tagIds is empty";
        onFinished();
        return;
    }

    std::function<TagID()> getTagID = [tagIds]() -> TagID { return tagIds[0]; };
    if (tagIds.size() > 1) {
        std::unordered_map<TagID, double> weights;
        if (!statisticStore_->getTagOrEdgeWeights(spaceId, tagIds, &weights)) {
            std::string prefix = StatisticStoreUtils::tagOrEdgeWeightsPrefix(spaceId);
            std::unique_ptr<kvstore::KVIterator> iter;
            auto ret = statisticStore_->prefix(spaceId, prefix, &iter);
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                cpp2::ResultCode thriftRet;
                thriftRet.set_code(cpp2::ErrorCode::E_UNKNOWN);
                codes_.emplace_back(thriftRet);
                LOG(ERROR) << "SampleVertexProcessor tagOrEdgeWeightsPrefix=" << prefix
                           << ", iterator error, code=" << ret;
                onFinished();
                return;
            }
            for (; iter->valid(); iter->next()) {
                std::string key = iter->key().str();
                std::string value = iter->val().str();
                auto offset = prefix.size();
                std::string tagId = key.substr(offset, key.size() - offset);
                weights[atoi(tagId.data())] = strtod(value.data(), nullptr);
            }
            statisticStore_->addTagOrEdgeWeights(spaceId, weights);
        }
        std::vector<std::pair<TagID, double>> weightsPair;
        weightsPair.reserve(weights.size());
        for (auto weight : weights) {
            weightsPair.emplace_back(std::make_pair(weight.first, weight.second));
        }
        CompactWeightedCollection<TagID> tagWeightCollection;
        tagWeightCollection.init(weightsPair);
        getTagID = [tagWeightCollection]() -> TagID { return tagWeightCollection.sample().first; };
    }
    std::unordered_map<TagID, int64_t> counts;
    if (!statisticStore_->getTagOrEdgeCount(spaceId, tagIds, &counts)) {
        std::string prefix = StatisticStoreUtils::tagOrEdgeCountsPrefix(spaceId);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = statisticStore_->prefix(spaceId, prefix, &iter);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            cpp2::ResultCode thriftRet;
            thriftRet.set_code(cpp2::ErrorCode::E_UNKNOWN);
            codes_.emplace_back(thriftRet);
            LOG(ERROR) << "SampleVertexProcessor tagOrEdgeWeightsPrefix=" << prefix
                       << ", iterator error, code=" << ret;
            onFinished();
            return;
        }
        for (; iter->valid(); iter->next()) {
            std::string key = iter->key().str();
            std::string value = iter->val().str();
            auto offset = prefix.size();
            std::string tagId = key.substr(offset, key.size() - offset);
            counts[atoi(tagId.data())] = strtoll(value.data(), nullptr, 10);
        }
        statisticStore_->addTagOrEdgeCount(spaceId, counts);
    }

    std::unordered_map<int64_t, double> random;
    std::unordered_map<int64_t, TagID> sVidTagMap;
    std::vector<std::string> keys;
    keys.reserve(count);
    for (int i = 0; i < count; ++i) {
        TagID tagId = getTagID();
        int64_t sVid = nebula::NextLong(counts[tagId]);
        double prob = nebula::ThreadLocalRandom();
        random[sVid] = prob;
        sVidTagMap[sVid] = tagId;
        std::string key = StatisticStoreUtils::getStatisticKey(spaceId, tagId, sVid);
        keys.emplace_back(key);
        VLOG(1) << "space " << spaceId << "tagId " << tagId << ", random sVid " << sVid
                << ", random prob " << prob;
    }

    std::vector<std::string> values;
    statisticStore_->multiGet(spaceId, keys, &values);

    std::vector<cpp2::VertexWeight> vertexWeights;
    vertexWeights.reserve(count);
    for (auto value : values) {
        VertexStatistic statistic;
        StatisticStoreUtils::parseStatisticValue(&statistic, value);
        cpp2::VertexWeight vertexWeight;
        vertexWeight.set_tagId(sVidTagMap[statistic.sVid_]);
        if (random[statistic.sVid_] < statistic.prob_) {
            vertexWeight.set_vid(statistic.vid_);
            vertexWeight.set_weight(statistic.weight_);
        } else {
            vertexWeight.set_vid(statistic.aliasVid_);
            vertexWeight.set_weight(statistic.aliasWeight_);
        }
        vertexWeights.emplace_back(vertexWeight);
    }
    resp_.set_vertexWeights(std::move(vertexWeights));
    onFinished();
}

}   // namespace storage
}   // namespace nebula
