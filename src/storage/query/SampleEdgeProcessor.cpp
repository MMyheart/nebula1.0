/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "storage/query/SampleEdgeProcessor.h"
#include "common/rand/random.h"

namespace nebula {
namespace storage {

void SampleEdgeProcessor::process(const cpp2::SampleEdgeRequest& req) {
    auto spaceId = req.get_space_id();
    auto& edgeTypes = req.get_edge_types();
    auto count = req.get_count();
    if (edgeTypes.empty()) {
        LOG(ERROR) << "Sample Edge edgeTypes is empty";
        onFinished();
        return;
    }

    std::function<EdgeType()> getEdgeType = [edgeTypes]() -> EdgeType { return edgeTypes[0]; };
    if (edgeTypes.size() > 1) {
        std::unordered_map<EdgeType, double> weights;
        if (!statisticStore_->getTagOrEdgeWeights(spaceId, edgeTypes, &weights)) {
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
                std::string edgeType = key.substr(offset, key.size() - offset);
                weights[atoi(edgeType.data())] = strtod(value.data(), nullptr);
            }
            statisticStore_->addTagOrEdgeWeights(spaceId, weights);
        }
        std::vector<std::pair<EdgeType, double>> weightsPair;
        weightsPair.reserve(weights.size());
        for (auto weight : weights) {
            weightsPair.emplace_back(std::make_pair(weight.first, weight.second));
        }
        CompactWeightedCollection<EdgeType> edgeWeightCollection;
        edgeWeightCollection.init(weightsPair);
        getEdgeType = [edgeWeightCollection]() -> EdgeType {
            return edgeWeightCollection.sample().first;
        };
    }
    std::unordered_map<EdgeType, int64_t> counts;
    if (!statisticStore_->getTagOrEdgeCount(spaceId, edgeTypes, &counts)) {
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
            std::string edgeType = key.substr(offset, key.size() - offset);
            counts[atoi(edgeType.data())] = strtoll(value.data(), nullptr, 10);
        }
        statisticStore_->addTagOrEdgeCount(spaceId, counts);
    }

    std::unordered_map<int64_t, double> random;
    std::unordered_map<int64_t, EdgeType> sVidEdgeMap;
    std::vector<std::string> keys;
    keys.reserve(count);
    for (int i = 0; i < count; ++i) {
        EdgeType edgeType = getEdgeType();
        int64_t sVid = nebula::NextLong(counts[edgeType]);
        double prob = nebula::ThreadLocalRandom();
        random[sVid] = prob;
        sVidEdgeMap[sVid] = edgeType;
        std::string key = StatisticStoreUtils::getStatisticKey(spaceId, edgeType, sVid);
        keys.emplace_back(key);
        VLOG(1) << "space " << spaceId << "edgeType " << edgeType << ", random sVid " << sVid
                << ", random prob " << prob;
    }

    std::vector<std::string> values;
    statisticStore_->multiGet(spaceId, keys, &values);

    std::vector<cpp2::EdgeWeight> edgeWeights;
    edgeWeights.reserve(count);
    for (auto value : values) {
        EdgeStatistic statistic;
        StatisticStoreUtils::parseStatisticValue(&statistic, value);
        cpp2::EdgeWeight edgeWeight;
        edgeWeight.set_edgeType(sVidEdgeMap[statistic.sVid_]);
        if (random[statistic.sVid_] < statistic.prob_) {
            edgeWeight.set_srcVid(statistic.srcVid_);
            edgeWeight.set_dstVid(statistic.dstVid_);
            edgeWeight.set_weight(statistic.weight_);
        } else {
            edgeWeight.set_srcVid(statistic.aliasSrcVid_);
            edgeWeight.set_dstVid(statistic.aliasDstVid_);
            edgeWeight.set_weight(statistic.aliasWeight_);
        }
        edgeWeights.emplace_back(edgeWeight);
    }
    resp_.set_edgeWeights(std::move(edgeWeights));
    onFinished();
}

}   // namespace storage
}   // namespace nebula
