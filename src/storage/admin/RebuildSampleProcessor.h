/* Copyright (c) 2021 vesoft inc. All rights reserved.:
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_REBUILDSAMPLEPROCESSOR_H_
#define STORAGE_REBUILDSAMPLEPROCESSOR_H_

#include "base/Base.h"
#include "kvstore/StatisticStore.h"
#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {

struct VertexStatistic {
    int64_t sVid_{0};
    double prob_{0.0};
    VertexID vid_{0};
    double weight_{0.0};
    VertexID aliasVid_{0};
    double aliasWeight_{0.0};
};
struct EdgeStatistic {
    int64_t sVid_{0};
    double prob_{0.0};
    VertexID srcVid_{0};
    VertexID dstVid_{0};
    double weight_{0.0};
    VertexID aliasSrcVid_{0};
    VertexID aliasDstVid_{0};
    double aliasWeight_{0.0};
    EdgeType edgeType_{0};
};

class StatisticStoreUtils final {
public:
    static std::string getStatisticLargeKey(GraphSpaceID spaceID, int32_t tagOrEdge, int64_t sVid);

    static std::string getStatisticSmallKey(GraphSpaceID spaceID, int32_t tagOrEdge, int64_t sVid);

    static std::string getStatisticKey(GraphSpaceID spaceID,
                                       int32_t tagOrEdge,
                                       int64_t sVid,
                                       const std::string& flag = "A");

    static std::string getStatisticValue(VertexStatistic statistic);
    static std::string getStatisticValue(EdgeStatistic statistic);

    static void parseStatisticValue(VertexStatistic* statistic, std::string& value);
    static void parseStatisticValue(EdgeStatistic* statistic, std::string& value);

    static std::string statisticLargePrefix(GraphSpaceID spaceID, int32_t tagOrEdge);

    static std::string statisticSmallPrefix(GraphSpaceID spaceID, int32_t tagOrEdge);

    static std::string statisticPrefix(GraphSpaceID spaceID,
                                       int32_t tagOrEdge,
                                       const std::string& flag = "A");

    static std::string getTagOrEdgeWeightsKey(GraphSpaceID spaceID, int32_t tagOrEdge);

    static std::string tagOrEdgeWeightsPrefix(GraphSpaceID spaceID);

    static std::string getTagOrEdgeCountsKey(GraphSpaceID spaceID, int32_t tagOrEdge);

    static std::string tagOrEdgeCountsPrefix(GraphSpaceID spaceID);

    static std::string doubleToString(const double value, unsigned int precisionAfterPoint = 20);

    static void splitString(const std::string& s,
                            std::vector<std::string>& v,
                            const std::string& c);
};

class RebuildSampleProcessor : public BaseProcessor<cpp2::RebuildSampleResponse> {
public:
    static RebuildSampleProcessor* instance(kvstore::KVStore* kvstore,
                                            kvstore::StatisticStore* statisticStore,
                                            meta::SchemaManager* schemaMan) {
        return new RebuildSampleProcessor(kvstore, statisticStore, schemaMan);
    }

    void process(const cpp2::RebuildSampleRequest& req);

private:
    explicit RebuildSampleProcessor(kvstore::KVStore* kvstore,
                                    kvstore::StatisticStore* statisticStore,
                                    meta::SchemaManager* schemaMan)
        : BaseProcessor<cpp2::RebuildSampleResponse>(kvstore, schemaMan, nullptr),
          statisticStore_(statisticStore) {}

    void parseTag(folly::StringPiece key, folly::StringPiece val);

    void saveTagStatistic(TagID tagId);

    void splitLargeAndSmallTag(TagID tagId);

    void aliasSampleTag(TagID tagId);

    void parseEdge(folly::StringPiece key, folly::StringPiece val);

    void saveEdgeStatistic(EdgeType edgeType);

    void splitLargeAndSmallEdge(EdgeType edgeType);

    void aliasSampleEdge(EdgeType edgeType);

    void saveTagAndEdgeWeights();

    void deleteSplitInfo(int32_t tagOrEdge, bool isTag);

    void processResult();

private:
    kvstore::StatisticStore* statisticStore_{nullptr};
    GraphSpaceID spaceId_;

    uint64_t batchSize_ = 100000;
    std::string weightField_{"w"};
    // TagID -> RocksDB

    // TagID -> current index
    std::unordered_map<TagID, int64_t> tagIndex_;
    std::unordered_map<TagID, int64_t> oldTagIndex_;
    std::unordered_map<TagID, double> tagStandard_;
    // TagID -> vid + weight
    std::unordered_map<TagID, std::vector<VertexStatistic>> tagStatistic_;
    std::unordered_map<TagID, double> tagWeightSummary_;

    // EdgeType -> current index
    std::unordered_map<EdgeType, int64_t> edgeIndex_;
    std::unordered_map<EdgeType, int64_t> oldEdgeIndex_;
    std::unordered_map<EdgeType, double> edgeStandard_;
    // EdgeType -> vid + weight
    std::unordered_map<EdgeType, std::vector<EdgeStatistic>> edgeStatistic_;
    std::unordered_map<EdgeType, double> edgeWeightSummary_;
};

}   // namespace storage
}   // namespace nebula

#endif   // STORAGE_REBUILDSAMPLEPROCESSOR_H_
