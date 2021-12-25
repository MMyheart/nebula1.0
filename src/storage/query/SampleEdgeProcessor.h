/* Copyright (c) 2021 vesoft inc. All rights reserved.:
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_SAMPLEEDGEPROCESSOR_H_
#define STORAGE_SAMPLEEDGEPROCESSOR_H_

#include "base/Base.h"
#include "common/sample/WeightedCollection.h"
#include "storage/BaseProcessor.h"
#include "storage/admin/RebuildSampleProcessor.h"

namespace nebula {
namespace storage {

class SampleEdgeProcessor : public BaseProcessor<cpp2::SampleEdgeResp> {
public:
    static SampleEdgeProcessor* instance(kvstore::StatisticStore* statisticStore,
                                         meta::SchemaManager* schemaMan,
                                         stats::Stats* stats) {
        return new SampleEdgeProcessor(statisticStore, schemaMan, stats);
    }

    void process(const cpp2::SampleEdgeRequest& req);

private:
    explicit SampleEdgeProcessor(kvstore::StatisticStore* statisticStore,
                                 meta::SchemaManager* schemaMan,
                                 stats::Stats* stats)
        : BaseProcessor<cpp2::SampleEdgeResp>(nullptr, schemaMan, stats),
          statisticStore_(statisticStore) {}

    kvstore::StatisticStore* statisticStore_{nullptr};

    GraphSpaceID spaceId_;
    std::unordered_map<EdgeType, std::string> weights_;
};

}   // namespace storage
}   // namespace nebula
#endif   // STORAGE_SAMPLEEDGEPROCESSOR_H_
