/* Copyright (c) 2021 vesoft inc. All rights reserved.:
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_SAMPLEVERTEXPROCESSOR_H_
#define STORAGE_SAMPLEVERTEXPROCESSOR_H_

#include "base/Base.h"
#include "common/sample/WeightedCollection.h"
#include "storage/BaseProcessor.h"
#include "storage/admin/RebuildSampleProcessor.h"

namespace nebula {
namespace storage {

class SampleVertexProcessor : public BaseProcessor<cpp2::SampleVertexResp> {
public:
    static SampleVertexProcessor* instance(kvstore::StatisticStore* statisticStore,
                                           meta::SchemaManager* schemaMan,
                                           stats::Stats* stats) {
        return new SampleVertexProcessor(statisticStore, schemaMan, stats);
    }

    void process(const cpp2::SampleVertexRequest& req);

private:
    explicit SampleVertexProcessor(kvstore::StatisticStore* statisticStore,
                                   meta::SchemaManager* schemaMan,
                                   stats::Stats* stats)
        : BaseProcessor<cpp2::SampleVertexResp>(nullptr, schemaMan, stats),
          statisticStore_(statisticStore) {}

    kvstore::StatisticStore* statisticStore_{nullptr};
};

}   // namespace storage
}   // namespace nebula
#endif   // STORAGE_SAMPLEVERTEXPROCESSOR_H_
