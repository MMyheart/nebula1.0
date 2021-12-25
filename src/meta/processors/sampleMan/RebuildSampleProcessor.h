/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_REBUILDSAMPLEPROCESSOR_H
#define META_REBUILDSAMPLEPROCESSOR_H

#include "meta/processors/BaseProcessor.h"
#include "meta/processors/admin/AdminClient.h"

namespace nebula {
namespace meta {

class RebuildSampleProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static RebuildSampleProcessor* instance(kvstore::KVStore* kvstore, AdminClient* adminClient) {
        return new RebuildSampleProcessor(kvstore, adminClient);
    }

    void process(const cpp2::RebuildSampleReq& req);

private:
    explicit RebuildSampleProcessor(kvstore::KVStore* kvstore, AdminClient* adminClient)
        : BaseProcessor<cpp2::ExecResp>(kvstore), adminClient_(adminClient) {}

    bool checkIfNeedToRebuild(GraphSpaceID space,
                              std::vector<TagID> tagIds,
                              std::vector<EdgeType> edgeTypes);

    bool saveRebuildStatus(kvstore::KVStore* kvStore,
                           cpp2::ExecResp& resp,
                           GraphSpaceID space,
                           std::vector<TagID> tagIds,
                           std::vector<EdgeType> edgeTypes,
                           std::string status);

    bool saveRebuildWeights(kvstore::KVStore* kvStore,
                            GraphSpaceID space,
                            TagIdHostWeights tagIdWeights,
                            EdgeTypeHostWeights edgeTypeWeights);

    bool getRebuildWeights(kvstore::KVStore* kvStore,
                           GraphSpaceID space,
                           TagIdHostWeights& tagIdWeights,
                           EdgeTypeHostWeights& edgeTypeWeights);

    void handleRebuildSampleResult(
        std::vector<folly::Future<storage::cpp2::RebuildSampleResponse>> results,
        kvstore::KVStore* kvstore,
        cpp2::ExecResp& resp,
        GraphSpaceID space,
        std::vector<TagID> tagIds,
        std::vector<EdgeType> edgeTypes);

private:
    AdminClient* adminClient_;
};

}   // namespace meta
}   // namespace nebula

#endif   // META_REBUILDSAMPLEPROCESSOR_H
