/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/StorageServiceHandler.h"
#include "base/Base.h"
#include "storage/query/QueryBoundProcessor.h"
#include "storage/query/QueryVertexPropsProcessor.h"
#include "storage/query/QueryEdgePropsProcessor.h"
#include "storage/query/QueryStatsProcessor.h"
#include "storage/query/GetUUIDProcessor.h"
#include "storage/query/ScanEdgeProcessor.h"
#include "storage/query/ScanVertexProcessor.h"
#include "storage/query/SampleVertexProcessor.h"
#include "storage/query/SampleEdgeProcessor.h"
#include "storage/mutate/AddVerticesProcessor.h"
#include "storage/mutate/AddEdgesProcessor.h"
#include "storage/mutate/DeleteVerticesProcessor.h"
#include "storage/mutate/DeleteEdgesProcessor.h"
#include "storage/mutate/UpdateVertexProcessor.h"
#include "storage/mutate/UpdateEdgeProcessor.h"
#include "storage/kv/PutProcessor.h"
#include "storage/kv/GetProcessor.h"
#include "storage/admin/AdminProcessor.h"
#include "storage/admin/CreateCheckpointProcessor.h"
#include "storage/admin/DropCheckpointProcessor.h"
#include "storage/admin/SendBlockSignProcessor.h"
#include "storage/admin/RebuildTagIndexProcessor.h"
#include "storage/admin/RebuildEdgeIndexProcessor.h"
#include "storage/index/LookUpIndexProcessor.h"
#include "storage/admin/RebuildSampleProcessor.h"

#define RETURN_FUTURE(processor) \
    auto f = processor->getFuture(); \
    processor->process(req); \
    return f;

DEFINE_int32(vertex_cache_num, 16 * 1000 * 1000, "Total keys inside the cache");
DEFINE_int32(vertex_cache_bucket_exp, 4, "Total buckets number is 1 << cache_bucket_exp");
DEFINE_int32(reader_handlers, 32, "Total reader handlers");
DEFINE_string(reader_handlers_type, "cpu", "Type of reader handlers, options: cpu,io");
DEFINE_bool(lookup_concurrently, false,
            "whether to traversal partitions concurrently in lookup processor");

namespace nebula {
namespace storage {

folly::Future<cpp2::QueryResponse>
StorageServiceHandler::future_getBound(const cpp2::GetNeighborsRequest& req) {
    auto* processor = QueryBoundProcessor::instance(kvstore_,
                                                    schemaMan_,
                                                    &getBoundQpsStat_,
                                                    readerPool_.get(),
                                                    &vertexCache_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::QueryStatsResponse>
StorageServiceHandler::future_boundStats(const cpp2::GetNeighborsRequest& req) {
    auto* processor = QueryStatsProcessor::instance(kvstore_,
                                                    schemaMan_,
                                                    &boundStatsQpsStat_,
                                                    readerPool_.get(),
                                                    &vertexCache_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::QueryResponse>
StorageServiceHandler::future_getProps(const cpp2::VertexPropRequest& req) {
    auto* processor = QueryVertexPropsProcessor::instance(kvstore_,
                                                          schemaMan_,
                                                          &vertexPropsQpsStat_,
                                                          readerPool_.get(),
                                                          &vertexCache_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::EdgePropResponse>
StorageServiceHandler::future_getEdgeProps(const cpp2::EdgePropRequest& req) {
    auto* processor = QueryEdgePropsProcessor::instance(kvstore_,
                                                        schemaMan_,
                                                        &edgePropsQpsStat_,
                                                        readerPool_.get());
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResponse>
StorageServiceHandler::future_addVertices(const cpp2::AddVerticesRequest& req) {
    auto* processor = AddVerticesProcessor::instance(kvstore_,
                                                     schemaMan_,
                                                     indexMan_,
                                                     &addVertexQpsStat_,
                                                     &vertexCache_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResponse>
StorageServiceHandler::future_addEdges(const cpp2::AddEdgesRequest& req) {
    auto* processor = AddEdgesProcessor::instance(kvstore_,
                                                  schemaMan_,
                                                  indexMan_,
                                                  &addEdgeQpsStat_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResponse>
StorageServiceHandler::future_deleteVertices(const cpp2::DeleteVerticesRequest& req) {
    auto* processor = DeleteVerticesProcessor::instance(kvstore_,
                                                        schemaMan_,
                                                        indexMan_,
                                                        &delVertexQpsStat_,
                                                        &vertexCache_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResponse>
StorageServiceHandler::future_deleteEdges(const cpp2::DeleteEdgesRequest& req) {
    auto* processor = DeleteEdgesProcessor::instance(kvstore_, schemaMan_, indexMan_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::UpdateResponse>
StorageServiceHandler::future_updateVertex(const cpp2::UpdateVertexRequest& req) {
    auto* processor = UpdateVertexProcessor::instance(kvstore_,
                                                      schemaMan_,
                                                      indexMan_,
                                                      &updateVertexQpsStat_,
                                                      &vertexCache_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::UpdateResponse>
StorageServiceHandler::future_updateEdge(const cpp2::UpdateEdgeRequest& req) {
    auto* processor = UpdateEdgeProcessor::instance(kvstore_,
                                                    schemaMan_,
                                                    indexMan_,
                                                    &updateEdgeQpsStat_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ScanEdgeResponse>
StorageServiceHandler::future_scanEdge(const cpp2::ScanEdgeRequest& req) {
    auto* processor = ScanEdgeProcessor::instance(kvstore_, schemaMan_, &scanEdgeQpsStat_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ScanVertexResponse>
StorageServiceHandler::future_scanVertex(const cpp2::ScanVertexRequest& req) {
    auto* processor = ScanVertexProcessor::instance(kvstore_, schemaMan_, &scanVertexQpsStat_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::AdminExecResp>
StorageServiceHandler::future_transLeader(const cpp2::TransLeaderReq& req) {
    auto* processor = TransLeaderProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::AdminExecResp>
StorageServiceHandler::future_addPart(const cpp2::AddPartReq& req) {
    auto* processor = AddPartProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::AdminExecResp>
StorageServiceHandler::future_addLearner(const cpp2::AddLearnerReq& req) {
    auto* processor = AddLearnerProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::AdminExecResp>
StorageServiceHandler::future_waitingForCatchUpData(const cpp2::CatchUpDataReq& req) {
    auto* processor = WaitingForCatchUpDataProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::AdminExecResp>
StorageServiceHandler::future_removePart(const cpp2::RemovePartReq& req) {
    auto* processor = RemovePartProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::AdminExecResp>
StorageServiceHandler::future_memberChange(const cpp2::MemberChangeReq& req) {
    auto* processor = MemberChangeProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::AdminExecResp>
StorageServiceHandler::future_checkPeers(const cpp2::CheckPeersReq& req) {
    auto* processor = CheckPeersProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::GetLeaderResp>
StorageServiceHandler::future_getLeaderPart(const cpp2::GetLeaderReq& req) {
    auto* processor = GetLeaderProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResponse>
StorageServiceHandler::future_put(const cpp2::PutRequest& req) {
    auto* processor = PutProcessor::instance(kvstore_, schemaMan_, &putKvQpsStat_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::GeneralResponse>
StorageServiceHandler::future_get(const cpp2::GetRequest& req) {
    auto* processor = GetProcessor::instance(kvstore_, schemaMan_, &getKvQpsStat_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::GetUUIDResp>
StorageServiceHandler::future_getUUID(const cpp2::GetUUIDReq& req) {
    auto* processor = GetUUIDProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::AdminExecResp>
StorageServiceHandler::future_createCheckpoint(const cpp2::CreateCPRequest& req) {
    auto* processor = CreateCheckpointProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::AdminExecResp>
StorageServiceHandler::future_dropCheckpoint(const cpp2::DropCPRequest& req) {
    auto* processor = DropCheckpointProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::AdminExecResp>
StorageServiceHandler::future_blockingWrites(const cpp2::BlockingSignRequest& req) {
    auto* processor = SendBlockSignProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::AdminExecResp>
StorageServiceHandler::future_rebuildTagIndex(const cpp2::RebuildIndexRequest& req) {
    auto* processor = RebuildTagIndexProcessor::instance(kvstore_,
                                                         schemaMan_,
                                                         indexMan_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::AdminExecResp>
StorageServiceHandler::future_rebuildEdgeIndex(const cpp2::RebuildIndexRequest& req) {
    auto* processor = RebuildEdgeIndexProcessor::instance(kvstore_,
                                                          schemaMan_,
                                                          indexMan_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::LookUpIndexResp>
StorageServiceHandler::future_lookUpIndex(const cpp2::LookUpIndexRequest& req) {
    folly::Executor* executor = nullptr;
    if (FLAGS_lookup_concurrently) {
        executor = readerPool_.get();
    }
    auto* processor = LookUpIndexProcessor::instance(kvstore_,
                                                     schemaMan_,
                                                     indexMan_,
                                                     &lookupVerticesQpsStat_,
                                                     executor,
                                                     &vertexCache_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::SampleVertexResp> StorageServiceHandler::future_sampleVertex(
    const cpp2::SampleVertexRequest& req) {
    auto* processor =
        SampleVertexProcessor::instance(statisticStore_, schemaMan_, &sampleVertexQpsStat_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::SampleEdgeResp> StorageServiceHandler::future_sampleEdge(
    const cpp2::SampleEdgeRequest& req) {
    auto* processor =
        SampleEdgeProcessor::instance(statisticStore_, schemaMan_, &sampleEdgeQpsStat_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::RebuildSampleResponse> StorageServiceHandler::future_rebuildSample(
    const cpp2::RebuildSampleRequest& req) {
    auto* processor = RebuildSampleProcessor::instance(kvstore_, statisticStore_, schemaMan_);
    RETURN_FUTURE(processor);
}

}  // namespace storage
}  // namespace nebula
