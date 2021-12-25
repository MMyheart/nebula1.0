/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_SAMPLENBEXECUTOR_H_
#define GRAPH_SAMPLENBEXECUTOR_H_

#include "base/Base.h"
#include "graph/TraverseExecutor.h"
#include "storage/client/StorageClient.h"

DECLARE_bool(filter_pushdown);

namespace nebula {

namespace storage {
namespace cpp2 {
class QueryResponse;
}   // namespace cpp2
}   // namespace storage

namespace graph {

class SampleNBExecutor final : public TraverseExecutor {
public:
    SampleNBExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char *name() const override {
        return "SampleNBExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

    void setupResponse(cpp2::ExecutionResponse &resp) override;

private:
    /**
     * To do some preparing works on the clauses
     */
    Status prepareClauses();

    Status prepareFrom();

    Status prepareOver();

    Status prepareWhere();

    Status prepareYield();

    void sampleNeighbor();

    StatusOr<std::vector<storage::cpp2::PropDef>> getPropNames();

    using RpcResponse = storage::StorageRpcResponse<storage::cpp2::QueryResponse>;

    void finishExecution(RpcResponse &&rpcResp);

    void buildSample();

    void handlerSample(VertexID srcId,
                       EdgeType edgeType,
                       std::vector<std::pair<VertexID, double>> &idWeightList,
                       double &edgeSumWeight);

    void generateResult();

    size_t randomSelect(std::vector<double> &sum_weights, size_t begin_pos, size_t end_pos);

    void onEmptyInputs();

    bool setupInterimResult(std::unique_ptr<InterimResult> &result) const;

private:
    SampleNBSentence *sentence_{nullptr};
    std::vector<EdgeType> edgeTypes_;
    std::vector<YieldColumn *> yields_;
    std::vector<std::string> resultColNames_{"edge", _SRC, _DST, _WEIGHT};
    using NeiborInfo = std::unordered_map<EdgeType, std::vector<std::pair<VertexID, double>>>;
    using SampleVexInfo =
        std::unordered_map<EdgeType, std::pair<std::vector<VertexID>, std::vector<double>>>;
    std::unordered_map<VertexID, NeiborInfo> neiborInfoMap_;
    std::unordered_map<VertexID, std::tuple<std::vector<EdgeType>, std::vector<double>, double>>
        sampleEdgeInfoMap_;
    std::unordered_map<VertexID, SampleVexInfo> sampleVexInfoMap_;
    std::vector<EdgeType> sampleEdges_;
    std::vector<double> sampleEdgeSumWeights_;
    std::unique_ptr<WhereWrapper> whereWrapper_;
    using InterimIndex = InterimResult::InterimResultIndex;
    std::unique_ptr<InterimIndex> index_;
    std::unique_ptr<ExpressionContext> expCtx_;
    std::vector<VertexID> starts_;
    std::unique_ptr<cpp2::ExecutionResponse> resp_;
    // The name of Tag or Edge, index of prop in data
    using SchemaPropIndex = std::unordered_map<std::pair<std::string, std::string>, int64_t>;
};

}   // namespace graph
}   // namespace nebula

#endif   // GRAPH_GOEXECUTOR_H_
