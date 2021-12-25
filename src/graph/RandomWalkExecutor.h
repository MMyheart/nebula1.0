/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_RANDOMWALKEXECUTOR_H_
#define GRAPH_RANDOMWALKEXECUTOR_H_

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

using NeiborInfo = std::unordered_map<EdgeType, std::vector<std::pair<VertexID, double>>>;

class RandomWalkExecutor final : public TraverseExecutor {
public:
    RandomWalkExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char *name() const override {
        return "RandomWalkExecutor";
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

    Status prepareStrategy();

    void randomWalk();

    void node2vecWalk(std::unordered_map<VertexID, NeiborInfo> &&neiborInfoMap);

    StatusOr<std::vector<storage::cpp2::PropDef>> getPropNames();

    using RpcResponse = storage::StorageRpcResponse<storage::cpp2::QueryResponse>;

    void randomWalkResponse(RpcResponse &&rpcResp);

    void sampleNeighbor(std::unordered_map<VertexID, NeiborInfo> &&neiborInfoMap);

    void generateResult();

    size_t randomSelect(std::vector<double> &sum_weights, size_t begin_pos, size_t end_pos);

    void buildWeights(VertexID parent_id,
                      std::vector<VertexID> &&parent_neighbors,
                      std::vector<VertexID> &neighbors,
                      std::vector<double> &weights);

    void onEmptyInputs();

    Status traversalExpr(const Expression *expr);

    bool setupInterimResult(std::unique_ptr<InterimResult> &result) const;

    enum WalkStrategy {
        traditional,
        node2vec,
    };

private:
    RandomWalkSentence *sentence_{nullptr};
    std::vector<EdgeType> edgeTypes_;
    std::vector<YieldColumn *> yields_;
    std::vector<std::string> resultColNames_{"path"};
    WalkStrategy walkStrategy_{traditional};
    double p_{1.0};
    double q_{1.0};
    std::unique_ptr<ExpressionContext> expCtx_;
    std::vector<VertexID> starts_;
    VertexID defaultID_{0};
    VertexID nonExistID_{-1};
    using RootID = VertexID;
    using ParentID = VertexID;
    std::unordered_map<RootID, std::vector<VertexID>> walkPath_;
    std::multimap<std::pair<uint32_t, VertexID>, RootID> backTracker_;

    using CurStepPNeighbors = std::pair<ParentID, std::vector<VertexID>>;
    std::unordered_map<RootID, CurStepPNeighbors> parent_neighbors_;
    uint32_t curStep_{1};
    std::unique_ptr<cpp2::ExecutionResponse> resp_;
};

}   // namespace graph
}   // namespace nebula

#endif   // GRAPH_RANDOMWALKEXECUTOR_H_
