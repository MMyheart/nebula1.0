/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_SAMPLEEXECUTOR_H_
#define GRAPH_SAMPLEEXECUTOR_H_

#include "base/Base.h"
#include "graph/TraverseExecutor.h"
#include "storage/client/StorageClient.h"
#include "common/sample/WeightedCollection.h"


namespace nebula {

namespace graph {

using RpcSampleVertexResponse = storage::StorageRpcResponse<storage::cpp2::SampleVertexResp>;
using RpcSampleEdgeResponse = storage::StorageRpcResponse<storage::cpp2::SampleEdgeResp>;
using EdgeID = std::tuple<VertexID, VertexID, EdgeType>;

class SampleExecutor : public TraverseExecutor {
public:
    SampleExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "SampleExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

    void setupResponse(cpp2::ExecutionResponse &resp) override;

private:
    Status prepareSampleLabels();

    Status prepareLimit();

protected:
    virtual Status prepareYield() = 0;

    virtual Status checkIfSchemaExist(std::string *label) = 0;

    virtual void sampleLabel() = 0;

protected:
    SampleSentence                              *sentence_{nullptr};
    std::unique_ptr<cpp2::ExecutionResponse>    resp_;
    std::unique_ptr<YieldColumns>               yieldColumns_;
    std::unordered_map<int32_t, std::string>    labelNames_;
    std::vector<std::string>                    resultColumns_;
    std::vector<storage::cpp2::PropDef>         weights_;
    int32_t                                     count_;
    GraphSpaceID                                spaceId_;
};

class SampleVertexExecutor final : public SampleExecutor {
public:
    SampleVertexExecutor(Sentence *sentence, ExecutionContext *ectx)
        : SampleExecutor(sentence, ectx) {}

protected:
    Status prepareYield() override;

    Status checkIfSchemaExist(std::string *label) override;

    void sampleLabel() override;

private:
    void processResult(RpcSampleVertexResponse &&rpcResp);

private:
    std::vector<TagID>                          tagIds_;
};

class SampleEdgeExecutor final : public SampleExecutor {
public:
    SampleEdgeExecutor(Sentence *sentence, ExecutionContext *ectx)
        : SampleExecutor(sentence, ectx) {}

protected:
    Status prepareYield() override;

    Status checkIfSchemaExist(std::string *label) override;

    void sampleLabel() override;

    void randomToCount();

private:
    void processResult(RpcSampleEdgeResponse &&rpcResp);

private:
    std::vector<EdgeType>                       edgeTypes_;
    std::unordered_map<EdgeType, float>         edgeTypeWeights_;
    float                                       allEdgeWeights_;
    CompactWeightedCollection<EdgeType>         edgeTypeWeightCollection_;
    std::unordered_map<EdgeType,
        FastWeightedCollection<EdgeID>>         edgeWeightCollection_;
    std::unordered_map<EdgeType,
                       std::pair<std::string,
                                         std::vector<
                                                 std::pair<EdgeID, double>
                                                    >
                                 >
                       >                        edgeResp_;
    std::unordered_map<EdgeType, int32_t>       counts_;
};

}   // namespace graph
}   // namespace nebula


#endif  // GRAPH_SAMPLEEXECUTOR_H_
