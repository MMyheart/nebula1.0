/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/SampleExecutor.h"
#include <boost/functional/hash.hpp>
#include "base/Base.h"
#include "dataman/ResultSchemaProvider.h"
#include "dataman/RowReader.h"
#include "dataman/RowSetReader.h"
#include "graph/SchemaHelper.h"

namespace nebula {
namespace graph {

SampleExecutor::SampleExecutor(Sentence *sentence, ExecutionContext *ectx)
    : TraverseExecutor(ectx, "sample") {
    // The RTTI is guaranteed by Sentence::Kind,
    // so we use `static_cast' instead of `dynamic_cast' for the sake of efficiency.
    sentence_ = static_cast<SampleSentence *>(sentence);
}

Status SampleExecutor::prepare() {
    return Status::OK();
}

Status SampleExecutor::prepareSampleLabels() {
    auto *lables = sentence_->sampleLabels();
    for (auto *label : lables->labels()) {
        auto labelStatus = checkIfSchemaExist(label);
        if (!labelStatus.ok()) {
            return labelStatus;
        }
    }
    return Status::OK();
}

Status SampleExecutor::prepareLimit() {
    count_ = sentence_->count();
    if (count_ < 0) {
        return Status::SyntaxError("limit `%u' is illegal", count_);
    }
    return Status::OK();
}

void SampleExecutor::execute() {
    Status status = checkIfGraphSpaceChosen();
    if (!status.ok()) {
        doError(std::move(status));
        return;
    }
    status = prepareLimit();
    if (!status.ok()) {
        doError(std::move(status));
        return;
    }
    status = prepareSampleLabels();
    if (!status.ok()) {
        doError(std::move(status));
        return;
    }
    status = prepareYield();
    if (!status.ok()) {
        doError(std::move(status));
        return;
    }
    sampleLabel();
}

void SampleExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    if (resp_ == nullptr) {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
    }
    resp = std::move(*resp_);
}

Status SampleVertexExecutor::prepareYield() {
    resultColumns_.reserve(2);
    resultColumns_.emplace_back("Tag");
    resultColumns_.emplace_back("VertexId");
    return Status::OK();
}
Status SampleVertexExecutor::checkIfSchemaExist(std::string *label) {
    auto spaceId = ectx()->rctx()->session()->space();
    auto tagStatus = ectx()->schemaManager()->toTagID(spaceId, *label);
    if (!tagStatus.ok()) {
        return tagStatus.status();
    }
    auto v = tagStatus.value();
    tagIds_.push_back(v);
    labelNames_[v] = *label;
    return Status::OK();
}
void SampleVertexExecutor::sampleLabel() {
    spaceId_ = ectx()->rctx()->session()->space();
    auto future = ectx()->getStorageClient()->sampleVertex(spaceId_, tagIds_, count_);
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this](auto &&resp) {
        auto completeness = resp.completeness();
        if (completeness == 0) {
            doError(Status::Error("Sample vertex failed"));
            return;
        } else if (completeness != 100) {
            LOG(INFO) << "Sample vertex partially failed: " << completeness << "%";
            for (auto &error : resp.failedParts()) {
                LOG(ERROR) << "part: " << error.first
                           << "error code: " << static_cast<int>(error.second);
            }
            ectx()->addWarningMsg("Sample vertex executor was partially performed");
        }
        processResult(std::move(resp));
    };
    auto error = [this](auto &&e) {
        LOG(ERROR) << "Exception when sample vertex : " << e.what();
        doError(Status::Error("Exception when sample vertex : %s.", e.what().c_str()));
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}

void SampleVertexExecutor::processResult(RpcSampleVertexResponse &&rpcResp) {
    resp_ = std::make_unique<cpp2::ExecutionResponse>();
    resp_->set_column_names(resultColumns_);
    std::vector<cpp2::RowValue> rowValue;
    rowValue.reserve(count_);
    for (auto &resp : rpcResp.responses()) {
        for (auto weight : resp.get_vertexWeights()) {
            std::vector<cpp2::ColumnValue> row;
            row.reserve(resultColumns_.size());
            row.emplace_back();
            row.back().set_str(labelNames_[weight.get_tagId()]);
            row.emplace_back();
            row.back().set_id(weight.get_vid());
            rowValue.emplace_back();
            rowValue.back().set_columns(std::move(row));
        }
    }
    resp_->set_rows(rowValue);
    doFinish(Executor::ProcessControl::kNext);
}

Status SampleEdgeExecutor::prepareYield() {
    resultColumns_.reserve(4);
    resultColumns_.emplace_back("Edge");
    resultColumns_.emplace_back(_SRC);
    resultColumns_.emplace_back(_DST);
    resultColumns_.emplace_back(_TYPE);
    weights_.reserve(edgeTypes_.size());
    for (auto it = edgeTypes_.begin(); it != edgeTypes_.end(); it++) {
        storage::cpp2::PropDef propDef;
        propDef.owner = storage::cpp2::PropOwner::EDGE;
        propDef.name = "w";
        propDef.id.set_edge_type(*it);
        weights_.emplace_back(propDef);
    }
    return Status::OK();
}

Status SampleEdgeExecutor::checkIfSchemaExist(std::string *label) {
    auto spaceId = ectx()->rctx()->session()->space();
    auto edgeStatus = ectx()->schemaManager()->toEdgeType(spaceId, *label);
    if (!edgeStatus.ok()) {
        return edgeStatus.status();
    }
    auto v = edgeStatus.value();
    edgeTypes_.push_back(v);
    edgeResp_[v] = std::make_pair(*label, std::vector<std::pair<EdgeID, double>>());
    counts_[v] = count_;
    labelNames_[v] = *label;
    return Status::OK();
}
void SampleEdgeExecutor::sampleLabel() {
    auto spaceId = ectx()->rctx()->session()->space();
    auto future = ectx()->getStorageClient()->sampleEdge(spaceId, edgeTypes_, count_);
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this](auto &&resp) {
        auto completeness = resp.completeness();
        if (completeness == 0) {
            doError(Status::Error("Sample edge failed"));
            return;
        } else if (completeness != 100) {
            LOG(INFO) << "Sample edge partially failed: " << completeness << "%";
            for (auto &error : resp.failedParts()) {
                LOG(ERROR) << "part: " << error.first
                           << "error code: " << static_cast<int>(error.second);
            }
            ectx()->addWarningMsg("Sample edge executor was partially performed");
        }
        processResult(std::move(resp));
    };
    auto error = [this](auto &&e) {
        LOG(ERROR) << "Exception when sample edge : " << e.what();
        doError(Status::Error("Exception when sample edge : %s.", e.what().c_str()));
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}

void SampleEdgeExecutor::processResult(RpcSampleEdgeResponse &&rpcResp) {
    resp_ = std::make_unique<cpp2::ExecutionResponse>();
    resp_->set_column_names(resultColumns_);
    std::vector<cpp2::RowValue> rowValue;
    rowValue.reserve(count_);
    for (auto &resp : rpcResp.responses()) {
        for (auto weight : resp.get_edgeWeights()) {
            std::vector<cpp2::ColumnValue> row;
            row.reserve(resultColumns_.size());
            row.emplace_back();
            row.back().set_str(labelNames_[weight.get_edgeType()]);
            row.emplace_back();
            row.back().set_id(weight.get_srcVid());
            row.emplace_back();
            row.back().set_id(weight.get_dstVid());
            row.emplace_back();
            row.back().set_id(weight.get_edgeType());
            rowValue.emplace_back();
            rowValue.back().set_columns(std::move(row));
        }
    }
    resp_->set_rows(rowValue);
    doFinish(Executor::ProcessControl::kNext);
}

}   // namespace graph
}   // namespace nebula
