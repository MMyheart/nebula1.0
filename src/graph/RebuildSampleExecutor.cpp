/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/RebuildSampleExecutor.h"

namespace nebula {
namespace graph {

RebuildSampleExecutor::RebuildSampleExecutor(Sentence *sentence, ExecutionContext *ectx)
    : Executor(ectx) {
    sentence_ = static_cast<RebuildSampleSentence *>(sentence);
}

Status RebuildSampleExecutor::prepare() {
    return Status::OK();
}
Status RebuildSampleExecutor::prepareClause() {
    if (sentence_->isAll()) {
        prepareAllTag();
        prepareAllEdge();
        return Status::OK();
    }
    if (sentence_->isTag()) {
        if (sentence_->sampleLabels() == nullptr) {
            prepareAllTag();
            return Status::OK();
        }
        std::vector<std::string *> labels = sentence_->sampleLabels()->labels();
        for (auto it = labels.begin(); it != labels.end(); it++) {
            auto tagStatus = ectx()->schemaManager()->toTagID(spaceId_, **it);
            if (!tagStatus.ok()) {
                return tagStatus.status();
            }
            auto tagId = tagStatus.value();
            tagIds_.emplace_back(tagId);
        }
    }
    if (sentence_->isEdge()) {
        if (sentence_->sampleLabels() == nullptr) {
            prepareAllEdge();
            return Status::OK();
        }
        std::vector<std::string *> labels = sentence_->sampleLabels()->labels();
        for (auto it = labels.begin(); it != labels.end(); it++) {
            auto edgeStatus = ectx()->schemaManager()->toEdgeType(spaceId_, **it);
            if (!edgeStatus.ok()) {
                return edgeStatus.status();
            }
            auto edgeType = edgeStatus.value();
            edgeTypes_.emplace_back(edgeType);
        }
    }
    return Status::OK();
}

Status RebuildSampleExecutor::prepareAllTag() {
    auto tagsStatus = ectx()->schemaManager()->getAllTag(spaceId_);
    if (!tagsStatus.ok()) {
        return tagsStatus.status();
    }
    std::unordered_set<TagID> tagIdSet;
    for (auto &tagName : std::move(tagsStatus).value()) {
        auto tagStatus = ectx()->schemaManager()->toTagID(spaceId_, tagName);
        if (!tagStatus.ok()) {
            return tagStatus.status();
        }
        auto tagId = tagStatus.value();
        auto result = tagIdSet.emplace(tagId);
        if (!result.second) {
            return Status::Error(folly::sformat("tag({}) was dup", tagName));
        }
    }
    tagIds_.assign(tagIdSet.begin(), tagIdSet.end());
    return Status::OK();
}

Status RebuildSampleExecutor::prepareAllEdge() {
    auto edgesStatus = ectx()->schemaManager()->getAllEdge(spaceId_);
    if (!edgesStatus.ok()) {
        return edgesStatus.status();
    }
    std::unordered_set<EdgeType> edgeTypeSet;
    for (auto &edgeName : std::move(edgesStatus).value()) {
        auto edgeStatus = ectx()->schemaManager()->toEdgeType(spaceId_, edgeName);
        if (!edgeStatus.ok()) {
            return edgeStatus.status();
        }
        auto edgeType = edgeStatus.value();
        auto result = edgeTypeSet.emplace(edgeType);
        if (!result.second) {
            return Status::Error(folly::sformat("edge({}) was dup", edgeName));
        }
    }
    edgeTypes_.assign(edgeTypeSet.begin(), edgeTypeSet.end());
    return Status::OK();
}

void RebuildSampleExecutor::execute() {
    auto status = checkIfGraphSpaceChosen();
    if (!status.ok()) {
        DCHECK(onError_);
        onError_(std::move(status));
        return;
    }
    spaceId_ = ectx()->rctx()->session()->space();
    status = prepareClause();
    if (!status.ok()) {
        DCHECK(onError_);
        onError_(std::move(status));
        return;
    }
    auto future = ectx()->getMetaClient()->rebuildSample(
        spaceId_, std::move(tagIds_), std::move(edgeTypes_), sentence_->isForce());
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this](auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(resp.status());
            return;
        }

        DCHECK(onFinish_);
        onFinish_(Executor::ProcessControl::kNext);
    };

    auto error = [this](auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        onError_(Status::Error("Internal error"));
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

}   // namespace graph
}   // namespace nebula
