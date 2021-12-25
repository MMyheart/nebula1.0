/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_REBUILDSAMPLEEXECUTOR_H_
#define GRAPH_REBUILDSAMPLEEXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {

class RebuildSampleExecutor final : public Executor {
public:
    RebuildSampleExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char *name() const override {
        return "RebuildSampleExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

    Status MUST_USE_RESULT prepareClause();

    Status prepareAllTag();

    Status prepareAllEdge();

private:
    RebuildSampleSentence *sentence_{nullptr};
    GraphSpaceID spaceId_;
    std::vector<TagID> tagIds_;
    std::vector<EdgeType> edgeTypes_;
};

}   // namespace graph
}   // namespace nebula

#endif   // GRAPH_REBUILDSAMPLEEXECUTOR_H_
