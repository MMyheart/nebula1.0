/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_SCANEXECUTOR_H_
#define GRAPH_SCANEXECUTOR_H_

#include "base/Base.h"
#include "graph/TraverseExecutor.h"
#include "storage/client/StorageClient.h"

namespace nebula {
namespace graph {
class ScanExecutor final : public TraverseExecutor {
public:
    ScanExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char *name() const override {
        return "ScanExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

    void setupResponse(cpp2::ExecutionResponse &resp) override;

private:
    StatusOr<std::unique_ptr<InterimResult>> setupInterimResult();

    Status prepareExpr();

    ScanSentence *sentence_{nullptr};

    PartitionID partition_;
    std::string cursor_;
    int64_t startTime_{0};
    int64_t endTime_{std::numeric_limits<int64_t>::max()};
    int32_t limit_;
    std::unordered_map<TagID, std::vector<storage::cpp2::PropDef>> return_columns_;

    std::vector<std::string> colNames_;
    std::vector<cpp2::RowValue> rows_;

    std::shared_ptr<SchemaWriter> schema_;
};

}   // namespace graph
}   // namespace nebula
#endif
