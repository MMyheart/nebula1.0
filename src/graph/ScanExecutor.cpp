/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/ScanExecutor.h"
#include "base/Base.h"
#include "time/WallClock.h"
#include "utils/NebulaKeyUtils.h"

namespace nebula {
namespace graph {

ScanExecutor::ScanExecutor(Sentence *sentence, ExecutionContext *ectx)
    : TraverseExecutor(ectx, "scan") {
    sentence_ = static_cast<ScanSentence *>(sentence);
}

static Status _getInt(Expression *expr, int64_t &v) {
    Getters getters;

    auto status = expr->prepare();
    if (!status.ok()) {
        return status;
    }
    auto value = expr->eval(getters);
    if (!value.ok()) {
        return status;
    }
    auto r = value.value();
    if (!Expression::isInt(r)) {
        return Status::Error("should be of type integer");
    }
    v = Expression::asInt(r);

    return Status::OK();
}

Status ScanExecutor::prepare() {
    return Status::OK();
}

Status ScanExecutor::prepareExpr() {
    auto space = ectx()->rctx()->session()->space();

    auto tag = sentence_->tag();

    auto tagStatus = ectx()->schemaManager()->toTagID(space, tag);
    if (!tagStatus.ok()) {
        LOG(ERROR) << "No schema found for " << tag;
        return Status::Error("No schema found for `%s'", tag.c_str());
    }
    auto tagId = tagStatus.value();
    return_columns_[tagId] = {};

    {
        auto expr = sentence_->partition();
        int64_t partitionId = 0;
        auto status = _getInt(expr, partitionId);
        if (!status.ok()) {
            return status;
        }
        partition_ = partitionId;
    }

    {
        auto expr = sentence_->from();
        if (expr) {
            VertexID vertexId = 0;
            auto status = _getInt(expr, vertexId);
            if (!status.ok()) {
                return status;
            }
            cursor_ = NebulaKeyUtils::vertexPrefix(partition_, vertexId);
        }
    }

    {
        auto expr = sentence_->limit();
        if (expr) {
            int64_t limit = 0;
            auto status = _getInt(expr, limit);
            if (!status.ok()) {
                return status;
            }
            limit_ = limit;
        } else {
            limit_ = std::numeric_limits<int32_t>::max();
        }
    }

    return Status::OK();
}

void ScanExecutor::execute() {
    auto exprStatus = prepareExpr();
    if (!exprStatus.ok()) {
        doError(std::move(exprStatus));
        return;
    }

    auto space = ectx()->rctx()->session()->space();

    VLOG(1) << "ScanVertex: space:" << space << " partition: " << partition_
            << " startTime: " << startTime_ << " endTime: " << endTime_ << " limit: " << limit_;

    auto future = ectx()->getStorageClient()->ScanVertex(
        space, partition_, cursor_, return_columns_, true, limit_, startTime_, endTime_);

    auto *runner = ectx()->rctx()->runner();
    auto cb = [this](StatusOr<storage::cpp2::ScanVertexResponse> &&resultStatus) {
        if (!resultStatus.ok()) {
            LOG(ERROR) << "ScanVertex error " << resultStatus.status().toString();
            doError(resultStatus.status());
            return;
        }
        auto result = std::move(resultStatus).value();
        if (!result.get_result().get_failed_codes().empty()) {
            for (auto &error : result.get_result().failed_codes) {
                LOG(INFO) << "ScanVertex failed, part: " << error.get_part_id()
                          << " error code: " << static_cast<int>(error.get_code());
            }
            doError(Status::Error("ScanVertex failed."));
            return;
        }

        if (result.get_vertex_schema().size() != 1) {
            LOG(ERROR) << "ScanVertex return vertex_schema size != 1.";
            doError(Status::Error("ScanVertex return vertex_schema size != 1."));
            return;
        }
        colNames_.emplace_back("VertexID");
        auto schema =
            std::make_shared<ResultSchemaProvider>(result.get_vertex_schema().begin()->second);
        schema_ = std::make_shared<SchemaWriter>();
        schema_->appendCol("VertexID", nebula::cpp2::SupportedType::VID);
        for (auto &iter : *schema) {
            colNames_.emplace_back(iter.getName());
            schema_->appendCol(iter.getName(), iter.getType().get_type());
        }

        rows_.reserve(result.get_vertex_data().size());
        for (auto &data : result.get_vertex_data()) {
            cpp2::RowValue row;
            row.columns.reserve(colNames_.size());

            cpp2::ColumnValue vid;
            vid.set_id(data.get_vertexId());
            row.columns.emplace_back(vid);

            auto reader = RowReader::getRowReader(data.get_value(), schema);
            for (unsigned i = 0; i < schema->getNumFields(); i++) {
                cpp2::ColumnValue col;
                switch (schema->getFieldType(i).get_type()) {
                    case nebula::cpp2::SupportedType::BOOL: {
                        bool v;
                        auto ret = reader->getBool(i, v);
                        if (ret != ResultType::SUCCEEDED) {
                            LOG(ERROR) << "ScanVertex return unsupported type.";
                            doError(Status::Error("ScanVertex return unsupported type."));
                            return;
                        }
                        col.set_bool_val(v);
                    } break;
                    case nebula::cpp2::SupportedType::INT: {
                        int64_t v;
                        auto ret = reader->getInt(i, v);
                        if (ret != ResultType::SUCCEEDED) {
                            LOG(ERROR) << "ScanVertex return unsupported type.";
                            doError(Status::Error("ScanVertex return unsupported type."));
                            return;
                        }
                        col.set_integer(v);
                    } break;
                    case nebula::cpp2::SupportedType::TIMESTAMP: {
                        int64_t v;
                        auto ret = reader->getInt(i, v);
                        if (ret != ResultType::SUCCEEDED) {
                            LOG(ERROR) << "ScanVertex return unsupported type.";
                            doError(Status::Error("ScanVertex return unsupported type."));
                            return;
                        }
                        col.set_timestamp(v);
                    } break;
                    case nebula::cpp2::SupportedType::VID: {
                        VertexID v;
                        auto ret = reader->getVid(i, v);
                        if (ret != ResultType::SUCCEEDED) {
                            LOG(ERROR) << "ScanVertex return unsupported type.";
                            doError(Status::Error("ScanVertex return unsupported type."));
                            return;
                        }
                        col.set_id(v);
                    } break;
                    case nebula::cpp2::SupportedType::FLOAT: {
                        float v;
                        auto ret = reader->getFloat(i, v);
                        if (ret != ResultType::SUCCEEDED) {
                            LOG(ERROR) << "ScanVertex return unsupported type.";
                            doError(Status::Error("ScanVertex return unsupported type."));
                            return;
                        }
                        col.set_single_precision(v);
                    } break;
                    case nebula::cpp2::SupportedType::DOUBLE: {
                        double v;
                        auto ret = reader->getDouble(i, v);
                        if (ret != ResultType::SUCCEEDED) {
                            LOG(ERROR) << "ScanVertex return unsupported type.";
                            doError(Status::Error("ScanVertex return unsupported type."));
                            return;
                        }
                        col.set_double_precision(v);
                    } break;
                    case nebula::cpp2::SupportedType::STRING: {
                        folly::StringPiece v;
                        auto ret = reader->getString(i, v);
                        if (ret != ResultType::SUCCEEDED) {
                            LOG(ERROR) << "ScanVertex return unsupported type.";
                            doError(Status::Error("ScanVertex return unsupported type."));
                            return;
                        }
                        col.set_str(v);
                    } break;
                    case nebula::cpp2::SupportedType::YEAR:
                    case nebula::cpp2::SupportedType::YEARMONTH:
                    case nebula::cpp2::SupportedType::DATE:
                    case nebula::cpp2::SupportedType::DATETIME:
                    case nebula::cpp2::SupportedType::PATH:
                    case nebula::cpp2::SupportedType::UNKNOWN:
                        LOG(ERROR) << "ScanVertex return unsupported type.";
                        doError(Status::Error("ScanVertex return unsupported type."));
                        break;
                }
                row.columns.emplace_back(col);
            }

            rows_.emplace_back(std::move(row));
        }
        if (onResult_) {
            auto status = setupInterimResult();
            if (!status.ok()) {
                DCHECK(onError_);
                onError_(std::move(status).status());
                return;
            }
            onResult_(std::move(status).value());
        }

        doFinish(Executor::ProcessControl::kNext);
    };
    auto error = [this](auto &&e) {
        LOG(ERROR) << "Exception when handle scan: " << e.what();
        doError(Status::Error("Exeception when handle scan: %s.", e.what().c_str()));
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}

StatusOr<std::unique_ptr<InterimResult>> ScanExecutor::setupInterimResult() {
    auto result = std::make_unique<InterimResult>(std::move(colNames_));
    if (rows_.empty()) {
        return result;
    }

    auto rsWriter = std::make_unique<RowSetWriter>(schema_);
    using Type = cpp2::ColumnValue::Type;
    for (auto &row : rows_) {
        RowWriter writer(schema_);
        auto columns = row.get_columns();
        for (auto &column : columns) {
            switch (column.getType()) {
                case cpp2::ColumnValue::Type::id:
                    writer << column.get_id();
                    break;
                case Type::integer:
                    writer << column.get_integer();
                    break;
                case Type::double_precision:
                    writer << column.get_double_precision();
                    break;
                case Type::bool_val:
                    writer << column.get_bool_val();
                    break;
                case Type::str:
                    writer << column.get_str();
                    break;
                case cpp2::ColumnValue::Type::timestamp:
                    writer << column.get_timestamp();
                    break;
                default:
                    LOG(ERROR) << "Not Support type: " << column.getType();
                    return Status::Error("Not Support type: %d", column.getType());
            }
        }
        rsWriter->addRow(writer);
    }

    if (rsWriter != nullptr) {
        result->setInterim(std::move(rsWriter));
    }
    return result;
}

void ScanExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    resp.set_column_names(std::move(colNames_));

    if (rows_.empty()) {
        return;
    }
    resp.set_rows(std::move(rows_));
}

}   // namespace graph
}   // namespace nebula
