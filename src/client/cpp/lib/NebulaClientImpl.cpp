/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "NebulaClientImpl.h"
#include "ConnectionPool.h"
#include "base/Base.h"
#include "time/Duration.h"

namespace nebula {
namespace graph {

void NebulaClientImpl::initEnv(int argc, char* argv[]) {
    LOG(ERROR) << "NebulaClientImpl initEnv {}" << argc;
    folly::init(&argc, &argv, true);
}

void NebulaClientImpl::initConnectionPool(const ConnectionInfo& addrInfo) {
    if (!ConnectionPool::instance().init(addrInfo)) {
        LOG(ERROR) << "Init pool failed";
        return;
    }
}

NebulaClientImpl::NebulaClientImpl() {
    connection_ = ConnectionPool::instance().getConnection(indexId_);
}

NebulaClientImpl::~NebulaClientImpl() {
    signout();
}

void NebulaClientImpl::signout() {
    if (connection_ == nullptr) {
        return;
    }
    connection_ = nullptr;
    ConnectionPool::instance().returnConnection(indexId_);
}

void NebulaClientImpl::feedPath(const cpp2::Path& inPath, Path& outPath) {
    auto entryList = inPath.get_entry_list();
    std::vector<PathEntry> pathEntrys;
    for (auto& entry : entryList) {
        PathEntry pathEntry;
        if (entry.getType() == cpp2::PathEntry::vertex) {
            Vertex vertex;
            vertex.id = entry.get_vertex().get_id();
            pathEntry.setVertexValue(std::move(vertex));
            pathEntrys.emplace_back(pathEntry);
            continue;
        }

        if (entry.getType() == cpp2::PathEntry::edge) {
            auto e = entry.get_edge();
            Edge edge;
            edge.type = e.get_type();
            edge.ranking = e.get_ranking();
            if (e.get_src() != nullptr) {
                edge.src = *e.get_src();
            }
            if (e.get_dst() != nullptr) {
                edge.dst = *e.get_dst();
            }
            pathEntry.setEdgeValue(std::move(edge));
            pathEntrys.emplace_back(std::move(pathEntry));
        }
    }
    outPath.setEntryList(std::move(pathEntrys));
}

void NebulaClientImpl::feedRows(const cpp2::ExecutionResponse& inResp, ExecutionResponse& outResp) {
    using nebula::graph::cpp2::ColumnValue;
    std::vector<RowValue> rows;
    for (auto& row : (*inResp.get_rows())) {
        std::vector<ColValue> columns;
        for (auto& col : row.get_columns()) {
            columns.emplace_back();
            switch (col.getType()) {
                case ColumnValue::Type::__EMPTY__: {
                    break;
                }
                case ColumnValue::Type::bool_val: {
                    columns.back().setBoolValue(col.get_bool_val());
                    break;
                }
                case ColumnValue::Type::integer: {
                    columns.back().setIntValue(col.get_integer());
                    break;
                }
                case ColumnValue::Type::id: {
                    columns.back().setIdValue(col.get_id());
                    break;
                }
                case ColumnValue::Type::single_precision: {
                    columns.back().setFloatValue(col.get_single_precision());
                    break;
                }
                case ColumnValue::Type::double_precision: {
                    columns.back().setDoubleValue(col.get_double_precision());
                    break;
                }
                case ColumnValue::Type::str: {
                    columns.back().setStrValue(std::move(col.get_str()));
                    break;
                }
                case ColumnValue::Type::timestamp: {
                    columns.back().setTimestampValue(col.get_timestamp());
                    break;
                }
                case ColumnValue::Type::year: {
                    break;
                }
                case ColumnValue::Type::month: {
                    break;
                }
                case ColumnValue::Type::date: {
                    break;
                }
                case ColumnValue::Type::datetime: {
                    break;
                }
                case ColumnValue::Type::path: {
                    Path path;
                    feedPath(col.get_path(), path);
                    columns.back().setPathValue(std::move(path));
                    break;
                }
            }
        }
        rows.emplace_back();
        rows.back().setColumns(std::move(columns));
    }
    outResp.setRows(std::move(rows));
}

cpp2::ErrorCode NebulaClientImpl::execute(folly::StringPiece stmt, cpp2::ExecutionResponse& resp) {
    if (!connection_) {
        LOG(ERROR) << "Disconnected from the server";
        return cpp2::ErrorCode::E_DISCONNECTED;
    }
    auto status = connection_->execute(stmt.toString()).get();
    if (!status.ok()) {
        return cpp2::ErrorCode::E_RPC_FAILURE;
    }
    resp = std::move(status).value();
    return resp.get_error_code();
}

ErrorCode NebulaClientImpl::doExecute(std::string stmt, ExecutionResponse& resp) {
    time::Duration duration;
    cpp2::ExecutionResponse response;
    cpp2::ErrorCode code = execute(stmt, response);
    if (code == cpp2::ErrorCode::E_RPC_FAILURE) {
        graph::ConnectionPool::instance().destoryConnection(indexId_);
        connection_ = ConnectionPool::instance().getConnection(indexId_);
        code = execute(stmt, response);
    }

    resp.setWholeLatencyInUss(duration.elapsedInUSec());
    resp.setErrorCode(static_cast<ErrorCode>(code));
    resp.setLatencyInUs(response.get_latency_in_us());
    if (response.get_error_msg() != nullptr) {
        LOG(ERROR) << "Execute \"" << stmt << "\" failed, error: " << *response.get_error_msg();
        resp.setErrorMsg(std::move(*response.get_error_msg()));
    }
    if (response.get_space_name() != nullptr) {
        resp.setSpaceName(std::move(*response.get_space_name()));
    }
    if (response.get_column_names() != nullptr) {
        resp.setColumnNames(std::move(*response.get_column_names()));
    }
    if (response.get_rows() != nullptr) {
        feedRows(response, resp);
    }
    return static_cast<ErrorCode>(code);
}

}   // namespace graph
}   // namespace nebula
