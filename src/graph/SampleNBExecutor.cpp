/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/SampleNBExecutor.h"
#include <boost/functional/hash.hpp>
#include "base/Base.h"
#include "dataman/ResultSchemaProvider.h"
#include "dataman/RowReader.h"
#include "dataman/RowSetReader.h"
#include "graph/SchemaHelper.h"

DEFINE_bool(filter_pushdown_sampleNB, true, "If pushdown the filter to storage.");
DEFINE_bool(trace_sampleNB, false, "Whether to dump the detail trace log from one go request");

namespace nebula {
namespace graph {

using SchemaProps = std::unordered_map<std::string, std::vector<std::string>>;
using nebula::cpp2::SupportedType;

SampleNBExecutor::SampleNBExecutor(Sentence *sentence, ExecutionContext *ectx)
    : TraverseExecutor(ectx, "sampleNB") {
    // The RTTI is guaranteed by Sentence::Kind,
    // so we use `static_cast' instead of `dynamic_cast' for the sake of efficiency.
    sentence_ = static_cast<SampleNBSentence *>(sentence);
}

Status SampleNBExecutor::prepare() {
    return Status::OK();
}

Status SampleNBExecutor::prepareClauses() {
    DCHECK(sentence_ != nullptr);
    Status status;
    expCtx_ = std::make_unique<ExpressionContext>();
    expCtx_->setStorageClient(ectx()->getStorageClient());

    do {
        status = checkIfGraphSpaceChosen();
        if (!status.ok()) {
            break;
        }

        status = prepareFrom();
        if (!status.ok()) {
            break;
        }

        //  before prepareYield()
        status = prepareOver();
        if (!status.ok()) {
            break;
        }

        status = prepareYield();
        if (!status.ok()) {
            break;
        }
        status = prepareWhere();
        if (!status.ok()) {
            break;
        }
    } while (false);

    if (!status.ok()) {
        LOG(ERROR) << "Preparing failed: " << status;
        return status;
    }

    return status;
}

void SampleNBExecutor::execute() {
    auto status = prepareClauses();
    if (!status.ok()) {
        doError(std::move(status));
        return;
    }

    if (starts_.empty()) {
        onEmptyInputs();
        return;
    }

    sampleNeighbor();
}

Status SampleNBExecutor::prepareFrom() {
    Status status = Status::OK();
    auto *clause = sentence_->fromClause();
    do {
        if (clause == nullptr) {
            LOG(ERROR) << "From clause shall never be null";
            return Status::Error("From clause shall never be null");
        }

        auto space = ectx()->rctx()->session()->space();
        expCtx_->setSpace(space);
        auto vidList = clause->vidList();
        Getters getters;
        for (auto *expr : vidList) {
            expr->setContext(expCtx_.get());

            status = expr->prepare();
            if (!status.ok()) {
                break;
            }
            auto value = expr->eval(getters);
            if (!value.ok()) {
                status = Status::Error();
                break;
            }
            if (expr->isFunCallExpression()) {
                auto *funcExpr = static_cast<FunctionCallExpression *>(expr);
                if (*(funcExpr->name()) == "near") {
                    auto v = Expression::asString(value.value());
                    std::vector<VertexID> result;
                    folly::split(",", v, result, true);
                    starts_.insert(starts_.end(),
                                   std::make_move_iterator(result.begin()),
                                   std::make_move_iterator(result.end()));
                    continue;
                }
            }
            auto v = value.value();
            if (!Expression::isInt(v)) {
                status = Status::Error("Vertex ID should be of type integer");
                break;
            }
            starts_.push_back(Expression::asInt(v));
        }

        if (!status.ok()) {
            break;
        }
    } while (false);
    return status;
}

Status SampleNBExecutor::prepareOver() {
    Status status = Status::OK();
    auto *clause = sentence_->overClause();
    if (clause == nullptr) {
        LOG(ERROR) << "Over clause shall never be null";
        return Status::Error("Over clause shall never be null");
    }
    auto edges = clause->edges();
    for (auto e : edges) {
        auto spaceId = ectx()->rctx()->session()->space();
        auto edgeStatus = ectx()->schemaManager()->toEdgeType(spaceId, *e->edge());
        if (!edgeStatus.ok()) {
            return edgeStatus.status();
        }

        auto v = edgeStatus.value();
        edgeTypes_.push_back(v);

        if (e->alias() != nullptr) {
            if (!expCtx_->addEdge(*e->alias(), std::abs(v))) {
                return Status::Error(folly::sformat("edge alias({}) was dup", *e->alias()));
            }
        } else {
            if (!expCtx_->addEdge(*e->edge(), std::abs(v))) {
                return Status::Error(folly::sformat("edge alias({}) was dup", *e->edge()));
            }
        }
    }

    return status;
}

Status SampleNBExecutor::prepareWhere() {
    auto *clause = sentence_->whereClause();
    whereWrapper_ = std::make_unique<WhereWrapper>(clause);
    auto status = whereWrapper_->prepare(expCtx_.get());
    return status;
}

Status SampleNBExecutor::prepareYield() {
    Status status = Status::OK();
    // init _dst weight
    for (const auto &alias : expCtx_->getEdgeAlias()) {
        {
            Expression *dst_exp = new EdgeDstIdExpression(new std::string(alias));
            dst_exp->setContext(expCtx_.get());
            YieldColumn *dst_column = new YieldColumn(dst_exp);
            status = dst_column->expr()->prepare();
            if (!status.ok()) {
                break;
            }
            yields_.emplace_back(dst_column);
        }
        {
            Expression *exp = new AliasPropertyExpression(
                new std::string(""), new std::string(alias), new std::string(_WEIGHT));
            exp->setContext(expCtx_.get());
            YieldColumn *column = new YieldColumn(exp);
            status = column->expr()->prepare();
            if (!status.ok()) {
                break;
            }
            yields_.emplace_back(column);
        }
    }
    return status;
}

void SampleNBExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    if (resp_ == nullptr) {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
    }
    resp = std::move(*resp_);
}

void SampleNBExecutor::sampleNeighbor() {
    auto spaceId = ectx()->rctx()->session()->space();
    auto status = getPropNames();
    if (!status.ok()) {
        doError(std::move(status).status());
        return;
    }
    auto returns = status.value();
    std::string filterPushdown = "";
    if (FLAGS_filter_pushdown_sampleNB) {
        // TODO: not support filter pushdown in reversely traversal now.
        // filterPushdown = whereWrapper_->filterPushdown_;
    }
    VLOG(1) << "edge type size: " << edgeTypes_.size() << " return cols: " << returns.size();
    auto future = ectx()->getStorageClient()->getNeighbors(
        spaceId, starts_, edgeTypes_, filterPushdown, std::move(returns));
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this](auto &&result) {
        auto completeness = result.completeness();
        if (completeness == 0) {
            doError(Status::Error("Get neighbors failed"));
            return;
        } else if (completeness != 100) {
            LOG(INFO) << "Get neighbors partially failed: " << completeness << "%";
            for (auto &error : result.failedParts()) {
                LOG(ERROR) << "part: " << error.first
                           << "error code: " << static_cast<int>(error.second);
            }
        }
        if (FLAGS_trace_sampleNB) {
            LOG(INFO) << "SampleNB finished, total request vertices " << starts_.size();
            auto &hostLatency = result.hostLatency();
            for (size_t i = 0; i < hostLatency.size(); i++) {
                LOG(INFO) << std::get<0>(hostLatency[i]) << ", time cost "
                          << std::get<1>(hostLatency[i]) << "us / " << std::get<2>(hostLatency[i])
                          << "us, total results " << result.responses()[i].get_vertices()->size();
            }
        }
        finishExecution(std::move(result));
    };
    auto error = [this](auto &&e) {
        LOG(ERROR) << "SampleNB Exception when handle out-bounds/in-bounds: " << e.what();
        doError(Status::Error("Exception when handle out-bounds/in-bounds: %s.", e.what().c_str()));
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}

void SampleNBExecutor::finishExecution(RpcResponse &&rpcResp) {
    VertexID srcId = 0;
    EdgeType edgeType = 0;
    VertexID dstId = 0;
    RowReader reader = RowReader::getEmptyRowReader();
    std::unordered_map<EdgeType, std::shared_ptr<ResultSchemaProvider>> edgeSchema;

    Getters getters;
    getters.getEdgeDstId =
        [this, &dstId, &edgeType](const std::string &edgeName) -> OptVariantType {
        if (edgeTypes_.size() > 1) {
            EdgeType type;
            auto found = expCtx_->getEdgeType(edgeName, type);
            if (!found) {
                return Status::Error("Get edge type for `%s' failed in getters.", edgeName.c_str());
            }
            if (type != std::abs(edgeType)) {
                return 0L;
            }
        }
        return dstId;
    };

    getters.getAliasProp = [&reader, &srcId, &edgeType, this](
                               const std::string &edgeName,
                               const std::string &prop) mutable -> OptVariantType {
        EdgeType type;
        auto found = expCtx_->getEdgeType(edgeName, type);
        if (!found) {
            return Status::Error("Get edge type for `%s' failed in getters.", edgeName.c_str());
        }
        if (std::abs(edgeType) != type) {
            //            auto sit = edgeSchema.find(type);
            //            if (sit == edgeSchema.end()) {
            //                std::string errMsg = folly::stringPrintf(
            //                        "Can't find shcema for %s when get default.",
            //                        edgeName.c_str());
            //                LOG(ERROR) << errMsg;
            //                return Status::Error(errMsg);
            //            }
            //            return RowReader::getDefaultProp(sit->second.get(), prop);
            std::string errMsg =
                folly::stringPrintf("Get edge type `%s' inconsistent.", edgeName.c_str());
            return Status::Error(errMsg);
        }

        if (prop == _SRC) {
            return srcId;
        }
        DCHECK(reader != nullptr);
        auto res = RowReader::getPropByName(reader.get(), prop);
        if (!ok(res)) {
            LOG(ERROR) << "Can't get prop for " << prop << ", edge " << edgeName;
            return Status::Error(folly::sformat("get prop({}.{}) failed", edgeName, prop));
        }
        return value(std::move(res));
    };   // getAliasProp

    const auto &all = rpcResp.responses();
    for (const auto &resp : all) {
        if (resp.get_vertices() == nullptr) {
            continue;
        }
        VLOG(1) << "Total resp.vertices size " << resp.vertices.size();
        for (const auto &vdata : resp.vertices) {
            VLOG(1) << "Total vdata.edge_data size " << vdata.edge_data.size();
            srcId = vdata.get_vertex_id();
            auto func = [&]() mutable {
                NeiborInfo neiborInfo_;
                for (const auto &edata : vdata.edge_data) {
                    edgeType = edata.type;
                    auto *eschema = resp.get_edge_schema();
                    if (eschema != nullptr) {
                        std::transform(
                            eschema->cbegin(),
                            eschema->cend(),
                            std::inserter(edgeSchema, edgeSchema.begin()),
                            [](auto &schema) {
                                return std::make_pair(
                                    schema.first,
                                    std::make_shared<ResultSchemaProvider>(schema.second));
                            });
                    }
                    VLOG(1) << "Total edata.edges size " << edata.edges.size() << ", for edge "
                            << edgeType;
                    std::shared_ptr<ResultSchemaProvider> currEdgeSchema;
                    auto it = edgeSchema.find(edgeType);
                    if (it != edgeSchema.end()) {
                        currEdgeSchema = it->second;
                    }
                    VLOG(1) << "CurrEdgeSchema is null? " << (currEdgeSchema == nullptr);
                    for (const auto &edge : edata.edges) {
                        dstId = edge.get_dst();
                        if (currEdgeSchema) {
                            reader = RowReader::getRowReader(edge.props, currEdgeSchema);
                        } else {
                            reader = RowReader::getEmptyRowReader();
                        }
                        double v;
                        for (auto *column : yields_) {
                            auto *expr = column->expr();
                            if (expr->isAliasExpression()) {
                                auto value = expr->eval(getters);
                                if (value.ok()) {
                                    auto *e0 = static_cast<const AliasPropertyExpression *>(expr);
                                    if (*(e0->prop()) == _WEIGHT) {
                                        v = boost::get<double>(std::move(value.value()));
                                    }
                                }
                            }
                        }
                        neiborInfo_[edgeType].emplace_back(
                            std::pair<VertexID, double>(edge.get_dst(), v));
                    }
                }
                if (neiborInfo_.size() > 0) {
                    neiborInfoMap_.insert(
                        std::pair<VertexID, NeiborInfo>(srcId, std::move(neiborInfo_)));
                }
                return true;
            };

            if (!func()) {
                return;
            }
        }   // for vdata
    }       // for `resp'
    // buildTagEdgeSample();
    buildSample();
    generateResult();
}

void SampleNBExecutor::buildSample() {
    if (neiborInfoMap_.size() > 0) {
        for (auto &info : neiborInfoMap_) {
            auto srcId = info.first;
            auto neiborInfo_ = info.second;
            double edgeSumWeight = 0.0;
            for (auto &elem : edgeTypes_) {
                auto iter = neiborInfo_.find(elem);
                if (iter != neiborInfo_.end()) {
                    handlerSample(srcId, iter->first, iter->second, edgeSumWeight);
                }
            }
            sampleEdgeInfoMap_[srcId] =
                std::tuple<std::vector<EdgeType>, std::vector<double>, double>(
                    std::move(sampleEdges_), std::move(sampleEdgeSumWeights_), edgeSumWeight);
        }
    }
}

void SampleNBExecutor::handlerSample(VertexID srcId,
                                     EdgeType edgeType,
                                     std::vector<std::pair<VertexID, double>> &idWeightList,
                                     double &edgeSumWeight) {
    std::vector<VertexID> ids;
    std::vector<double> sum_weights;
    ids.resize(idWeightList.size());
    sum_weights.resize(idWeightList.size());
    double sum_weight_ = 0.0;
    for (uint64_t j = 0; j < idWeightList.size(); j++) {
        std::pair<VertexID, double> idWeightPair = idWeightList[j];
        ids[j] = idWeightPair.first;
        sum_weight_ += idWeightPair.second;
        sum_weights[j] = sum_weight_;
    }
    edgeSumWeight += sum_weight_;
    sampleEdges_.push_back(edgeType);
    sampleEdgeSumWeights_.push_back(edgeSumWeight);
    sampleVexInfoMap_[srcId][edgeType] = std::pair<std::vector<VertexID>, std::vector<double>>(
        std::move(ids), std::move(sum_weights));
}

void SampleNBExecutor::generateResult() {
    std::vector<cpp2::RowValue> rows;
    resp_ = std::make_unique<cpp2::ExecutionResponse>();
    resp_->set_column_names(resultColNames_);
    for (auto srcId : starts_) {
        auto edgeIter = sampleEdgeInfoMap_.find(srcId);
        auto vertexIter = sampleVexInfoMap_.find(srcId);
        if (edgeIter != sampleEdgeInfoMap_.end() && vertexIter != sampleVexInfoMap_.end()) {
            auto sampleVexInfo = vertexIter->second;
            auto sampleEdgeTuple = edgeIter->second;
            double edgeSumWeight = std::get<2>(sampleEdgeTuple);
            if (edgeSumWeight <= 0.0) {
                continue;
            }
            auto edgeSumWeights = std::get<1>(sampleEdgeTuple);
            auto edgeTypes = std::get<0>(sampleEdgeTuple);
            for (int64_t i = 0; i < sentence_->count(); ++i) {
                // sample edge
                EdgeType sampleEdgeType = 0;
                if (edgeTypes.size() == 1) {
                    sampleEdgeType = edgeTypes[0];
                } else {
                    size_t mid = randomSelect(edgeSumWeights, 0, edgeSumWeights.size() - 1);
                    sampleEdgeType = edgeTypes[mid];
                }
                // sample node
                auto nodesIter = sampleVexInfo.find(sampleEdgeType);
                if (nodesIter != sampleVexInfo.end()) {
                    std::vector<cpp2::ColumnValue> row;
                    row.resize(4);
                    auto idWeigthPair = nodesIter->second;
                    auto mid = randomSelect(idWeigthPair.second, 0, idWeigthPair.second.size() - 1);
                    double pre_sum_weight = mid <= 0 ? 0 : idWeigthPair.second[mid - 1];
                    auto spaceId = ectx()->rctx()->session()->space();
                    auto edgeStatus = ectx()->schemaManager()->toEdgeName(spaceId, sampleEdgeType);
                    if (edgeStatus.ok()) {
                        row[0].set_str(edgeStatus.value());
                    } else {
                        row[0].set_str("");
                    }
                    row[1].set_integer(srcId);
                    row[2].set_integer(idWeigthPair.first[mid]);
                    row[3].set_double_precision(idWeigthPair.second[mid] - pre_sum_weight);
                    //
                    rows.emplace_back();
                    rows.back().set_columns(std::move(row));
                }
            }
        }
    }

    if (!rows.empty()) {
        resp_->set_rows(rows);
    }
    doFinish(Executor::ProcessControl::kNext);
}

StatusOr<std::vector<storage::cpp2::PropDef>> SampleNBExecutor::getPropNames() {
    std::vector<storage::cpp2::PropDef> props;
    auto spaceId_ = ectx()->rctx()->session()->space();
    for (auto &prop : expCtx_->aliasProps()) {
        storage::cpp2::PropDef pd;
        pd.owner = storage::cpp2::PropOwner::EDGE;
        pd.name = prop.second;
        auto status = ectx()->schemaManager()->toEdgeType(spaceId_, prop.first);
        if (!status.ok()) {
            return Status::Error("No schema found for '%s'", prop.first.c_str());
        }
        auto edgeType = status.value();
        pd.id.set_edge_type(edgeType);
        props.emplace_back(std::move(pd));
    }
    return props;
}

void SampleNBExecutor::onEmptyInputs() {
    auto outputs = std::make_unique<InterimResult>(std::move(resultColNames_));
    if (onResult_) {
        onResult_(std::move(outputs));
    } else if (resp_ == nullptr) {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
    }
    doFinish(Executor::ProcessControl::kNext);
}

size_t SampleNBExecutor::randomSelect(std::vector<double> &sum_weights,
                                      size_t begin_pos,
                                      size_t end_pos) {
    float limit_begin = begin_pos == 0 ? 0 : sum_weights[begin_pos - 1];
    float limit_end = sum_weights[end_pos];
    static std::default_random_engine re(std::time(0));
    static std::uniform_real_distribution<double> u(0., 1.);
    double r = u(re) * (limit_end - limit_begin) + limit_begin;
    size_t low = begin_pos, high = end_pos, mid = 0;
    bool finish = false;
    while (low <= high && !finish) {
        mid = (low + high) / 2;
        float interval_begin = mid == 0 ? 0 : sum_weights[mid - 1];
        float interval_end = sum_weights[mid];
        if (interval_begin <= r && r < interval_end) {
            finish = true;
        } else if (interval_begin > r) {
            high = mid - 1;
        } else if (interval_end <= r) {
            low = mid + 1;
        }
    }
    return mid;
}

}   // namespace graph
}   // namespace nebula
