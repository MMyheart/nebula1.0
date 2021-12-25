/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/RandomWalkExecutor.h"
#include <boost/functional/hash.hpp>
#include "base/Base.h"
#include "dataman/ResultSchemaProvider.h"
#include "dataman/RowReader.h"
#include "dataman/RowSetReader.h"
#include "graph/SchemaHelper.h"

DEFINE_bool(trace_random_walk, false, "Whether to dump the detail trace log from one go request");

namespace nebula {
namespace graph {

using SchemaProps = std::unordered_map<std::string, std::vector<std::string>>;
using nebula::cpp2::SupportedType;

RandomWalkExecutor::RandomWalkExecutor(Sentence *sentence, ExecutionContext *ectx)
    : TraverseExecutor(ectx, "randomWalk") {
    // The RTTI is guaranteed by Sentence::Kind,
    // so we use `static_cast' instead of `dynamic_cast' for the sake of efficiency.
    sentence_ = static_cast<RandomWalkSentence *>(sentence);
}

Status RandomWalkExecutor::prepare() {
    return Status::OK();
}

Status RandomWalkExecutor::prepareClauses() {
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

        // before prepareWhere
        status = prepareStrategy();
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

void RandomWalkExecutor::execute() {
    auto status = prepareClauses();
    if (!status.ok()) {
        doError(std::move(status));
        return;
    }

    if (sentence_->walkLen() == 0) {
        onEmptyInputs();
        return;
    }

    if (starts_.empty()) {
        onEmptyInputs();
        return;
    }

    randomWalk();
}

Status RandomWalkExecutor::prepareFrom() {
    Status status = Status::OK();
    auto *clause = sentence_->fromClause();
    do {
        if (clause == nullptr) {
            LOG(ERROR) << "From clause shall never be null";
            return Status::Error("From clause shall never be null");
        }
        std::unordered_set<VertexID> idSet;
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
            idSet.emplace(Expression::asInt(v));
        }
        auto startVec = std::vector<VertexID>(idSet.begin(), idSet.end());
        starts_ = std::move(startVec);

        if (!status.ok()) {
            break;
        }
    } while (false);
    return status;
}

Status RandomWalkExecutor::prepareOver() {
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

Status RandomWalkExecutor::prepareWhere() {
    if (sentence_->whereClause() == nullptr) {
        return Status::OK();
    }
    auto status = traversalExpr(sentence_->whereClause()->filter());
    return status;
}

Status RandomWalkExecutor::traversalExpr(const Expression *expr) {
    switch (expr->kind()) {
        case nebula::Expression::kLogical: {
            auto *lExpr = dynamic_cast<const LogicalExpression *>(expr);
            if (lExpr->op() == LogicalExpression::Operator::XOR ||
                lExpr->op() == LogicalExpression::Operator::OR) {
                return Status::SyntaxError("OR and XOR are not supported "
                                           "in lookup where clause ：%s",
                                           lExpr->toString().c_str());
            }
            auto *left = lExpr->left();
            Status ret = traversalExpr(left);
            if (!ret.ok()) {
                return ret;
            }
            auto *right = lExpr->right();
            ret = traversalExpr(right);
            if (!ret.ok()) {
                return ret;
            }
            break;
        }
        case nebula::Expression::kRelational: {
            std::string prop;
            auto *rExpr = dynamic_cast<const RelationalExpression *>(expr);
            auto *left = rExpr->left();
            auto *right = rExpr->right();
            if (left->kind() != nebula::Expression::kPrimary &&
                right->kind() != nebula::Expression::kPrimary) {
                return Status::SyntaxError("left and right both property");
            }
            Getters getters;
            auto *lE = dynamic_cast<const PrimaryExpression *>(left);
            auto *rE = dynamic_cast<const PrimaryExpression *>(right);
            auto lvalue = lE->eval(getters);
            auto rvalue = rE->eval(getters);
            auto lv = std::move(lvalue.value());
            auto rv = std::move(rvalue.value());

            if (lv.which() != VAR_STR || (rv.which() != VAR_DOUBLE && rv.which() != VAR_INT64)) {
                return Status::SyntaxError("VariantType is double.");
            }

            auto lstr = boost::get<std::string>(lv);
            if (lstr == "default") {
                defaultID_ = boost::get<int64_t>(rv);
            } else if (lstr == "p" || lstr == "q") {
                double rdouble;
                if (rv.which() == VAR_INT64) {
                    rdouble = static_cast<double>(boost::get<int64_t>(rv));
                } else {
                    rdouble = boost::get<double>(rv);
                }
                if (lstr == "p") {
                    p_ = rdouble;
                } else {
                    q_ = rdouble;
                }
            } else {
                return Status::SyntaxError("left and right property is p or q");
            }
            break;
        }
        default: {
            return Status::SyntaxError("Syntax error ： %s", expr->toString().c_str());
        }
    }
    return Status::OK();
}

Status RandomWalkExecutor::prepareStrategy() {
    const double kEps = 1.0e-6;
    if (std::fabs(p_ - 1.0) <= kEps && std::fabs(q_ - 1.0) <= kEps) {
        walkStrategy_ = traditional;
    } else {
        walkStrategy_ = node2vec;
    }
    LOG(INFO) << "prepareStrategy p=" << p_ << " q=" << q_ << " defaultID=" << defaultID_;
    return Status::OK();
}

Status RandomWalkExecutor::prepareYield() {
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

void RandomWalkExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    if (resp_ == nullptr) {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
    }
    resp = std::move(*resp_);
}

void RandomWalkExecutor::randomWalk() {
    auto spaceId = ectx()->rctx()->session()->space();
    auto status = getPropNames();
    if (!status.ok()) {
        doError(std::move(status).status());
        return;
    }
    auto returns = status.value();
    VLOG(1) << "edge type size: " << edgeTypes_.size() << " return cols: " << returns.size();
    auto future = ectx()->getStorageClient()->getNeighbors(
        spaceId, starts_, edgeTypes_, "", std::move(returns));
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
        if (FLAGS_trace_random_walk) {
            LOG(INFO) << "RandomWalk finished, total request vertices " << starts_.size();
            auto &hostLatency = result.hostLatency();
            for (size_t i = 0; i < hostLatency.size(); i++) {
                LOG(INFO) << std::get<0>(hostLatency[i]) << ", time cost "
                          << std::get<1>(hostLatency[i]) << "us / " << std::get<2>(hostLatency[i])
                          << "us, total results " << result.responses()[i].get_vertices()->size();
            }
        }
        randomWalkResponse(std::move(result));
    };
    auto error = [this](auto &&e) {
        LOG(ERROR) << "RandomWalk Exception when handle out-bounds/in-bounds: " << e.what();
        doError(Status::Error("Exception when handle out-bounds/in-bounds: %s.", e.what().c_str()));
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}

void RandomWalkExecutor::randomWalkResponse(RpcResponse &&rpcResp) {
    const auto &all = rpcResp.responses();
    std::unordered_map<VertexID, NeiborInfo> neiborInfoMap;
    for (const auto &resp : all) {
        if (resp.get_vertices() == nullptr) {
            continue;
        }
        VLOG(1) << "Total resp.vertices size " << resp.vertices.size();
        for (const auto &vdata : resp.vertices) {
            VLOG(1) << "Total vdata.edge_data size " << vdata.edge_data.size();
            VertexID srcId = vdata.get_vertex_id();
            auto func = [&]() mutable {
                NeiborInfo neiborInfo_;
                for (const auto &edata : vdata.edge_data) {
                    EdgeType edgeType = edata.type;
                    auto *eschema = resp.get_edge_schema();
                    std::unordered_map<EdgeType, std::shared_ptr<ResultSchemaProvider>> edgeSchema;
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
                        RowReader reader = RowReader::getEmptyRowReader();
                        if (currEdgeSchema) {
                            reader = RowReader::getRowReader(edge.props, currEdgeSchema);
                        }
                        double v;
                        for (auto *column : yields_) {
                            auto *expr = column->expr();
                            if (expr->isAliasExpression()) {
                                auto *e0 = static_cast<const AliasPropertyExpression *>(expr);
                                EdgeType type;
                                auto found = expCtx_->getEdgeType((*(e0->alias())), type);
                                if (found && std::abs(edgeType) == type) {
                                    if (*(e0->prop()) == _WEIGHT) {
                                        auto res = RowReader::getPropByName(reader.get(), _WEIGHT);
                                        v = boost::get<double>(std::move(res).right());
                                        break;
                                    }
                                }
                            }
                        }
                        neiborInfo_[edgeType].emplace_back(
                            std::pair<VertexID, double>(edge.get_dst(), v));
                    }
                }
                if (neiborInfo_.size() > 0) {
                    neiborInfoMap.insert(
                        std::pair<VertexID, NeiborInfo>(srcId, std::move(neiborInfo_)));
                }
                return true;
            };

            if (!func()) {
                return;
            }
        }   // for vdata
    }       // for `resp'

    if (walkStrategy_ == traditional) {
        sampleNeighbor(std::move(neiborInfoMap));
    } else {
        node2vecWalk(std::move(neiborInfoMap));
    }

    if (curStep_ == sentence_->walkLen() || starts_.empty()) {
        generateResult();
    } else {
        curStep_++;
        randomWalk();
    }
}

void RandomWalkExecutor::node2vecWalk(std::unordered_map<VertexID, NeiborInfo> &&neighborInfoMap) {
    std::unordered_set<VertexID> set;
    std::unordered_set<std::pair<VertexID, VertexID>> curBackTrace;

    for (auto &srcId : starts_) {
        auto nIter = neighborInfoMap.find(srcId);
        if (nIter != neighborInfoMap.end()) {
            auto neighborInfo_ = nIter->second;
            std::vector<VertexID> ids;
            std::vector<double> weights;
            for (auto &info : neighborInfo_) {
                std::vector<std::pair<VertexID, double>> idWeightList = info.second;
                for (uint64_t i = 0; i < idWeightList.size(); i++) {
                    std::pair<VertexID, double> idWeightPair = idWeightList[i];
                    ids.emplace_back(idWeightPair.first);
                    weights.emplace_back(idWeightPair.second);
                }
            }

            if (curStep_ == 1) {
                std::vector<VertexID> pNeighbors;
                buildWeights(srcId, std::move(pNeighbors), ids, weights);
                std::vector<double> sum_weights;
                sum_weights.resize(weights.size());
                double sum_weight_ = 0.0;
                for (uint64_t j = 0; j < weights.size(); j++) {
                    sum_weight_ += weights[j];
                    sum_weights[j] = sum_weight_;
                }
                auto mid = randomSelect(sum_weights, 0, sum_weights.size() - 1);
                auto dst = ids[mid];

                walkPath_[srcId].emplace_back(dst);
                auto key = std::pair<uint32_t, VertexID>(curStep_, dst);
                backTracker_.emplace(key, srcId);

                if (curStep_ != sentence_->walkLen()) {
                    parent_neighbors_[srcId] =
                        std::pair<VertexID, std::vector<VertexID>>(srcId, std::move(ids));
                    set.emplace(dst);
                }

            } else {
                auto preRange =
                    backTracker_.equal_range(std::pair<uint32_t, VertexID>(curStep_ - 1, srcId));
                for (auto trace = preRange.first; trace != preRange.second; ++trace) {
                    auto root = trace->second;
                    auto pnIter = parent_neighbors_.find(root);
                    if (pnIter != parent_neighbors_.end()) {
                        CurStepPNeighbors curStepPNeighbors = pnIter->second;
                        buildWeights(curStepPNeighbors.first,
                                     std::move(curStepPNeighbors.second),
                                     ids,
                                     weights);
                        std::vector<double> sum_weights;
                        sum_weights.resize(weights.size());
                        double sum_weight_ = 0.0;
                        for (uint64_t j = 0; j < weights.size(); j++) {
                            sum_weight_ += weights[j];
                            sum_weights[j] = sum_weight_;
                        }
                        auto mid = randomSelect(sum_weights, 0, sum_weights.size() - 1);
                        auto dst = ids[mid];

                        walkPath_[root].emplace_back(dst);
                        if (curStep_ != sentence_->walkLen()) {
                            parent_neighbors_[root] =
                                std::pair<VertexID, std::vector<VertexID>>(srcId, ids);
                            set.emplace(dst);
                            auto key = std::pair<VertexID, VertexID>(dst, root);
                            curBackTrace.emplace(key);
                        }
                    }
                }
            }
        } else {
            VertexID dst = -1;
            if (curStep_ == 1) {
                walkPath_[srcId].emplace_back(dst);
            } else {
                auto preRange =
                    backTracker_.equal_range(std::pair<uint32_t, VertexID>(curStep_ - 1, srcId));
                for (auto trace = preRange.first; trace != preRange.second; ++trace) {
                    auto root = trace->second;
                    walkPath_[root].emplace_back(dst);
                }
            }
        }
    }

    if (curBackTrace.size() > 0) {
        for (auto &iter : curBackTrace) {
            backTracker_.emplace(std::pair<uint32_t, VertexID>(curStep_, iter.first), iter.second);
        }
    }
    auto startVec = std::vector<VertexID>(set.begin(), set.end());
    starts_ = std::move(startVec);
}

void RandomWalkExecutor::sampleNeighbor(std::unordered_map<VertexID, NeiborInfo> &&neiborInfoMap) {
    std::unordered_set<VertexID> set;
    std::unordered_map<VertexID, VertexID> mapping;
    if (neiborInfoMap.size() > 0) {
        for (auto &info : neiborInfoMap) {
            auto srcId = info.first;
            auto neiborInfo_ = info.second;

            std::vector<EdgeType> sampleEdges;
            std::vector<double> sampleEdgeSumWeights;
            sampleEdges.resize(edgeTypes_.size());
            sampleEdgeSumWeights.resize(edgeTypes_.size());

            std::unordered_map<EdgeType, std::pair<std::vector<VertexID>, std::vector<double>>>
                sampleVexInfoMap;

            double edgeSumWeight = 0.0;
            for (auto &elem : edgeTypes_) {
                auto iter = neiborInfo_.find(elem);
                if (iter != neiborInfo_.end()) {
                    EdgeType edgeType = iter->first;
                    std::vector<std::pair<VertexID, double>> idWeightList = iter->second;

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
                    sampleEdges.push_back(edgeType);
                    sampleEdgeSumWeights.push_back(edgeSumWeight);
                    sampleVexInfoMap[edgeType] =
                        std::pair<std::vector<VertexID>, std::vector<double>>(
                            std::move(ids), std::move(sum_weights));
                }
            }

            // sample edge
            EdgeType sampleEdgeType = 0;
            if (sampleEdges.size() == 1) {
                sampleEdgeType = sampleEdges[0];
            } else {
                auto mid = randomSelect(sampleEdgeSumWeights, 0, sampleEdgeSumWeights.size() - 1);
                sampleEdgeType = sampleEdges[mid];
            }

            // sample vertex
            auto nodesIter = sampleVexInfoMap.find(sampleEdgeType);
            if (nodesIter != sampleVexInfoMap.end()) {
                auto idWeigthPair = nodesIter->second;
                auto mid = randomSelect(idWeigthPair.second, 0, idWeigthPair.second.size() - 1);
                auto dst = idWeigthPair.first[mid];
                mapping[srcId] = dst;
                if (curStep_ != sentence_->walkLen()) {
                    set.emplace(dst);
                }
            }
        }
    }

    std::unordered_set<std::pair<VertexID, VertexID>> curBackTrace;
    for (auto &src : starts_) {
        auto found = mapping.find(src);
        VertexID dst = -1;
        if (found != mapping.end()) {
            dst = found->second;
        } else if (curStep_ != sentence_->walkLen()) {
            set.emplace(defaultID_);
        }
        if (curStep_ == 1) {
            walkPath_[src].emplace_back(dst);
            auto key = std::pair<uint32_t, VertexID>(curStep_, (dst == -1) ? defaultID_ : dst);
            backTracker_.emplace(key, src);
        } else {
            auto preRange =
                backTracker_.equal_range(std::pair<uint32_t, VertexID>(curStep_ - 1, src));
            for (auto trace = preRange.first; trace != preRange.second; ++trace) {
                auto root = trace->second;
                walkPath_[root].emplace_back(dst);
                // record backTracker
                if (curStep_ != sentence_->walkLen()) {
                    auto key = std::pair<VertexID, VertexID>((dst == -1) ? defaultID_ : dst, root);
                    curBackTrace.emplace(key);
                }
            }
        }
    }

    if (curBackTrace.size() > 0) {
        for (auto &iter : curBackTrace) {
            backTracker_.emplace(std::pair<uint32_t, VertexID>(curStep_, iter.first), iter.second);
        }
    }
    auto startVec = std::vector<VertexID>(set.begin(), set.end());
    starts_ = std::move(startVec);
}

void RandomWalkExecutor::generateResult() {
    std::vector<cpp2::RowValue> rows;
    resp_ = std::make_unique<cpp2::ExecutionResponse>();
    resp_->set_column_names(resultColNames_);
    for (auto &iter : walkPath_) {
        auto path = iter.second;
        std::ostringstream oss;
        oss << iter.first;
        for (auto &id : path) {
            oss << "#" << id;
        }
        if (path.size() < sentence_->walkLen()) {
            for (uint64_t i = path.size(); i < sentence_->walkLen(); i++) {
                oss << "#" << nonExistID_;
            }
        }
        std::vector<cpp2::ColumnValue> row;
        row.resize(1);
        row[0].set_str(oss.str());
        rows.emplace_back();
        rows.back().set_columns(std::move(row));
    }

    if (!rows.empty()) {
        resp_->set_rows(rows);
    }
    doFinish(Executor::ProcessControl::kNext);
}

StatusOr<std::vector<storage::cpp2::PropDef>> RandomWalkExecutor::getPropNames() {
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

void RandomWalkExecutor::onEmptyInputs() {
    auto outputs = std::make_unique<InterimResult>(std::move(resultColNames_));
    if (onResult_) {
        onResult_(std::move(outputs));
    } else if (resp_ == nullptr) {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
    }
    doFinish(Executor::ProcessControl::kNext);
}

size_t RandomWalkExecutor::randomSelect(std::vector<double> &sum_weights,
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

void RandomWalkExecutor::buildWeights(VertexID parent_id,
                                      std::vector<VertexID> &&parent_neighbors,
                                      std::vector<VertexID> &neighbors,
                                      std::vector<double> &weights) {
    size_t j = 0;
    size_t k = 0;
    while (j < neighbors.size() && k < parent_neighbors.size()) {
        if (neighbors[j] < parent_neighbors[k]) {
            if (neighbors[j] != parent_id) {
                weights[j] /= q_;   // d_tx = 2
            } else {
                weights[j] /= p_;   // d_tx = 0
            }
            ++j;
        } else if (neighbors[j] == parent_neighbors[k]) {
            ++k;
            ++j;
        } else {
            ++k;
        }
    }
    while (j < neighbors.size()) {
        if (neighbors[j] != parent_id) {
            weights[j] /= q_;   // d_tx = 2
        } else {
            weights[j] /= p_;   // d_tx = 0
        }
        ++j;
    }
}

}   // namespace graph
}   // namespace nebula
