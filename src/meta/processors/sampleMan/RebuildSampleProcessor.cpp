/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "meta/processors/sampleMan/RebuildSampleProcessor.h"
#include <iomanip>

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace meta {

void RebuildSampleProcessor::process(const cpp2::RebuildSampleReq& req) {
    auto space = req.get_space_id();
    CHECK_SPACE_ID_AND_RETURN(space);
    auto tagIds = req.get_tagIds();
    auto edgeTypes = req.get_edgeTypes();
    bool force = req.get_force();

    if (!force && !checkIfNeedToRebuild(space, tagIds, edgeTypes)) {
        onFinished();
        return;
    }

    if (!saveRebuildStatus(kvstore_, resp_, space, tagIds, edgeTypes, "RUNNING")) {
        onFinished();
        return;
    }

    const auto& hostPrefix = MetaServiceUtils::leaderPrefix();
    std::unique_ptr<kvstore::KVIterator> leaderIter;
    auto leaderRet = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, hostPrefix, &leaderIter);
    if (leaderRet != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Get space " << space << "'s part failed";
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    std::vector<folly::Future<storage::cpp2::RebuildSampleResponse>> results;
    auto activeHosts = ActiveHostsMan::getActiveHosts(kvstore_, FLAGS_heartbeat_interval_secs + 1);
    while (leaderIter->valid()) {
        auto host = MetaServiceUtils::parseLeaderKey(leaderIter->key());
        auto ip = NetworkUtils::intToIPv4(host.get_ip());
        auto port = host.get_port();
        auto hostAddrRet = NetworkUtils::toHostAddr(ip, port);
        if (!hostAddrRet.ok()) {
            LOG(ERROR) << "Can't cast to host " << ip + ":" << port;
            resp_.set_code(cpp2::ErrorCode::E_STORE_FAILURE);
            onFinished();
            return;
        }

        auto hostAddr = hostAddrRet.value();
        if (std::find(activeHosts.begin(), activeHosts.end(), HostAddr(host.ip, host.port)) !=
            activeHosts.end()) {
            auto leaderParts = MetaServiceUtils::parseLeaderVal(leaderIter->val());
            auto& partIds = leaderParts[space];
            std::vector<TagID> tags(tagIds.begin(), tagIds.end());
            std::vector<EdgeType> edges(edgeTypes.begin(), edgeTypes.end());
            auto future = adminClient_->rebuildSample(
                hostAddr, space, std::move(partIds), std::move(tags), std::move(edges));
            results.emplace_back(std::move(future));
        }
        leaderIter->next();
    }
    handleRebuildSampleResult(std::move(results), kvstore_, resp_, space, tagIds, edgeTypes);
    onFinished();
}

bool RebuildSampleProcessor::checkIfNeedToRebuild(GraphSpaceID space,
                                                  std::vector<TagID> tagIds,
                                                  std::vector<EdgeType> edgeTypes) {
    for (auto tagId : tagIds) {
        auto prefix = MetaServiceUtils::rebuildSampleTagStatus(space, tagId);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            LOG(ERROR) << "List Tag Sample Status Failed: SpaceID " << space << ", TagId " << tagId;
            resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
            return false;
        }
        if (iter->valid() && (iter->val().str() == "SUCCEEDED" || iter->val().str() == "RUNNING")) {
            LOG(INFO) << "There is not need to rebuild sample tag for tagID " << tagId;
            resp_.set_code(cpp2::ErrorCode::E_EXISTED);
            return false;
        }
    }
    for (auto edgeType : edgeTypes) {
        auto prefix = MetaServiceUtils::rebuildSampleEdgeStatus(space, edgeType);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            LOG(ERROR) << "List Edge Sample Status Failed: SpaceID " << space << ", EdgeType "
                       << edgeType;
            resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
            return false;
        }
        if (iter->valid() && (iter->val().str() == "SUCCEEDED" || iter->val().str() == "RUNNING")) {
            LOG(INFO) << "There is not need to rebuild sample edge for edgeType " << edgeType;
            resp_.set_code(cpp2::ErrorCode::E_EXISTED);
            return false;
        }
    }
    return true;
}

bool RebuildSampleProcessor::saveRebuildStatus(kvstore::KVStore* kvStore,
                                               cpp2::ExecResp& resp,
                                               GraphSpaceID space,
                                               std::vector<TagID> tagIds,
                                               std::vector<EdgeType> edgeTypes,
                                               std::string status) {
    for (auto tagId : tagIds) {
        std::string str = status;
        std::string statusKey = MetaServiceUtils::rebuildSampleTagStatus(space, tagId);
        if (!MetaCommon::saveRebuildStatus(kvStore, statusKey, const_cast<std::string&&>(str))) {
            LOG(ERROR) << "Save rebuild sample status failed";
            resp.set_code(cpp2::ErrorCode::E_STORE_FAILURE);
            LastUpdateTimeMan::update(kvStore, time::WallClock::fastNowInMilliSec());
            return false;
        }
    }
    for (auto edgeType : edgeTypes) {
        std::string str = status;
        std::string statusKey = MetaServiceUtils::rebuildSampleEdgeStatus(space, edgeType);
        if (!MetaCommon::saveRebuildStatus(kvStore, statusKey, const_cast<std::string&&>(str))) {
            LOG(ERROR) << "Save rebuild sample status failed";
            resp.set_code(cpp2::ErrorCode::E_STORE_FAILURE);
            LastUpdateTimeMan::update(kvStore, time::WallClock::fastNowInMilliSec());
            return false;
        }
    }
    LastUpdateTimeMan::update(kvStore, time::WallClock::fastNowInMilliSec());
    LOG(INFO) << "saveRebuildStatus space " << space << " , status " << status;
    return true;
}

bool RebuildSampleProcessor::saveRebuildWeights(kvstore::KVStore* kvStore,
                                                GraphSpaceID space,
                                                TagIdHostWeights tagIdWeights,
                                                EdgeTypeHostWeights edgeTypeWeights) {
    for (auto it = tagIdWeights.begin(); it != tagIdWeights.end(); it++) {
        std::string weightsKey = MetaServiceUtils::rebuildSampleTagWeights(space, it->first);
        std::string str;
        for (auto weight : it->second) {
            str.append(weight.first).append(",");

            std::ostringstream out;
            out.precision(std::numeric_limits<double>::digits10);
            out << weight.second;

            std::string res = std::move(out.str());
            auto pos = res.find('.');
            if (pos == std::string::npos) {
                str.append(res).append(",");
            } else {
                auto splitLen = pos + 21;
                if (res.size() <= splitLen) {
                    str.append(res).append(",");
                } else {
                    str.append(res.substr(0, splitLen)).append(",");
                }
            }
        }
        std::string value = str.substr(0, str.length() - 1);
        LOG(INFO) << "rebuild sample weights for tagId " << it->first << ", weights " << value;
        if (!MetaCommon::saveRebuildStatus(kvStore, weightsKey, value.data())) {
            LOG(ERROR) << "Save rebuild sample tag weights failed, spaceId " << space << ", tagId "
                       << it->first << ", weight " << value;
            return false;
        }
    }
    for (auto it = edgeTypeWeights.begin(); it != edgeTypeWeights.end(); it++) {
        std::string weightsKey = MetaServiceUtils::rebuildSampleEdgeWeights(space, it->first);
        std::string str;
        for (auto weight : it->second) {
            str.append(weight.first).append(",");

            std::ostringstream out;
            out.precision(std::numeric_limits<double>::digits10);
            out << weight.second;

            std::string res = std::move(out.str());
            auto pos = res.find('.');
            if (pos == std::string::npos) {
                str.append(res).append(",");
            } else {
                auto splitLen = pos + 21;
                if (res.size() <= splitLen) {
                    str.append(res).append(",");
                } else {
                    str.append(res.substr(0, splitLen)).append(",");
                }
            }
        }
        std::string value = str.substr(0, str.length() - 1);
        LOG(INFO) << "rebuild sample weights for edgeType " << it->first << ", weights " << value;
        if (!MetaCommon::saveRebuildStatus(kvStore, weightsKey, value.data())) {
            LOG(ERROR) << "Save rebuild sample edge weights failed, spaceId " << space
                       << ", edgeType " << it->first << ", weight " << value;
            return false;
        }
    }
    return true;
}

bool RebuildSampleProcessor::getRebuildWeights(kvstore::KVStore* kvStore,
                                               GraphSpaceID space,
                                               TagIdHostWeights& tagIdWeights,
                                               EdgeTypeHostWeights& edgeTypeWeights) {
    std::string weightsPrefix = MetaServiceUtils::rebuildSampleWeights(space);
    std::unique_ptr<kvstore::KVIterator> weightsIter;
    auto ret2 = kvStore->prefix(kDefaultSpaceId, kDefaultPartId, weightsPrefix, &weightsIter);
    if (ret2 != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Get Rebuild Sample Weights Failed: SpaceID " << space;
        return false;
    }
    while (weightsIter->valid()) {
        auto key = weightsIter->key().str();
        auto offset = weightsPrefix.size();
        auto type = key.substr(offset, 1);
        auto id = key.substr(offset + 1, key.size() - offset - 1);
        std::string value = weightsIter->val().str();
        std::vector<std::string> values;
        std::string::size_type pos1, pos2;
        std::string c(",");
        pos2 = value.find(c);
        pos1 = 0;
        while (std::string::npos != pos2) {
            values.push_back(value.substr(pos1, pos2 - pos1));
            pos1 = pos2 + c.size();
            pos2 = value.find(c, pos1);
        }
        if (pos1 != value.length()) {
            values.push_back(value.substr(pos1));
        }
        if (values.size() % 3 != 0) {
            LOG(ERROR) << "list sample status split value size = " << values.size()
                       << ", key=" << key << ", value=" << value;
            return false;
        }
        std::unordered_map<std::string, double> hostWeights;
        for (auto it = values.begin(); it != values.end(); it++) {
            std::string host = *it;
            std::string port = *(++it);
            std::string hostStr;
            hostStr.append(host).append(",").append(port);
            double weight = strtod((++it)->data(), nullptr);
            hostWeights[hostStr] = weight;
        }
        if (type == "T") {
            TagID tagId = *reinterpret_cast<const TagID*>(id.data());
            tagIdWeights[tagId] = std::move(hostWeights);
        } else {
            EdgeType edgeType = *reinterpret_cast<const EdgeType*>(id.data());
            edgeTypeWeights[edgeType] = std::move(hostWeights);
        }
        weightsIter->next();
    }
    return true;
}

void RebuildSampleProcessor::handleRebuildSampleResult(
    std::vector<folly::Future<storage::cpp2::RebuildSampleResponse>> results,
    kvstore::KVStore* kvstore,
    cpp2::ExecResp& resp,
    GraphSpaceID space,
    std::vector<TagID> tagIds,
    std::vector<EdgeType> edgeTypes) {
    folly::collectAll(std::move(results))
        .thenValue([this, kvstore, resp, space, tagIds, edgeTypes](
                       const std::vector<folly::Try<storage::cpp2::RebuildSampleResponse>>&
                           tries) mutable {
            for (const auto& t : tries) {
                if (!t.value().get_result().get_failed_codes().empty()) {
                    LOG(ERROR) << "Rebuild Sample Failed";
                    saveRebuildStatus(kvstore, resp, space, tagIds, edgeTypes, "FAILED");
                    return;
                }
            }

            TagIdHostWeights tagIdHostWeights;
            EdgeTypeHostWeights edgeTypeHostWeights;
            if (!getRebuildWeights(kvstore, space, tagIdHostWeights, edgeTypeHostWeights)) {
                saveRebuildStatus(kvstore, resp, space, tagIds, edgeTypes, "FAILED");
                return;
            }
            std::vector<std::string> hostVec;
            hostVec.reserve(tries.size());
            for (const auto& t : tries) {
                if (!t.value().__isset.weights) {
                    LOG(ERROR) << "Rebuild sample weights is empty";
                    saveRebuildStatus(kvstore, resp, space, tagIds, edgeTypes, "FAILED");
                    return;
                }
                auto weights = t.value().get_weights();
                auto host = t.value().get_host();
                std::string hostStr;
                hostStr.append(std::to_string(host->get_ip()))
                    .append(",")
                    .append(std::to_string(host->get_port()));
                hostVec.emplace_back(hostStr);
                for (auto it = weights->begin(); it != weights->end(); it++) {
                    if (it->__isset.tagId) {
                        tagIdHostWeights[*it->get_tagId()][hostStr] = it->get_weights();
                    } else {
                        edgeTypeHostWeights[*it->get_edgeType()][hostStr] = it->get_weights();
                    }
                }
            }
            for (auto host : hostVec) {
                tagIdHostWeights[0][host] = 0;
                edgeTypeHostWeights[0][host] = 0;
                double hostWeights = 0;
                for (auto it : tagIdHostWeights) {
                    hostWeights += it.second[host];
                }
                tagIdHostWeights[0][host] = hostWeights;
                hostWeights = 0;
                for (auto it : edgeTypeHostWeights) {
                    hostWeights += it.second[host];
                }
                edgeTypeHostWeights[0][host] = hostWeights;
            }
            std::unordered_map<TagID, double> tagWeights;
            std::unordered_map<EdgeType, double> edgeWeights;
            for (auto tag : tagIdHostWeights) {
                tag.second["0,0"] = 0;
                double summaryWeights = 0;
                for (auto host : tag.second) {
                    summaryWeights += host.second;
                }
                tagWeights[tag.first] = summaryWeights;
            }
            for (auto edge : edgeTypeHostWeights) {
                edge.second["0,0"] = 0;
                double summaryWeights = 0;
                for (auto host : edge.second) {
                    summaryWeights += host.second;
                }
                edgeWeights[edge.first] = summaryWeights;
            }
            for (auto it : tagWeights) {
                tagIdHostWeights[it.first]["0,0"] = it.second;
            }
            for (auto it : edgeWeights) {
                edgeTypeHostWeights[it.first]["0,0"] = it.second;
            }

            if (!saveRebuildWeights(kvstore, space, tagIdHostWeights, edgeTypeHostWeights)) {
                LOG(ERROR) << "Save rebuild weights failed";
                saveRebuildStatus(kvstore, resp, space, tagIds, edgeTypes, "FAILED");
                return;
            }
            if (!saveRebuildStatus(kvstore, resp, space, tagIds, edgeTypes, "SUCCEEDED")) {
                LOG(ERROR) << "Save rebuild status failed";
                return;
            }
        })
        .thenError([this, kvstore, resp, space, tagIds, edgeTypes](auto&& e) mutable {
            LOG(ERROR) << "Exception caught: " << e.what();
            saveRebuildStatus(kvstore, resp, space, tagIds, edgeTypes, "FAILED");
        });
}

}   // namespace meta
}   // namespace nebula
