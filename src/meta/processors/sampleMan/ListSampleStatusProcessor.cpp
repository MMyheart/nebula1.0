/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/sampleMan/ListSampleStatusProcessor.h"

namespace nebula {
namespace meta {

void ListSampleStatusProcessor::process(const cpp2::ListSampleStatusReq &req) {
    GraphSpaceID spaceID = req.get_space_id();
    if (req.get_remove()) {
        remove(spaceID);
        return;
    }
    std::string statusPrefix = MetaServiceUtils::rebuildSampleStatus(spaceID);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, statusPrefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "List Rebuild Sample Status Failed: SpaceID " << spaceID;
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    std::vector<cpp2::SampleStatus> vec;
    while (iter->valid()) {
        auto key = iter->key().str();
        auto offset = statusPrefix.size();
        cpp2::SampleStatus sampleStatus;
        sampleStatus.set_status(iter->val().str());
        auto type = key.substr(offset, 1);
        std::string id = key.substr(offset + 1, key.size() - offset - 1);
        if (type == "T") {
            sampleStatus.set_tagId(*reinterpret_cast<const TagID *>(id.data()));
        } else {
            sampleStatus.set_edgeType(*reinterpret_cast<const EdgeType *>(id.data()));
        }
        vec.emplace_back(sampleStatus);
        iter->next();
    }

    std::string weightsPrefix = MetaServiceUtils::rebuildSampleWeights(spaceID);
    std::unique_ptr<kvstore::KVIterator> weightsIter;
    auto ret2 = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, weightsPrefix, &weightsIter);
    if (ret2 != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "List Rebuild Sample Weights Failed: SpaceID " << spaceID;
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    std::unordered_map<TagID, std::vector<cpp2::SampleWeights>> tagIdHostWeights;
    std::unordered_map<EdgeType, std::vector<cpp2::SampleWeights>> edgeTypeHostWeights;
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
            return;
        }
        std::vector<cpp2::SampleWeights> sampleWeights;
        sampleWeights.reserve(values.size() / 3);
        for (auto it = values.begin(); it != values.end(); it++) {
            std::string host = *it;
            std::string port = *(++it);
            std::string hostStr;
            hostStr.append(host).append(",").append(port);
            double weight = strtod((++it)->data(), nullptr);
            cpp2::SampleWeights sampleWeight;
            sampleWeight.set_hostStr(hostStr);
            sampleWeight.set_weight(weight);
            sampleWeights.emplace_back(sampleWeight);
        }
        if (type == "T") {
            TagID tagId = *reinterpret_cast<const TagID *>(id.data());
            tagIdHostWeights[tagId] = std::move(sampleWeights);
        } else {
            EdgeType edgeType = *reinterpret_cast<const EdgeType *>(id.data());
            edgeTypeHostWeights[edgeType] = std::move(sampleWeights);
        }
        weightsIter->next();
    }
    resp_.set_statuses(std::move(vec));
    resp_.set_tagIdWeights(std::move(tagIdHostWeights));
    resp_.set_edgeTypeWeights(std::move(edgeTypeHostWeights));
    onFinished();
}

void ListSampleStatusProcessor::remove(GraphSpaceID spaceId) {
    std::vector<std::string> keysRemove;
    std::string statusPrefix = MetaServiceUtils::rebuildSampleStatus(spaceId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, statusPrefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "List Rebuild Sample Status Failed: SpaceID " << spaceId;
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    while (iter->valid()) {
        keysRemove.emplace_back(iter->key());
        LOG(INFO) << "List Rebuild Sample Remove key=" << iter->key().str()
                  << ", value=" << iter->val().str();
        iter->next();
    }

    std::string weightsPrefix = MetaServiceUtils::rebuildSampleWeights(spaceId);
    std::unique_ptr<kvstore::KVIterator> weightsIter;
    auto ret2 = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, weightsPrefix, &weightsIter);
    if (ret2 != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "List Rebuild Sample Weights Failed: SpaceID " << spaceId;
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    while (weightsIter->valid()) {
        keysRemove.emplace_back(weightsIter->key());
        LOG(INFO) << "List Rebuild Sample Remove key=" << weightsIter->key().str()
                  << ", value=" << weightsIter->val().str();
        weightsIter->next();
    }
    folly::Baton<true, std::atomic> baton;
    auto retCode = kvstore::ResultCode::SUCCEEDED;
    kvstore_->asyncMultiRemove(kDefaultSpaceId,
                               kDefaultPartId,
                               std::move(keysRemove),
                               [&retCode, &baton](kvstore::ResultCode code) {
                                   if (code != kvstore::ResultCode::SUCCEEDED) {
                                       retCode = code;
                                       LOG(ERROR) << "Async remove sample keys failed: " << code;
                                   }
                                   baton.post();
                               });
    baton.wait();
    if (retCode != kvstore::ResultCode::SUCCEEDED) {
        this->handleErrorCode(MetaCommon::to(retCode));
    }
    this->onFinished();
    return;
}

}   // namespace meta
}   // namespace nebula
