/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "RebuildSampleProcessor.h"
#include "utils/NebulaKeyUtils.h"

namespace nebula {
namespace storage {

std::string StatisticStoreUtils::getStatisticLargeKey(GraphSpaceID spaceID,
                                                      int32_t tagOrEdge,
                                                      int64_t sVid) {
    return getStatisticKey(spaceID, tagOrEdge, sVid, "L");
}
std::string StatisticStoreUtils::getStatisticSmallKey(GraphSpaceID spaceID,
                                                      int32_t tagOrEdge,
                                                      int64_t sVid) {
    return getStatisticKey(spaceID, tagOrEdge, sVid, "S");
}
std::string StatisticStoreUtils::getStatisticKey(GraphSpaceID spaceID,
                                                 int32_t tagOrEdge,
                                                 int64_t sVid,
                                                 const std::string& flag) {
    std::string key;
    key.append(flag)
        .append(std::to_string(spaceID))
        .append(std::to_string(tagOrEdge))
        .append(std::to_string(sVid));
    return key;
}

std::string StatisticStoreUtils::getStatisticValue(VertexStatistic statistic) {
    std::string separator_ = "$";
    std::string str;
    str.append(std::to_string(statistic.sVid_))
        .append(separator_)
        .append(doubleToString(statistic.prob_))
        .append(separator_)
        .append(std::to_string(statistic.vid_))
        .append(separator_)
        .append(doubleToString(statistic.weight_))
        .append(separator_)
        .append(std::to_string(statistic.aliasVid_))
        .append(separator_)
        .append(doubleToString(statistic.aliasWeight_))
        .append(separator_);
    return str;
}

std::string StatisticStoreUtils::getStatisticValue(EdgeStatistic statistic) {
    std::string separator_ = "$";
    std::string str;
    str.append(std::to_string(statistic.sVid_))
        .append(separator_)
        .append(doubleToString(statistic.prob_))
        .append(separator_)
        .append(std::to_string(statistic.srcVid_))
        .append(separator_)
        .append(std::to_string(statistic.dstVid_))
        .append(separator_)
        .append(doubleToString(statistic.weight_))
        .append(separator_)
        .append(std::to_string(statistic.aliasSrcVid_))
        .append(separator_)
        .append(std::to_string(statistic.aliasDstVid_))
        .append(separator_)
        .append(doubleToString(statistic.aliasWeight_))
        .append(separator_)
        .append(std::to_string(statistic.edgeType_))
        .append(separator_);
    return str;
}

void StatisticStoreUtils::parseStatisticValue(VertexStatistic* statistic, std::string& value) {
    std::string separator_ = "$";
    std::vector<std::string> vec;
    splitString(value, vec, separator_);

    statistic->sVid_ = strtoll(vec[0].data(), nullptr, 10);
    statistic->prob_ = strtod(vec[1].data(), nullptr);
    statistic->vid_ = strtoll(vec[2].data(), nullptr, 10);
    statistic->weight_ = strtod(vec[3].data(), nullptr);
    statistic->aliasVid_ = strtoll(vec[4].data(), nullptr, 10);
    statistic->aliasWeight_ = strtod(vec[5].data(), nullptr);
}

void StatisticStoreUtils::parseStatisticValue(EdgeStatistic* statistic, std::string& value) {
    std::string separator_ = "$";
    std::vector<std::string> vec;
    splitString(value, vec, separator_);

    statistic->sVid_ = strtoll(vec[0].data(), nullptr, 10);
    statistic->prob_ = strtod(vec[1].data(), nullptr);
    statistic->srcVid_ = strtoll(vec[2].data(), nullptr, 10);
    statistic->dstVid_ = strtoll(vec[3].data(), nullptr, 10);
    statistic->weight_ = strtod(vec[4].data(), nullptr);
    statistic->aliasSrcVid_ = strtoll(vec[5].data(), nullptr, 10);
    statistic->aliasDstVid_ = strtoll(vec[6].data(), nullptr, 10);
    statistic->aliasWeight_ = strtod(vec[7].data(), nullptr);
    statistic->edgeType_ = atoi(vec[8].data());
}

std::string StatisticStoreUtils::statisticLargePrefix(GraphSpaceID spaceID, int32_t tagOrEdge) {
    return statisticPrefix(spaceID, tagOrEdge, "L");
}

std::string StatisticStoreUtils::statisticSmallPrefix(GraphSpaceID spaceID, int32_t tagOrEdge) {
    return statisticPrefix(spaceID, tagOrEdge, "S");
}

std::string StatisticStoreUtils::statisticPrefix(GraphSpaceID spaceID,
                                                 int32_t tagOrEdge,
                                                 const std::string& flag) {
    std::string key;
    key.append(flag).append(std::to_string(spaceID)).append(std::to_string(tagOrEdge));
    return key;
}

std::string StatisticStoreUtils::getTagOrEdgeWeightsKey(GraphSpaceID spaceID, int32_t tagOrEdge) {
    std::string key;
    key.append("W").append(std::to_string(spaceID)).append(std::to_string(tagOrEdge));
    return key;
}

std::string StatisticStoreUtils::tagOrEdgeWeightsPrefix(GraphSpaceID spaceID) {
    std::string key;
    key.append("W").append(std::to_string(spaceID));
    return key;
}

std::string StatisticStoreUtils::getTagOrEdgeCountsKey(GraphSpaceID spaceID, int32_t tagOrEdge) {
    std::string key;
    key.append("C").append(std::to_string(spaceID)).append(std::to_string(tagOrEdge));
    return key;
}

std::string StatisticStoreUtils::tagOrEdgeCountsPrefix(GraphSpaceID spaceID) {
    std::string key;
    key.append("C").append(std::to_string(spaceID));
    return key;
}

std::string StatisticStoreUtils::doubleToString(const double value,
                                                unsigned int precisionAfterPoint) {
    std::ostringstream out;
    out.precision(std::numeric_limits<double>::digits10);
    out << value;

    std::string res = std::move(out.str());
    auto pos = res.find('.');
    if (pos == std::string::npos) {
        return res;
    }
    auto splitLen = pos + 1 + precisionAfterPoint;
    if (res.size() <= splitLen) {
        return res;
    }
    return res.substr(0, splitLen);
}

void StatisticStoreUtils::splitString(const std::string& s,
                                      std::vector<std::string>& v,
                                      const std::string& c) {
    std::string::size_type pos1, pos2;
    pos2 = s.find(c);
    pos1 = 0;
    while (std::string::npos != pos2) {
        v.push_back(s.substr(pos1, pos2 - pos1));
        pos1 = pos2 + c.size();
        pos2 = s.find(c, pos1);
    }
    if (pos1 != s.length()) {
        v.push_back(s.substr(pos1));
    }
}

void RebuildSampleProcessor::process(const cpp2::RebuildSampleRequest& req) {
    if (req.get_parts().empty()) {
        cpp2::ResultCode thriftRet;
        thriftRet.set_code(cpp2::ErrorCode::E_PART_NOT_FOUND);
        codes_.emplace_back(thriftRet);
        LOG(INFO) << "RebuildSampleProcessor part is null ";
        onFinished();
        return;
    }
    resp_.set_host(req.get_host());
    spaceId_ = req.get_space_id();

    statisticStore_->addStatistic(spaceId_);

    if (!statisticStore_->getTagOrEdgeCount(spaceId_, req.get_tagIds(), &oldTagIndex_)) {
        std::string prefix = StatisticStoreUtils::tagOrEdgeCountsPrefix(spaceId_);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = statisticStore_->prefix(spaceId_, prefix, &iter);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            cpp2::ResultCode thriftRet;
            thriftRet.set_code(cpp2::ErrorCode::E_UNKNOWN);
            codes_.emplace_back(thriftRet);
            LOG(ERROR) << "SampleVertexProcessor tagOrEdgeWeightsPrefix=" << prefix
                       << ", iterator error, code=" << ret;
            onFinished();
            return;
        }
        for (; iter->valid(); iter->next()) {
            std::string key = iter->key().str();
            std::string value = iter->val().str();
            auto offset = prefix.size();
            std::string tagId = key.substr(offset, key.size() - offset);
            oldTagIndex_[atoi(tagId.data())] = strtoll(value.data(), nullptr, 10);
        }
        statisticStore_->addTagOrEdgeCount(spaceId_, oldTagIndex_);
    }
    statisticStore_->getTagOrEdgeCount(spaceId_, req.get_edgeTypes(), &oldEdgeIndex_);

    for (auto tagId : req.get_tagIds()) {
        tagIndex_[tagId] = 0;
        tagWeightSummary_[tagId] = 0;
        tagStatistic_[tagId].reserve(batchSize_);
    }
    for (auto edgeType : req.get_edgeTypes()) {
        edgeIndex_[edgeType] = 0;
        edgeWeightSummary_[edgeType] = 0;
        edgeStatistic_[edgeType].reserve(batchSize_);
    }
    for (auto part : req.get_parts()) {
        std::unique_ptr<kvstore::KVIterator> iter;
        std::string prefix = NebulaKeyUtils::prefix(part);
        auto ret = doPrefix(spaceId_, part, prefix, &iter);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            pushResultCode(to(ret), part);
            onFinished();
            return;
        }
        for (; iter->valid(); iter->next()) {
            auto key = iter->key();
            if (NebulaKeyUtils::isVertex(key)) {
                parseTag(key, iter->val());
            }
            if (NebulaKeyUtils::isEdge(key)) {
                parseEdge(key, iter->val());
            }
        }
    }
    LOG(INFO) << "space parse finished";
    saveTagAndEdgeWeights();

    for (auto& it : tagIndex_) {
        LOG(INFO) << "parse result tagId " << it.first << ", "
                  << " tagWeightSummary " << tagWeightSummary_[it.first] << " tagIndex "
                  << tagIndex_[it.first] << " oldTagIndex " << oldTagIndex_[it.first];
        tagStandard_[it.first] = 1 / static_cast<double>(it.second);
        saveTagStatistic(it.first);
        std::vector<std::string> keys;
        keys.reserve(batchSize_);
        for (int64_t i = it.second; i < oldTagIndex_[it.first]; i++) {
            std::string key = StatisticStoreUtils::getStatisticKey(spaceId_, it.first, i);
            keys.emplace_back(key);
            if (keys.size() == batchSize_) {
                statisticStore_->multiRemove(spaceId_, keys);
                keys.clear();
            }
        }
        if (!keys.empty()) {
            statisticStore_->multiRemove(spaceId_, keys);
        }
        splitLargeAndSmallTag(it.first);
        aliasSampleTag(it.first);
        deleteSplitInfo(it.first, true);
    }

    for (auto& it : edgeIndex_) {
        LOG(INFO) << "parse result edgeType " << it.first << ", "
                  << " edgeWeightSummary " << edgeWeightSummary_[it.first] << " edgeIndex "
                  << edgeIndex_[it.first] << " oldEdgeIndex " << oldEdgeIndex_[it.first];
        edgeStandard_[it.first] = 1 / static_cast<double>(it.second);
        saveEdgeStatistic(it.first);
        std::vector<std::string> keys;
        keys.reserve(batchSize_);
        for (int64_t i = it.second; i < oldEdgeIndex_[it.first]; i++) {
            std::string key = StatisticStoreUtils::getStatisticKey(spaceId_, it.first, i);
            keys.emplace_back(key);
            if (keys.size() == batchSize_) {
                statisticStore_->multiRemove(spaceId_, keys);
                keys.clear();
            }
        }
        if (!keys.empty()) {
            statisticStore_->multiRemove(spaceId_, keys);
        }
        splitLargeAndSmallEdge(it.first);
        aliasSampleEdge(it.first);
        deleteSplitInfo(it.first, false);
    }
    statisticStore_->compact(spaceId_);
    processResult();
}

void RebuildSampleProcessor::parseTag(folly::StringPiece key, folly::StringPiece val) {
    TagID tagId = NebulaKeyUtils::getTagId(key);
    if (tagIndex_.find(tagId) == tagIndex_.end()) {
        return;
    }
    VertexID vid = NebulaKeyUtils::getVertexId(key);

    double weight;
    auto reader = RowReader::getTagPropReader(schemaMan_, val, spaceId_, tagId);
    if (reader == nullptr) {
        return;
    }
    auto res = RowReader::getPropByName(reader.get(), weightField_);
    if (!ok(res)) {
        VLOG(1) << "Bad value for prop: " << weightField_;
        return;
    } else {
        VariantType v = value(std::move(res));
        switch (v.which()) {
            case VAR_INT64:
                weight = boost::get<int64_t>(v);
                break;
            case VAR_DOUBLE:
                weight = boost::get<double>(v);
                break;
            default:
                LOG(FATAL) << "Unknown VariantType: " << v.which();
                // todo return weight type error
        }
    }
    VertexStatistic statistic;
    statistic.sVid_ = tagIndex_[tagId]++;
    statistic.vid_ = vid;
    statistic.weight_ = weight;
    tagStatistic_[tagId].emplace_back(statistic);
    tagWeightSummary_[tagId] += weight;

    if (tagIndex_[tagId] % batchSize_ == 0) {
        saveTagStatistic(tagId);
    }
}

void RebuildSampleProcessor::saveTagStatistic(TagID tagId) {
    std::vector<kvstore::KV> data;
    data.reserve(tagStatistic_[tagId].size());
    for (auto iter = tagStatistic_[tagId].begin(); iter != tagStatistic_[tagId].end(); iter++) {
        std::string key = StatisticStoreUtils::getStatisticKey(spaceId_, tagId, iter->sVid_);
        std::string value = StatisticStoreUtils::getStatisticValue(*iter);
        data.emplace_back(std::make_pair(key, value));
    }
    statisticStore_->multiPut(spaceId_, data);
    tagStatistic_[tagId].clear();
}

void RebuildSampleProcessor::splitLargeAndSmallTag(TagID tagId) {
    std::unique_ptr<kvstore::KVIterator> iter;
    std::string prefix = StatisticStoreUtils::statisticPrefix(spaceId_, tagId);
    auto ret = statisticStore_->prefix(spaceId_, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        cpp2::ResultCode thriftRet;
        thriftRet.set_code(cpp2::ErrorCode::E_UNKNOWN);
        codes_.emplace_back(thriftRet);
        LOG(ERROR) << "RebuildSampleProcessor statisticPrefix=" << prefix
                   << ", iterator error, code=" << ret;
        onFinished();
        return;
    }

    std::vector<kvstore::KV> dataForLarge;
    std::vector<kvstore::KV> dataForSmall;
    dataForLarge.reserve(batchSize_);
    dataForSmall.reserve(batchSize_);
    double standard = tagStandard_[tagId];
    double weightSummary = tagWeightSummary_[tagId];
    for (; iter->valid(); iter->next()) {
        std::string val = iter->val().str();
        VertexStatistic statistic;
        StatisticStoreUtils::parseStatisticValue(&statistic, val);
        statistic.weight_ = statistic.weight_ / weightSummary;
        std::string key;
        if (statistic.weight_ > standard) {
            key = StatisticStoreUtils::getStatisticLargeKey(spaceId_, tagId, statistic.sVid_);
            dataForLarge.emplace_back(
                std::make_pair(key, StatisticStoreUtils::doubleToString(statistic.weight_)));
        } else {
            key = StatisticStoreUtils::getStatisticSmallKey(spaceId_, tagId, statistic.sVid_);
            dataForSmall.emplace_back(
                std::make_pair(key, StatisticStoreUtils::doubleToString(statistic.weight_)));
        }
        if (dataForLarge.size() == batchSize_) {
            statisticStore_->multiPut(spaceId_, dataForLarge);
            dataForLarge.clear();
        }
        if (dataForSmall.size() == batchSize_) {
            statisticStore_->multiPut(spaceId_, dataForSmall);
            dataForSmall.clear();
        }
    }
    statisticStore_->multiPut(spaceId_, dataForLarge);
    statisticStore_->multiPut(spaceId_, dataForSmall);
    LOG(INFO) << "tagId " << tagId << " split large and small finished";
}

void RebuildSampleProcessor::aliasSampleTag(TagID tagId) {
    std::unique_ptr<kvstore::KVIterator> largeIter;
    std::string largePrefix = StatisticStoreUtils::statisticLargePrefix(spaceId_, tagId);
    auto ret = statisticStore_->prefix(spaceId_, largePrefix, &largeIter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        cpp2::ResultCode thriftRet;
        thriftRet.set_code(cpp2::ErrorCode::E_UNKNOWN);
        codes_.emplace_back(thriftRet);
        LOG(ERROR) << "RebuildSampleProcessor statisticLargePrefix=" << largePrefix
                   << ", iterator error, code=" << ret;
        onFinished();
        return;
    }
    std::unique_ptr<kvstore::KVIterator> smallIter;
    std::string smallPrefix = StatisticStoreUtils::statisticSmallPrefix(spaceId_, tagId);
    ret = statisticStore_->prefix(spaceId_, smallPrefix, &smallIter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        cpp2::ResultCode thriftRet;
        thriftRet.set_code(cpp2::ErrorCode::E_UNKNOWN);
        codes_.emplace_back(thriftRet);
        LOG(ERROR) << "RebuildSampleProcessor statisticSmallPrefix=" << smallPrefix
                   << ", iterator error, code=" << ret;
        onFinished();
        return;
    }

    int64_t largeSVid;
    int64_t smallSVid;
    std::unordered_map<int64_t, VertexStatistic> statisticMap;
    double largeWeight = 0, smallWeight = 0;
    if (largeIter->valid()) {
        std::string largeKey = largeIter->key().str();
        std::string weight = largeIter->val().str();
        largeWeight = strtod(weight.data(), nullptr);
        auto largeOffset = largePrefix.size();
        std::string largeId = largeKey.substr(largeOffset, largeKey.size() - largeOffset);
        largeSVid = strtoll(largeId.data(), nullptr, 10);
        std::string value;
        statisticStore_->get(
            spaceId_, StatisticStoreUtils::getStatisticKey(spaceId_, tagId, largeSVid), &value);
        VertexStatistic statistic;
        StatisticStoreUtils::parseStatisticValue(&statistic, value);
        statisticMap[largeSVid] = statistic;
    } else {
        while (smallIter->valid()) {
            std::string smallKey = smallIter->key().str();
            auto smallOffset = smallPrefix.size();
            std::string smallId = smallKey.substr(smallOffset, smallKey.size() - smallOffset);
            smallSVid = strtoll(smallId.data(), nullptr, 10);
            std::string value;
            statisticStore_->get(
                spaceId_, StatisticStoreUtils::getStatisticKey(spaceId_, tagId, smallSVid), &value);
            VertexStatistic statistic;
            StatisticStoreUtils::parseStatisticValue(&statistic, value);
            statistic.prob_ = 1.0;
            tagStatistic_[tagId].emplace_back(statistic);
            if (tagStatistic_[tagId].size() == batchSize_) {
                saveTagStatistic(tagId);
            }
            smallIter->next();
        }
        saveTagStatistic(tagId);
        LOG(INFO) << "tagId " << tagId << " alias sample build finished";
        return;
    }

    bool loadLarge = false;
    double standard = tagStandard_[tagId];
    while (largeIter->valid() && smallIter->valid()) {
        if (loadLarge) {
            std::string largeKey = largeIter->key().str();
            std::string weight = largeIter->val().str();
            largeWeight = strtod(weight.data(), nullptr);
            auto largeOffset = largePrefix.size();
            std::string largeId = largeKey.substr(largeOffset, largeKey.size() - largeOffset);
            largeSVid = strtoll(largeId.data(), nullptr, 10);
            std::string value;
            statisticStore_->get(
                spaceId_, StatisticStoreUtils::getStatisticKey(spaceId_, tagId, largeSVid), &value);
            VertexStatistic statistic;
            StatisticStoreUtils::parseStatisticValue(&statistic, value);
            statisticMap[largeSVid] = statistic;
        } else {
            std::string smallKey = smallIter->key().str();
            std::string weight = smallIter->val().str();
            smallWeight = strtod(weight.data(), nullptr);
            auto smallOffset = smallPrefix.size();
            std::string smallId = smallKey.substr(smallOffset, smallKey.size() - smallOffset);
            smallSVid = strtoll(smallId.data(), nullptr, 10);
            std::string value;
            statisticStore_->get(
                spaceId_, StatisticStoreUtils::getStatisticKey(spaceId_, tagId, smallSVid), &value);
            VertexStatistic statistic;
            StatisticStoreUtils::parseStatisticValue(&statistic, value);
            statisticMap[smallSVid] = statistic;
        }

        statisticMap[smallSVid].prob_ = smallWeight * tagIndex_[tagId];
        statisticMap[smallSVid].aliasVid_ = statisticMap[largeSVid].vid_;
        statisticMap[smallSVid].aliasWeight_ = statisticMap[largeSVid].weight_;

        largeWeight = largeWeight + smallWeight - standard;

        tagStatistic_[tagId].emplace_back(statisticMap[smallSVid]);
        statisticMap.erase(smallSVid);

        if (largeWeight > standard) {
            loadLarge = false;
            smallIter->next();
        } else {
            loadLarge = true;
            smallSVid = largeSVid;
            smallWeight = largeWeight;
            largeIter->next();
        }

        if (tagStatistic_[tagId].size() == batchSize_) {
            saveTagStatistic(tagId);
        }
    }
    if (smallIter->valid()) {
        std::string value;
        statisticStore_->get(
            spaceId_, StatisticStoreUtils::getStatisticKey(spaceId_, tagId, smallSVid), &value);
        VertexStatistic statistic;
        StatisticStoreUtils::parseStatisticValue(&statistic, value);
        statistic.prob_ = 1.0;
        tagStatistic_[tagId].emplace_back(statistic);
        if (tagStatistic_[tagId].size() == batchSize_) {
            saveTagStatistic(tagId);
        }
        smallIter->next();
    }
    while (smallIter->valid()) {
        std::string smallKey = smallIter->key().str();
        auto smallOffset = smallPrefix.size();
        std::string smallId = smallKey.substr(smallOffset, smallKey.size() - smallOffset);
        smallSVid = strtoll(smallId.data(), nullptr, 10);
        std::string value;
        statisticStore_->get(
            spaceId_, StatisticStoreUtils::getStatisticKey(spaceId_, tagId, smallSVid), &value);
        VertexStatistic statistic;
        StatisticStoreUtils::parseStatisticValue(&statistic, value);
        statistic.prob_ = 1.0;
        tagStatistic_[tagId].emplace_back(statistic);
        if (tagStatistic_[tagId].size() == batchSize_) {
            saveTagStatistic(tagId);
        }
        smallIter->next();
    }
    if (largeIter->valid()) {
        std::string value;
        statisticStore_->get(
            spaceId_, StatisticStoreUtils::getStatisticKey(spaceId_, tagId, largeSVid), &value);
        VertexStatistic statistic;
        StatisticStoreUtils::parseStatisticValue(&statistic, value);
        statistic.prob_ = 1.0;
        tagStatistic_[tagId].emplace_back(statistic);
        if (tagStatistic_[tagId].size() == batchSize_) {
            saveTagStatistic(tagId);
        }
        largeIter->next();
    }
    while (largeIter->valid()) {
        std::string largeKey = largeIter->key().str();
        auto largeOffset = largePrefix.size();
        std::string largeId = largeKey.substr(largeOffset, largeKey.size() - largeOffset);
        largeSVid = strtoll(largeId.data(), nullptr, 10);
        std::string value;
        statisticStore_->get(
            spaceId_, StatisticStoreUtils::getStatisticKey(spaceId_, tagId, largeSVid), &value);
        VertexStatistic statistic;
        StatisticStoreUtils::parseStatisticValue(&statistic, value);
        statistic.prob_ = 1.0;
        tagStatistic_[tagId].emplace_back(statistic);
        if (tagStatistic_[tagId].size() == batchSize_) {
            saveTagStatistic(tagId);
        }
        largeIter->next();
    }
    saveTagStatistic(tagId);
    LOG(INFO) << "tagId " << tagId << " alias sample build finished";
}

void RebuildSampleProcessor::parseEdge(folly::StringPiece key, folly::StringPiece val) {
    EdgeType edgeType = NebulaKeyUtils::getEdgeType(key);
    if (edgeIndex_.find(edgeType) == edgeIndex_.end()) {
        return;
    }
    VertexID srcId = NebulaKeyUtils::getSrcId(key);
    VertexID dstId = NebulaKeyUtils::getDstId(key);

    double weight;
    auto reader = RowReader::getEdgePropReader(schemaMan_, val, spaceId_, edgeType);
    if (reader == nullptr) {
        return;
    }
    auto res = RowReader::getPropByName(reader.get(), weightField_);
    if (!ok(res)) {
        VLOG(1) << "Bad value for prop: " << weightField_;
        return;
    } else {
        VariantType v = value(std::move(res));
        switch (v.which()) {
            case VAR_INT64:
                weight = boost::get<int64_t>(v);
                break;
            case VAR_DOUBLE:
                weight = boost::get<double>(v);
                break;
            default:
                LOG(FATAL) << "Unknown VariantType: " << v.which();
                // todo return weight type error
        }
    }
    EdgeStatistic statistic;
    statistic.sVid_ = edgeIndex_[edgeType]++;
    statistic.srcVid_ = srcId;
    statistic.dstVid_ = dstId;
    statistic.edgeType_ = edgeType;
    statistic.weight_ = weight;
    edgeStatistic_[edgeType].emplace_back(statistic);
    edgeWeightSummary_[edgeType] += weight;

    if (edgeIndex_[edgeType] % batchSize_ == 0) {
        saveEdgeStatistic(edgeType);
    }
}

void RebuildSampleProcessor::saveEdgeStatistic(EdgeType edgeType) {
    std::vector<kvstore::KV> data;
    data.reserve(edgeStatistic_[edgeType].size());
    for (auto iter = edgeStatistic_[edgeType].begin(); iter != edgeStatistic_[edgeType].end();
         iter++) {
        std::string key = StatisticStoreUtils::getStatisticKey(spaceId_, edgeType, iter->sVid_);
        std::string value = StatisticStoreUtils::getStatisticValue(*iter);
        data.emplace_back(std::make_pair(key, value));
    }
    statisticStore_->multiPut(spaceId_, data);
    edgeStatistic_[edgeType].clear();
}

void RebuildSampleProcessor::splitLargeAndSmallEdge(EdgeType edgeType) {
    std::unique_ptr<kvstore::KVIterator> iter;
    std::string prefix = StatisticStoreUtils::statisticPrefix(spaceId_, edgeType);
    auto ret = statisticStore_->prefix(spaceId_, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        cpp2::ResultCode thriftRet;
        thriftRet.set_code(cpp2::ErrorCode::E_UNKNOWN);
        codes_.emplace_back(thriftRet);
        LOG(ERROR) << "RebuildSampleProcessor statisticPrefix=" << prefix
                   << ", iterator error, code=" << ret;
        onFinished();
        return;
    }

    std::vector<kvstore::KV> dataForLarge;
    std::vector<kvstore::KV> dataForSmall;
    dataForLarge.reserve(batchSize_);
    dataForSmall.reserve(batchSize_);
    double standard = edgeStandard_[edgeType];
    double weightSummary = edgeWeightSummary_[edgeType];
    for (; iter->valid(); iter->next()) {
        std::string val = iter->val().str();
        EdgeStatistic statistic;
        StatisticStoreUtils::parseStatisticValue(&statistic, val);
        statistic.weight_ = statistic.weight_ / weightSummary;
        std::string key;
        if (statistic.weight_ > standard) {
            key = StatisticStoreUtils::getStatisticLargeKey(spaceId_, edgeType, statistic.sVid_);
            dataForLarge.emplace_back(
                std::make_pair(key, StatisticStoreUtils::doubleToString(statistic.weight_)));
        } else {
            key = StatisticStoreUtils::getStatisticSmallKey(spaceId_, edgeType, statistic.sVid_);
            dataForSmall.emplace_back(
                std::make_pair(key, StatisticStoreUtils::doubleToString(statistic.weight_)));
        }
        if (dataForLarge.size() == batchSize_) {
            statisticStore_->multiPut(spaceId_, dataForLarge);
            dataForLarge.clear();
        }
        if (dataForSmall.size() == batchSize_) {
            statisticStore_->multiPut(spaceId_, dataForSmall);
            dataForSmall.clear();
        }
    }
    statisticStore_->multiPut(spaceId_, dataForLarge);
    statisticStore_->multiPut(spaceId_, dataForSmall);
    LOG(INFO) << "edgeType " << edgeType << " split large and small finished";
}

void RebuildSampleProcessor::aliasSampleEdge(EdgeType edgeType) {
    std::unique_ptr<kvstore::KVIterator> largeIter;
    std::string largePrefix = StatisticStoreUtils::statisticLargePrefix(spaceId_, edgeType);
    auto ret = statisticStore_->prefix(spaceId_, largePrefix, &largeIter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        cpp2::ResultCode thriftRet;
        thriftRet.set_code(cpp2::ErrorCode::E_UNKNOWN);
        codes_.emplace_back(thriftRet);
        LOG(ERROR) << "RebuildSampleProcessor statisticLargePrefix=" << largePrefix
                   << ", iterator error, code=" << ret;
        onFinished();
        return;
    }
    std::unique_ptr<kvstore::KVIterator> smallIter;
    std::string smallPrefix = StatisticStoreUtils::statisticSmallPrefix(spaceId_, edgeType);
    ret = statisticStore_->prefix(spaceId_, smallPrefix, &smallIter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        cpp2::ResultCode thriftRet;
        thriftRet.set_code(cpp2::ErrorCode::E_UNKNOWN);
        codes_.emplace_back(thriftRet);
        LOG(ERROR) << "RebuildSampleProcessor statisticSmallPrefix=" << smallPrefix
                   << ", iterator error, code=" << ret;
        onFinished();
        return;
    }

    int64_t largeSVid;
    int64_t smallSVid;
    std::unordered_map<int64_t, EdgeStatistic> statisticMap;
    double largeWeight = 0, smallWeight = 0;
    if (largeIter->valid()) {
        std::string largeKey = largeIter->key().str();
        std::string weight = largeIter->val().str();
        largeWeight = strtod(weight.data(), nullptr);
        auto largeOffset = largePrefix.size();
        std::string largeId = largeKey.substr(largeOffset, largeKey.size() - largeOffset);
        largeSVid = strtoll(largeId.data(), nullptr, 10);
        std::string value;
        statisticStore_->get(
            spaceId_, StatisticStoreUtils::getStatisticKey(spaceId_, edgeType, largeSVid), &value);
        EdgeStatistic statistic;
        StatisticStoreUtils::parseStatisticValue(&statistic, value);
        statisticMap[largeSVid] = statistic;
    } else {
        while (smallIter->valid()) {
            std::string smallKey = smallIter->key().str();
            auto smallOffset = smallPrefix.size();
            std::string smallId = smallKey.substr(smallOffset, smallKey.size() - smallOffset);
            smallSVid = strtoll(smallId.data(), nullptr, 10);
            std::string value;
            statisticStore_->get(
                spaceId_,
                StatisticStoreUtils::getStatisticKey(spaceId_, edgeType, smallSVid),
                &value);
            EdgeStatistic statistic;
            StatisticStoreUtils::parseStatisticValue(&statistic, value);
            statistic.prob_ = 1.0;
            edgeStatistic_[edgeType].emplace_back(statistic);
            if (edgeStatistic_[edgeType].size() == batchSize_) {
                saveEdgeStatistic(edgeType);
            }
            smallIter->next();
        }
        saveEdgeStatistic(edgeType);
        LOG(INFO) << "edgeType " << edgeType << " alias sample build finished";
        return;
    }

    bool loadLarge = false;
    double standard = edgeStandard_[edgeType];
    while (largeIter->valid() && smallIter->valid()) {
        if (loadLarge) {
            std::string largeKey = largeIter->key().str();
            std::string weight = largeIter->val().str();
            largeWeight = strtod(weight.data(), nullptr);
            auto largeOffset = largePrefix.size();
            std::string largeId = largeKey.substr(largeOffset, largeKey.size() - largeOffset);
            largeSVid = strtoll(largeId.data(), nullptr, 10);
            std::string value;
            statisticStore_->get(
                spaceId_,
                StatisticStoreUtils::getStatisticKey(spaceId_, edgeType, largeSVid),
                &value);
            EdgeStatistic statistic;
            StatisticStoreUtils::parseStatisticValue(&statistic, value);
            statisticMap[largeSVid] = statistic;
        } else {
            std::string smallKey = smallIter->key().str();
            std::string weight = smallIter->val().str();
            smallWeight = strtod(weight.data(), nullptr);
            auto smallOffset = smallPrefix.size();
            std::string smallId = smallKey.substr(smallOffset, smallKey.size() - smallOffset);
            smallSVid = strtoll(smallId.data(), nullptr, 10);
            std::string value;
            statisticStore_->get(
                spaceId_,
                StatisticStoreUtils::getStatisticKey(spaceId_, edgeType, smallSVid),
                &value);
            EdgeStatistic statistic;
            StatisticStoreUtils::parseStatisticValue(&statistic, value);
            statisticMap[smallSVid] = statistic;
        }

        statisticMap[smallSVid].prob_ = smallWeight * edgeIndex_[edgeType];
        statisticMap[smallSVid].aliasSrcVid_ = statisticMap[largeSVid].srcVid_;
        statisticMap[smallSVid].aliasDstVid_ = statisticMap[largeSVid].dstVid_;
        statisticMap[smallSVid].aliasWeight_ = statisticMap[largeSVid].weight_;

        largeWeight = largeWeight + smallWeight - standard;

        edgeStatistic_[edgeType].emplace_back(statisticMap[smallSVid]);
        statisticMap.erase(smallSVid);

        if (largeWeight > standard) {
            loadLarge = false;
            smallIter->next();
        } else {
            loadLarge = true;
            smallSVid = largeSVid;
            smallWeight = largeWeight;
            largeIter->next();
        }

        if (edgeStatistic_[edgeType].size() == batchSize_) {
            saveEdgeStatistic(edgeType);
        }
    }
    if (smallIter->valid()) {
        std::string value;
        statisticStore_->get(
            spaceId_, StatisticStoreUtils::getStatisticKey(spaceId_, edgeType, smallSVid), &value);
        EdgeStatistic statistic;
        StatisticStoreUtils::parseStatisticValue(&statistic, value);
        statistic.prob_ = 1.0;
        edgeStatistic_[edgeType].emplace_back(statistic);
        if (edgeStatistic_[edgeType].size() == batchSize_) {
            saveEdgeStatistic(edgeType);
        }
        smallIter->next();
    }
    while (smallIter->valid()) {
        std::string smallKey = smallIter->key().str();
        auto smallOffset = smallPrefix.size();
        std::string smallId = smallKey.substr(smallOffset, smallKey.size() - smallOffset);
        smallSVid = strtoll(smallId.data(), nullptr, 10);
        std::string value;
        statisticStore_->get(
            spaceId_, StatisticStoreUtils::getStatisticKey(spaceId_, edgeType, smallSVid), &value);
        EdgeStatistic statistic;
        StatisticStoreUtils::parseStatisticValue(&statistic, value);
        statistic.prob_ = 1.0;
        edgeStatistic_[edgeType].emplace_back(statistic);
        if (edgeStatistic_[edgeType].size() == batchSize_) {
            saveEdgeStatistic(edgeType);
        }
        smallIter->next();
    }
    if (largeIter->valid()) {
        std::string value;
        statisticStore_->get(
            spaceId_, StatisticStoreUtils::getStatisticKey(spaceId_, edgeType, largeSVid), &value);
        EdgeStatistic statistic;
        StatisticStoreUtils::parseStatisticValue(&statistic, value);
        statistic.prob_ = 1.0;
        edgeStatistic_[edgeType].emplace_back(statistic);
        if (edgeStatistic_[edgeType].size() == batchSize_) {
            saveEdgeStatistic(edgeType);
        }
        largeIter->next();
    }
    while (largeIter->valid()) {
        std::string largeKey = largeIter->key().str();
        auto largeOffset = largePrefix.size();
        std::string largeId = largeKey.substr(largeOffset, largeKey.size() - largeOffset);
        largeSVid = strtoll(largeId.data(), nullptr, 10);
        std::string value;
        statisticStore_->get(
            spaceId_, StatisticStoreUtils::getStatisticKey(spaceId_, edgeType, largeSVid), &value);
        EdgeStatistic statistic;
        StatisticStoreUtils::parseStatisticValue(&statistic, value);
        statistic.prob_ = 1.0;
        edgeStatistic_[edgeType].emplace_back(statistic);
        if (edgeStatistic_[edgeType].size() == batchSize_) {
            saveEdgeStatistic(edgeType);
        }
        largeIter->next();
    }
    saveEdgeStatistic(edgeType);
    LOG(INFO) << "edgeType " << edgeType << " alias sample build finished";
}

void RebuildSampleProcessor::saveTagAndEdgeWeights() {
    statisticStore_->addTagOrEdgeCount(spaceId_, tagIndex_);
    statisticStore_->addTagOrEdgeCount(spaceId_, edgeIndex_);
    statisticStore_->addTagOrEdgeWeights(spaceId_, tagWeightSummary_);
    statisticStore_->addTagOrEdgeWeights(spaceId_, edgeWeightSummary_);
    std::vector<kvstore::KV> data;
    data.reserve(tagIndex_.size() + edgeIndex_.size() + tagWeightSummary_.size() +
                 edgeWeightSummary_.size());
    for (auto tagIndex : tagIndex_) {
        std::string key = StatisticStoreUtils::getTagOrEdgeCountsKey(spaceId_, tagIndex.first);
        data.emplace_back(std::make_pair(key, std::to_string(tagIndex.second)));
    }
    for (auto edgeIndex : edgeIndex_) {
        std::string key = StatisticStoreUtils::getTagOrEdgeCountsKey(spaceId_, edgeIndex.first);
        data.emplace_back(std::make_pair(key, std::to_string(edgeIndex.second)));
    }
    for (auto tagWeight : tagWeightSummary_) {
        std::string key = StatisticStoreUtils::getTagOrEdgeWeightsKey(spaceId_, tagWeight.first);
        std::string weight = StatisticStoreUtils::doubleToString(tagWeight.second);
        data.emplace_back(std::make_pair(key, weight));
    }
    for (auto edgeWeight : edgeWeightSummary_) {
        std::string key = StatisticStoreUtils::getTagOrEdgeWeightsKey(spaceId_, edgeWeight.first);
        std::string weight = StatisticStoreUtils::doubleToString(edgeWeight.second);
        data.emplace_back(std::make_pair(key, weight));
    }
    statisticStore_->multiPut(spaceId_, data);
    LOG(INFO) << "saveTagAndEdgeWeights finished";
}

void RebuildSampleProcessor::deleteSplitInfo(int32_t tagOrEdge, bool isTag) {
    std::vector<std::string> keys;
    keys.reserve(batchSize_);
    int64_t index = 0;
    if (isTag) {
        index = tagIndex_[tagOrEdge];
    } else {
        index = edgeIndex_[tagOrEdge];
    }
    for (int64_t i = 0; i < index; i++) {
        std::string key = StatisticStoreUtils::getStatisticLargeKey(spaceId_, tagOrEdge, i);
        keys.emplace_back(key);
        if (keys.size() == batchSize_) {
            statisticStore_->multiRemove(spaceId_, keys);
            keys.clear();
        }
        key = StatisticStoreUtils::getStatisticSmallKey(spaceId_, tagOrEdge, i);
        keys.emplace_back(key);
        if (keys.size() == batchSize_) {
            statisticStore_->multiRemove(spaceId_, keys);
            keys.clear();
        }
    }
    statisticStore_->multiRemove(spaceId_, keys);
    if (isTag) {
        LOG(INFO) << "tagId " << tagOrEdge << " delete split info finished";
    } else {
        LOG(INFO) << "edgeType " << tagOrEdge << " delete split info finished";
    }
}

void RebuildSampleProcessor::processResult() {
    std::vector<cpp2::RebuildSampleWeights> weights;
    weights.reserve(tagWeightSummary_.size() + edgeWeightSummary_.size());
    for (auto tagWeight : tagWeightSummary_) {
        cpp2::RebuildSampleWeights weight;
        weight.set_tagId(tagWeight.first);
        weight.set_weights(tagWeight.second);
        weights.emplace_back(weight);
    }
    for (auto edgeWeight : edgeWeightSummary_) {
        cpp2::RebuildSampleWeights weight;
        weight.set_edgeType(edgeWeight.first);
        weight.set_weights(edgeWeight.second);
        weights.emplace_back(weight);
    }
    resp_.set_weights(std::move(weights));
    onFinished();
}

}   // namespace storage
}   // namespace nebula
