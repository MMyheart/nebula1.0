/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef KVSTORE_STATISTICSTORE_H_
#define KVSTORE_STATISTICSTORE_H_

#include <rocksdb/db.h>
#include "KVIterator.h"
#include "base/Base.h"
#include "kvstore/Common.h"

namespace nebula {
namespace kvstore {

class StatisticStore {
public:
    explicit StatisticStore(std::string dataPath) : dataPath_(dataPath) {}

    ~StatisticStore();

    bool init();

    void stop();

    void addStatistic(GraphSpaceID spaceId);

    ResultCode setOption(GraphSpaceID spaceId,
                         const std::string& configKey,
                         const std::string& configValue);

    ResultCode setDBOption(GraphSpaceID spaceId,
                           const std::string& configKey,
                           const std::string& configValue);

    ResultCode compact(GraphSpaceID spaceId);

    ResultCode flush(GraphSpaceID spaceId);

    ResultCode multiRemove(GraphSpaceID spaceId, std::vector<std::string> keys);

    ResultCode put(GraphSpaceID spaceId, KV kv);

    ResultCode multiPut(GraphSpaceID spaceId, std::vector<KV> kvs);

    ResultCode get(GraphSpaceID spaceId, std::string key, std::string* value);

    ResultCode multiGet(GraphSpaceID spaceId,
                        const std::vector<std::string>& keys,
                        std::vector<std::string>* values);

    ResultCode prefix(GraphSpaceID spaceId,
                      const std::string& prefix,
                      std::unique_ptr<KVIterator>* storageIter);

    void addTagOrEdgeCount(GraphSpaceID spaceId, std::unordered_map<int32_t, int64_t> count) {
        for (auto it = count.begin(); it != count.end(); it++) {
            if (tagOrEdgeCount_.find(spaceId) == tagOrEdgeCount_.end()) {
                std::unordered_map<int32_t, int64_t> map;
                tagOrEdgeCount_[spaceId] = map;
            }
            tagOrEdgeCount_[spaceId][it->first] = it->second;
        }
    }

    void addTagOrEdgeWeights(GraphSpaceID spaceId, std::unordered_map<int32_t, double> weights) {
        for (auto it = weights.begin(); it != weights.end(); it++) {
            if (tagOrEdgeWeights_.find(spaceId) == tagOrEdgeWeights_.end()) {
                std::unordered_map<int32_t, double> map;
                tagOrEdgeWeights_[spaceId] = map;
            }
            tagOrEdgeWeights_[spaceId][it->first] = it->second;
        }
    }

    bool getTagOrEdgeCount(GraphSpaceID spaceId,
                           const std::vector<int32_t>& tagOrEdges,
                           std::unordered_map<int32_t, int64_t>* count) {
        if (tagOrEdgeCount_.find(spaceId) == tagOrEdgeCount_.end()) {
            return false;
        }
        auto counts = tagOrEdgeCount_[spaceId];
        for (auto tagOrEdge : tagOrEdges) {
            count->insert(std::make_pair(tagOrEdge, counts[tagOrEdge]));
        }
        return true;
    }

    bool getTagOrEdgeWeights(GraphSpaceID spaceId,
                             const std::vector<int32_t>& tagOrEdges,
                             std::unordered_map<int32_t, double>* weight) {
        if (tagOrEdgeWeights_.find(spaceId) == tagOrEdgeWeights_.end()) {
            return false;
        }
        auto weights = tagOrEdgeWeights_[spaceId];
        for (auto tagOrEdge : tagOrEdges) {
            weight->insert(std::make_pair(tagOrEdge, weights[tagOrEdge]));
        }
        return true;
    }

private:
    rocksdb::DB* newDB(const std::string& path);

private:
    // The lock used to protect spaces_
    folly::RWSpinLock lock_;
    std::string dataPath_;
    std::unordered_map<GraphSpaceID, std::unique_ptr<rocksdb::DB>> dbs_;
    std::unordered_map<GraphSpaceID, std::unordered_map<int32_t, int64_t>> tagOrEdgeCount_;
    std::unordered_map<GraphSpaceID, std::unordered_map<int32_t, double>> tagOrEdgeWeights_;
};

}   // namespace kvstore
}   // namespace nebula

#endif   // KVSTORE_STATISTICSTORE_H_
