/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "kvstore/StatisticStore.h"
#include <rocksdb/convenience.h>
#include "base/Base.h"
#include "fs/FileUtils.h"
#include "kvstore/RocksEngine.h"
#include "kvstore/RocksEngineConfig.h"

namespace nebula {
namespace kvstore {

StatisticStore::~StatisticStore() {
    dbs_.clear();
    LOG(INFO) << "~StatisticStore()";
}

bool StatisticStore::init() {
    auto rootPath = folly::stringPrintf("%s/nebula", dataPath_.c_str());
    auto dirs = fs::FileUtils::listAllDirsInDir(rootPath.c_str());
    for (auto& dir : dirs) {
        GraphSpaceID spaceId;
        try {
            spaceId = folly::to<GraphSpaceID>(dir);
        } catch (const std::exception& ex) {
            LOG(ERROR) << "Data path invalid: " << ex.what();
            return false;
        }
        std::string statisticPath =
            folly::stringPrintf("%s/nebula/%d/statistic/data", dataPath_.c_str(), spaceId);
        if (fs::FileUtils::fileType(statisticPath.c_str()) == fs::FileType::NOTEXIST) {
            LOG(INFO) << "spaceID " << spaceId << " need not to statistic";
            continue;
        }
        if (fs::FileUtils::fileType(statisticPath.c_str()) != fs::FileType::DIRECTORY) {
            LOG(FATAL) << statisticPath << " is not directory";
        }
        dbs_[spaceId].reset(newDB(statisticPath));
    }
    return true;
}

void StatisticStore::stop() {
    for (const auto& db : dbs_) {
        rocksdb::CancelAllBackgroundWork(db.second.get(), true);
    }
}

rocksdb::DB* StatisticStore::newDB(const std::string& path) {
    rocksdb::Options options;
    rocksdb::DB* db = nullptr;
    rocksdb::Status status = initRocksdbOptions(options);
    CHECK(status.ok());
    status = rocksdb::DB::Open(options, path, &db);
    if (status.IsNoSpace()) {
        LOG(WARNING) << status.ToString();
    } else {
        CHECK(status.ok()) << status.ToString();
    }
    return db;
}

void StatisticStore::addStatistic(GraphSpaceID spaceId) {
    folly::RWSpinLock::WriteHolder wh(&lock_);
    if (this->dbs_.find(spaceId) != this->dbs_.end()) {
        LOG(INFO) << "Space " << spaceId << " has statistic!";
        return;
    }
    std::string statisticPath =
        folly::stringPrintf("%s/nebula/%d/statistic/data", dataPath_.c_str(), spaceId);
    if (!fs::FileUtils::makeDir(statisticPath)) {
        LOG(FATAL) << "makeDir " << statisticPath << " failed";
    }
    LOG(INFO) << "Create statistic for space " << spaceId << ", dataPath=" << statisticPath;
    dbs_[spaceId].reset(newDB(statisticPath));
}

ResultCode StatisticStore::setOption(GraphSpaceID spaceId,
                                     const std::string& configKey,
                                     const std::string& configValue) {
    std::unordered_map<std::string, std::string> configOptions = {{configKey, configValue}};

    rocksdb::Status status = dbs_[spaceId]->SetOptions(configOptions);
    if (status.ok()) {
        LOG(INFO) << "SetOption Succeeded: " << configKey << ":" << configValue;
        return ResultCode::SUCCEEDED;
    } else {
        LOG(ERROR) << "SetOption Failed: " << configKey << ":" << configValue;
        return ResultCode::ERR_INVALID_ARGUMENT;
    }
}

ResultCode StatisticStore::setDBOption(GraphSpaceID spaceId,
                                       const std::string& configKey,
                                       const std::string& configValue) {
    std::unordered_map<std::string, std::string> configOptions = {{configKey, configValue}};

    rocksdb::Status status = dbs_[spaceId]->SetDBOptions(configOptions);
    if (status.ok()) {
        LOG(INFO) << "SetDBOption Succeeded: " << configKey << ":" << configValue;
        return ResultCode::SUCCEEDED;
    } else {
        LOG(ERROR) << "SetDBOption Failed: " << configKey << ":" << configValue;
        return ResultCode::ERR_INVALID_ARGUMENT;
    }
}

ResultCode StatisticStore::compact(GraphSpaceID spaceId) {
    rocksdb::CompactRangeOptions options;
    rocksdb::Status status = dbs_[spaceId]->CompactRange(options, nullptr, nullptr);
    if (status.ok()) {
        return ResultCode::SUCCEEDED;
    } else {
        LOG(ERROR) << "CompactAll Failed: " << status.ToString();
        return ResultCode::ERR_UNKNOWN;
    }
}

ResultCode StatisticStore::flush(GraphSpaceID spaceId) {
    rocksdb::FlushOptions options;
    rocksdb::Status status = dbs_[spaceId]->Flush(options);
    if (status.ok()) {
        return ResultCode::SUCCEEDED;
    } else {
        LOG(ERROR) << "Flush Failed: " << status.ToString();
        return ResultCode::ERR_UNKNOWN;
    }
}

ResultCode StatisticStore::multiRemove(GraphSpaceID spaceId, std::vector<std::string> keys) {
    rocksdb::WriteBatch deletes(100000);
    for (size_t i = 0; i < keys.size(); i++) {
        deletes.Delete(keys[i]);
    }
    rocksdb::WriteOptions options;
    rocksdb::Status status = dbs_[spaceId]->Write(options, &deletes);
    if (status.ok()) {
        return ResultCode::SUCCEEDED;
    } else {
        VLOG(3) << "MultiRemove Failed: " << status.ToString();
        return ResultCode::ERR_UNKNOWN;
    }
}

ResultCode StatisticStore::multiPut(GraphSpaceID spaceId, std::vector<KV> kvs) {
    rocksdb::WriteBatch updates(100000);
    for (size_t i = 0; i < kvs.size(); i++) {
        updates.Put(kvs[i].first, kvs[i].second);
    }
    rocksdb::WriteOptions options;
    rocksdb::Status status = dbs_[spaceId]->Write(options, &updates);
    if (status.ok()) {
        return ResultCode::SUCCEEDED;
    } else {
        VLOG(3) << "MultiPut Failed: " << status.ToString();
        return ResultCode::ERR_UNKNOWN;
    }
}

ResultCode StatisticStore::put(GraphSpaceID spaceId, KV kv) {
    rocksdb::WriteOptions options;
    rocksdb::Status status = dbs_[spaceId]->Put(options, kv.first, kv.second);
    if (status.ok()) {
        return ResultCode::SUCCEEDED;
    } else {
        VLOG(3) << "Put Failed: " << kv.first << status.ToString();
        return ResultCode::ERR_UNKNOWN;
    }
}

ResultCode StatisticStore::get(GraphSpaceID spaceId, std::string key, std::string* value) {
    rocksdb::ReadOptions options;
    rocksdb::Status status = dbs_[spaceId]->Get(options, rocksdb::Slice(key), value);
    if (status.ok()) {
        return ResultCode::SUCCEEDED;
    } else if (status.IsNotFound()) {
        VLOG(3) << "Get: " << key << " Not Found";
        return ResultCode::ERR_KEY_NOT_FOUND;
    } else {
        VLOG(3) << "Get Failed: " << key << " " << status.ToString();
        return ResultCode::ERR_UNKNOWN;
    }
}

ResultCode StatisticStore::multiGet(GraphSpaceID spaceId,
                                    const std::vector<std::string>& keys,
                                    std::vector<std::string>* values) {
    rocksdb::ReadOptions options;
    std::vector<rocksdb::Slice> slices;
    for (size_t index = 0; index < keys.size(); index++) {
        slices.emplace_back(keys[index]);
    }
    auto status = dbs_[spaceId]->MultiGet(options, slices, values);
    auto allExist = std::all_of(status.begin(), status.end(), [](const auto& s) { return s.ok(); });
    if (allExist) {
        return ResultCode::SUCCEEDED;
    } else {
        return ResultCode::ERR_PARTIAL_RESULT;
    }
}

ResultCode StatisticStore::prefix(GraphSpaceID spaceId,
                                  const std::string& prefix,
                                  std::unique_ptr<KVIterator>* storageIter) {
    rocksdb::ReadOptions options;
    options.prefix_same_as_start = true;
    rocksdb::Iterator* iter = dbs_[spaceId]->NewIterator(options);
    if (iter) {
        iter->Seek(rocksdb::Slice(prefix));
    }
    *storageIter = std::make_unique<RocksPrefixIter>(iter, prefix);
    return ResultCode::SUCCEEDED;
}

}   // namespace kvstore
}   // namespace nebula
