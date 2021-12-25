/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "StatisticDumper.h"
#include <iomanip>
#include "base/Base.h"
#include "fs/FileUtils.h"
#include "time/Duration.h"

DEFINE_string(space, "", "The space id.");
DEFINE_string(db_path, "./", "Path to rocksdb.");
DEFINE_string(vids, "", "A list of vertex ids seperated by comma.");
DEFINE_string(tags, "", "A list of tag id seperated by comma.");
DEFINE_string(edges, "", "A list of edge id seperated by comma.");
DEFINE_int64(limit, 1000, "Limit to output.");

namespace nebula {
namespace storage {

Status StatisticDumper::init() {
    spaceId_ = atoi(FLAGS_space.data());
    try {
        folly::splitTo<VertexID>(',', FLAGS_vids, std::inserter(vids_, vids_.begin()), true);
        folly::splitTo<TagID>(',', FLAGS_tags, std::inserter(tags_, tags_.begin()), true);
        folly::splitTo<EdgeType>(',', FLAGS_edges, std::inserter(edges_, edges_.begin()), true);
    } catch (const std::exception& e) {
        return Status::Error("Parse parts/tags/edges error: %s", e.what());
    }
    return openDb();
}

Status StatisticDumper::openDb() {
    if (!fs::FileUtils::exist(FLAGS_db_path)) {
        return Status::Error("Db path '%s' not exists.", FLAGS_db_path.c_str());
    }
    auto path = fs::FileUtils::joinPath(FLAGS_db_path, "data");

    rocksdb::DB* dbPtr;
    auto status = rocksdb::DB::OpenForReadOnly(options_, path, &dbPtr);
    if (!status.ok()) {
        return Status::Error("Unable to open database '%s' for reading: '%s'",
                             path.c_str(),
                             status.ToString().c_str());
    }
    db_.reset(dbPtr);
    return Status::OK();
}

void StatisticDumper::run() {
    time::Duration dur;

    if (!tags_.empty()) {
        scanTag();
    }
    if (!edges_.empty()) {
        scanEdge();
    }
    scanCountAndSummaryWeights();

    std::cout << "Time cost: " << dur.elapsedInUSec() << " us\n\n";
}

void StatisticDumper::scanTag() {
    std::cout << std::setw(10) << std::left << "tagId" << std::setw(10) << std::left << "sVid"
              << std::setw(30) << std::left << "prob" << std::setw(10) << std::left << "vid"
              << std::setw(20) << std::left << "weight" << std::setw(10) << std::left << "aliasVid"
              << std::setw(10) << std::left << "aliasWeight" << std::endl;
    for (auto tag : tags_) {
        std::string keyPrefix;
        keyPrefix.append("A").append(std::to_string(spaceId_)).append(std::to_string(tag));

        rocksdb::ReadOptions options;
        options.prefix_same_as_start = true;
        rocksdb::Iterator* iter = db_->NewIterator(options);
        if (iter) {
            iter->Seek(rocksdb::Slice(keyPrefix));
        }
        std::unique_ptr<kvstore::KVIterator> smallIter =
            std::make_unique<kvstore::RocksPrefixIter>(iter, keyPrefix);

        auto offset = keyPrefix.size();

        for (; smallIter->valid(); smallIter->next()) {
            auto key = smallIter->key().str();
            auto value = smallIter->val().str();
            auto svid = key.substr(offset, key.size() - offset);

            std::string separator_ = "$";
            std::vector<std::string> vec;
            splitString(value, vec, separator_);

            std::cout << std::setw(10) << std::left << tag << std::setw(10) << std::left
                      << strtoll(vec[0].data(), nullptr, 10) << std::setw(30) << std::left
                      << std::fixed << strtod(vec[1].data(), nullptr) << std::setw(10) << std::left
                      << strtoll(vec[2].data(), nullptr, 10) << std::setw(20) << std::left
                      << strtod(vec[3].data(), nullptr) << std::setw(10) << std::left
                      << strtoll(vec[4].data(), nullptr, 10) << std::setw(10) << std::left
                      << strtod(vec[5].data(), nullptr) << std::endl;
        }
    }

    std::cout << std::endl << "---------------------------------------------------" << std::endl;
    std::cout << std::setw(10) << std::left << "small tagId" << std::setw(10) << std::left << "sVid"
              << std::setw(30) << std::left << "weight" << std::endl;
    for (auto tag : tags_) {
        std::string keyPrefix;
        keyPrefix.append("S").append(std::to_string(spaceId_)).append(std::to_string(tag));

        rocksdb::ReadOptions options;
        options.prefix_same_as_start = true;
        rocksdb::Iterator* iter = db_->NewIterator(options);
        if (iter) {
            iter->Seek(rocksdb::Slice(keyPrefix));
        }
        std::unique_ptr<kvstore::KVIterator> smallIter =
            std::make_unique<kvstore::RocksPrefixIter>(iter, keyPrefix);

        auto offset = keyPrefix.size();

        for (; smallIter->valid(); smallIter->next()) {
            auto key = smallIter->key().str();
            auto weight = smallIter->val().str();
            auto svid = key.substr(offset, key.size() - offset);
            std::cout << std::setw(10) << std::left << tag << std::setw(10) << std::left << svid
                      << std::setw(30) << std::left << weight << std::endl;
        }
    }

    std::cout << std::endl << "---------------------------------------------------" << std::endl;
    std::cout << std::setw(10) << std::left << "large tagId" << std::setw(10) << std::left << "sVid"
              << std::setw(30) << std::left << "weight" << std::endl;
    for (auto tag : tags_) {
        std::string keyPrefix;
        keyPrefix.append("L").append(std::to_string(spaceId_)).append(std::to_string(tag));

        rocksdb::ReadOptions options;
        options.prefix_same_as_start = true;
        rocksdb::Iterator* iter = db_->NewIterator(options);
        if (iter) {
            iter->Seek(rocksdb::Slice(keyPrefix));
        }
        std::unique_ptr<kvstore::KVIterator> smallIter =
            std::make_unique<kvstore::RocksPrefixIter>(iter, keyPrefix);

        auto offset = keyPrefix.size();

        for (; smallIter->valid(); smallIter->next()) {
            auto key = smallIter->key().str();
            auto weight = smallIter->val().str();
            auto svid = key.substr(offset, key.size() - offset);
            std::cout << std::setw(10) << std::left << tag << std::setw(10) << std::left << svid
                      << std::setw(30) << std::left << weight << std::endl;
        }
    }
}

void StatisticDumper::scanEdge() {
    std::cout << std::setw(10) << std::left << "EdgeType" << std::setw(10) << std::left << "sVid"
              << std::setw(30) << std::left << "prob" << std::setw(10) << std::left << "srcVid"
              << std::setw(10) << std::left << "dstVid" << std::setw(20) << std::left << "weight"
              << std::setw(20) << std::left << "aliasSrcVid" << std::setw(20) << std::left
              << "aliasDstVid" << std::setw(30) << std::left << "aliasWeight" << std::endl;
    for (auto edge : edges_) {
        std::string keyPrefix;
        keyPrefix.append("A").append(std::to_string(spaceId_)).append(std::to_string(edge));

        rocksdb::ReadOptions options;
        options.prefix_same_as_start = true;
        rocksdb::Iterator* iter = db_->NewIterator(options);
        if (iter) {
            iter->Seek(rocksdb::Slice(keyPrefix));
        }
        std::unique_ptr<kvstore::KVIterator> smallIter =
            std::make_unique<kvstore::RocksPrefixIter>(iter, keyPrefix);

        auto offset = keyPrefix.size();

        for (; smallIter->valid(); smallIter->next()) {
            auto key = smallIter->key().str();
            auto value = smallIter->val().str();
            auto svid = key.substr(offset, key.size() - offset);

            std::string separator_ = "$";
            std::vector<std::string> vec;
            splitString(value, vec, separator_);

            std::cout << std::setw(10) << std::left << edge << std::setw(10) << std::left
                      << strtoll(vec[0].data(), nullptr, 10) << std::setw(30) << std::left
                      << std::fixed << strtod(vec[1].data(), nullptr) << std::setw(10) << std::left
                      << strtoll(vec[2].data(), nullptr, 10) << std::setw(10) << std::left
                      << strtoll(vec[3].data(), nullptr, 10) << std::setw(20) << std::left
                      << strtod(vec[4].data(), nullptr) << std::setw(20) << std::left
                      << strtoll(vec[5].data(), nullptr, 10) << std::setw(20) << std::left
                      << strtoll(vec[6].data(), nullptr, 10) << std::setw(30) << std::left
                      << strtod(vec[7].data(), nullptr) << std::endl;
        }
    }

    std::cout << std::endl << "---------------------------------------------------" << std::endl;
    std::cout << std::setw(10) << std::left << "small edgeType" << std::setw(10) << std::left
              << "sVid" << std::setw(30) << std::left << "weight" << std::endl;
    for (auto edge : edges_) {
        std::string keyPrefix;
        keyPrefix.append("S").append(std::to_string(spaceId_)).append(std::to_string(edge));

        rocksdb::ReadOptions options;
        options.prefix_same_as_start = true;
        rocksdb::Iterator* iter = db_->NewIterator(options);
        if (iter) {
            iter->Seek(rocksdb::Slice(keyPrefix));
        }
        std::unique_ptr<kvstore::KVIterator> smallIter =
            std::make_unique<kvstore::RocksPrefixIter>(iter, keyPrefix);

        auto offset = keyPrefix.size();

        for (; smallIter->valid(); smallIter->next()) {
            auto key = smallIter->key().str();
            auto weight = smallIter->val().str();
            auto svid = key.substr(offset, key.size() - offset);
            std::cout << std::setw(10) << std::left << edge << std::setw(10) << std::left << svid
                      << std::setw(30) << std::left << weight << std::endl;
        }
    }

    std::cout << std::endl << "---------------------------------------------------" << std::endl;
    std::cout << std::setw(10) << std::left << "large edge" << std::setw(10) << std::left << "sVid"
              << std::setw(30) << std::left << "weight" << std::endl;
    for (auto edge : edges_) {
        std::string keyPrefix;
        keyPrefix.append("L").append(std::to_string(spaceId_)).append(std::to_string(edge));

        rocksdb::ReadOptions options;
        options.prefix_same_as_start = true;
        rocksdb::Iterator* iter = db_->NewIterator(options);
        if (iter) {
            iter->Seek(rocksdb::Slice(keyPrefix));
        }
        std::unique_ptr<kvstore::KVIterator> smallIter =
            std::make_unique<kvstore::RocksPrefixIter>(iter, keyPrefix);

        auto offset = keyPrefix.size();

        for (; smallIter->valid(); smallIter->next()) {
            auto key = smallIter->key().str();
            auto weight = smallIter->val().str();
            auto svid = key.substr(offset, key.size() - offset);
            std::cout << std::setw(10) << std::left << edge << std::setw(10) << std::left << svid
                      << std::setw(30) << std::left << weight << std::endl;
        }
    }
}

void StatisticDumper::scanCountAndSummaryWeights() {
    std::cout << std::setw(20) << std::left << "TagOrEdge" << std::setw(10) << std::left << "Count"
              << std::setw(30) << std::left << "Weights" << std::endl;
    std::unordered_map<std::string, std::string> counts;
    std::string prefix;
    prefix.append("C").append(std::to_string(spaceId_));
    rocksdb::ReadOptions options;
    options.prefix_same_as_start = true;
    rocksdb::Iterator* iter = db_->NewIterator(options);
    if (iter) {
        iter->Seek(rocksdb::Slice(prefix));
    }
    std::unique_ptr<kvstore::KVIterator> cIter =
        std::make_unique<kvstore::RocksPrefixIter>(iter, prefix);
    for (; cIter->valid(); cIter->next()) {
        std::string key = cIter->key().str();
        std::string value = cIter->val().str();
        auto offset = prefix.size();
        std::string tagId = key.substr(offset, key.size() - offset);
        counts[tagId] = value;
    }
    std::unordered_map<std::string, std::string> weights;
    std::string wprefix;
    wprefix.append("W").append(std::to_string(spaceId_));
    if (iter) {
        iter->Seek(rocksdb::Slice(wprefix));
    }
    std::unique_ptr<kvstore::KVIterator> wIter =
        std::make_unique<kvstore::RocksPrefixIter>(iter, wprefix);
    for (; wIter->valid(); wIter->next()) {
        std::string key = wIter->key().str();
        std::string value = wIter->val().str();
        auto offset = prefix.size();
        std::string tagId = key.substr(offset, key.size() - offset);
        weights[tagId] = value;
    }
    for (auto edge : edges_) {
        std::cout << std::setw(20) << std::left << edge << std::setw(10) << std::left
                  << counts[std::to_string(edge)] << std::setw(30) << std::left
                  << weights[std::to_string(edge)] << std::endl;
    }
    for (auto tag : tags_) {
        std::cout << std::setw(20) << std::left << tag << std::setw(10) << std::left
                  << counts[std::to_string(tag)] << std::setw(30) << std::left
                  << weights[std::to_string(tag)] << std::endl;
    }
}

void StatisticDumper::splitString(const std::string& s,
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

}   // namespace storage
}   // namespace nebula
