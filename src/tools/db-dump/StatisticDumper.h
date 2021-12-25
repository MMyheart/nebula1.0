/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef TOOLS_STATISTICDUMPER_H_
#define TOOLS_STATISTICDUMPER_H_

#include <rocksdb/db.h>
#include "base/Base.h"
#include "base/Status.h"
#include "dataman/RowReader.h"
#include "kvstore/RocksEngine.h"
#include "meta/ServerBasedSchemaManager.h"
#include "meta/client/MetaClient.h"

DECLARE_string(space);
DECLARE_string(db_path);
DECLARE_string(vids);
DECLARE_string(tags);
DECLARE_string(edges);
DECLARE_int64(limit);

namespace nebula {
namespace storage {
class StatisticDumper {
public:
    StatisticDumper() = default;

    ~StatisticDumper() = default;

    Status init();

    void run();

private:
    Status openDb();

    void scanTag();
    void scanEdge();
    void scanCountAndSummaryWeights();

    void splitString(const std::string& s, std::vector<std::string>& v, const std::string& c);

private:
    std::unique_ptr<rocksdb::DB> db_;
    rocksdb::Options options_;
    GraphSpaceID spaceId_;
    std::unordered_set<VertexID> vids_;
    std::unordered_set<TagID> tags_;
    std::unordered_set<EdgeType> edges_;

    int64_t count_{0};
};
}   // namespace storage
}   // namespace nebula
#endif   // TOOLS_STATISTICDUMPER_H_
