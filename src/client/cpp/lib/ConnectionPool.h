/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CLIENT_CPP_LIB_CONNECTIONPOOL_H
#define CLIENT_CPP_LIB_CONNECTIONPOOL_H

#include "ConnectionThread.h"
#include "client/cpp/include/nebula/ExecutionResponse.h"

namespace nebula {
namespace graph {

class ConnectionPool final {
public:
    // static
    // Returns a singleton instance
    static ConnectionPool &instance();

    ~ConnectionPool();

    bool init(const ConnectionInfo &addrInfo);

    // If there has not idle socket, it will return nullptr
    // TODO: support to block when there has no idle socket
    ConnectionThread *getConnection(int32_t &indexId);

    void returnConnection(int32_t indexId);
    void destoryConnection(int32_t indexId);

    bool addConnection(const std::pair<std::string, uint32_t> &addr);

    // TODO: add reconnect handle
private:
    ConnectionPool() = default;

private:
    std::unordered_map<int32_t, std::unique_ptr<ConnectionThread>> threads_;
    std::vector<int32_t> unusedIds_;
    std::mutex lock_;
    bool hasInit_{false};
    int32_t maxId_{0};
    std::unordered_map<std::pair<std::string, uint32_t>, uint32_t> addrConnectionNums_;
    std::vector<std::pair<std::string, uint32_t>> addrs_;
    uint32_t minConnectionNum_;
    uint32_t maxConnectionNum_;
    int32_t timeout_;
};

}   // namespace graph
}   // namespace nebula
#endif   // CLIENT_CPP_LIB_CONNECTIONPOOL_H
