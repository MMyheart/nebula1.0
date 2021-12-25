/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "ConnectionPool.h"

namespace nebula {
namespace graph {

static thread_local std::default_random_engine e(time(0));
static thread_local std::uniform_real_distribution<double> u(0., 1.);

// static
ConnectionPool &ConnectionPool::instance() {
    static ConnectionPool instance;
    return instance;
}

ConnectionPool::~ConnectionPool() {
    threads_.clear();
}

bool ConnectionPool::init(const ConnectionInfo &addrInfo) {
    if (addrInfo.addrs.empty()) {
        LOG(ERROR) << "Input empty server addr";
        return false;
    }
    std::lock_guard<std::mutex> g(lock_);
    if (hasInit_) {
        LOG(WARNING) << "ConnectionPool has init, couldn’t init again";
        return false;
    }
    minConnectionNum_ = addrInfo.minConnectionNum;
    if (minConnectionNum_ == 0) {
        LOG(ERROR) << "min connection num can not be zero";
        return false;
    }
    maxConnectionNum_ = addrInfo.maxConnectionNum;
    for (auto &item : addrInfo.addrs) {
        addrs_.emplace_back(item);
        addrConnectionNums_[item] = 0;
        for (auto i = 0u; i < minConnectionNum_; ++i) {
            if (!addConnection(item)) {
                return false;
            }
        }
    }
    hasInit_ = true;
    return true;
}

ConnectionThread *ConnectionPool::getConnection(int32_t &indexId) {
    std::lock_guard<std::mutex> g(lock_);
    if (unusedIds_.empty()) {
        std::vector<std::pair<std::string, uint32_t>> addrs(addrs_);
        int addNum = minConnectionNum_;
        while (addNum > 0 && addrs.size() > 0) {
            int index = floor(addrs.size() * u(e));
            if (addConnection(addrs[index])) {
                addrConnectionNums_[addrs[index]]++;
                if (addrConnectionNums_[addrs[index]] == maxConnectionNum_) {
                    for (size_t i = 0; i < addrs_.size(); i++) {
                        if (addrs_[i] == addrs[index]) {
                            addrs_[i] = addrs_[addrs_.size() - 1];
                            addrs_.pop_back();
                        }
                    }
                }
                addNum--;
                continue;
            }
            addrs[index] = addrs[addrs.size() - 1];
            addrs.pop_back();
        }
    }
    if (unusedIds_.empty()) {
        LOG(ERROR) << "No idle connections";
        return nullptr;
    }
    int32_t index = floor(unusedIds_.size() * u(e));
    indexId = unusedIds_[index];
    unusedIds_[index] = unusedIds_[unusedIds_.size() - 1];
    unusedIds_.pop_back();
    return threads_[indexId].get();
}

void ConnectionPool::returnConnection(int32_t indexId) {
    std::lock_guard<std::mutex> g(lock_);
    unusedIds_.emplace_back(indexId);
}

void ConnectionPool::destoryConnection(int32_t indexId) {
    std::lock_guard<std::mutex> g(lock_);
    auto addr = threads_[indexId]->getAddrAndPort();
    threads_.erase(indexId);
    if (addrConnectionNums_[addr] == maxConnectionNum_) {
        addrs_.emplace_back(addr);
    }
    addrConnectionNums_[addr]--;
}

bool ConnectionPool::addConnection(const std::pair<std::string, uint32_t> &addr) {
    auto connection = std::make_unique<ConnectionThread>(addr.first, addr.second, timeout_);
    auto ret = connection->createConnection();
    if (!ret) {
        LOG(ERROR) << "Connect to " << addr.first << ":" << addr.second << " failed";
        return false;
    }
    auto status = connection->authenticate("user", "password").get();
    if (!status.ok()) {
        LOG(ERROR) << "authenticate to " << addr.first << ":" << addr.second << " failed";
        return false;
    }
    unusedIds_.emplace_back(maxId_);
    threads_.emplace(maxId_, std::move(connection));
    maxId_++;
    addrConnectionNums_[addr]++;
    return true;
}

}   // namespace graph
}   // namespace nebula
