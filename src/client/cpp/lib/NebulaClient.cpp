/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "client/cpp/include/nebula/NebulaClient.h"
#include "ConnectionPool.h"
#include "NebulaClientImpl.h"

namespace nebula {

void NebulaClient::init(int argc, char *argv[]) {
    graph::NebulaClientImpl::initEnv(argc, argv);
}

void NebulaClient::initConnectionPool(const ConnectionInfo &addrInfo) {
    graph::NebulaClientImpl::initConnectionPool(addrInfo);
}

NebulaClient::NebulaClient() {
    client_ = std::make_unique<graph::NebulaClientImpl>();
}

NebulaClient::~NebulaClient() {
    if (client_ != nullptr) {
        executeFinished();
    }
}

void NebulaClient::executeFinished() {
    client_->signout();
    client_ = nullptr;
}

ErrorCode NebulaClient::execute(std::string stmt, ExecutionResponse &resp) {
    return client_->doExecute(stmt, resp);
}

}   // namespace nebula
