/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CLIENT_CPP_LIB_NEBULACLIENTIMPL_H_
#define CLIENT_CPP_LIB_NEBULACLIENTIMPL_H_

#include <string>
#include "ConnectionThread.h"
#include "client/cpp/include/nebula/ExecutionResponse.h"
#include "gen-cpp2/GraphServiceAsyncClient.h"

namespace nebula {
namespace graph {

using CallbackFun = std::function<void(ExecutionResponse*, ErrorCode)>;
class NebulaClientImpl final {
public:
    NebulaClientImpl();
    ~NebulaClientImpl();

    // must be call on the front of the main()
    static void initEnv(int argc, char* argv[]);
    static void initConnectionPool(const ConnectionInfo& addrInfo);

    void signout();

    cpp2::ErrorCode execute(folly::StringPiece stmt, cpp2::ExecutionResponse& resp);

    ErrorCode doExecute(std::string stmt, ExecutionResponse& resp);

private:
    void feedPath(const cpp2::Path& inPath, Path& outPath);

    void feedRows(const cpp2::ExecutionResponse& inResp, ExecutionResponse& outResp);

private:
    ConnectionThread* connection_{nullptr};
    int32_t indexId_{-1};
    std::string addr_;
    uint32_t port_;
};
}   // namespace graph
}   // namespace nebula
#endif   // CLIENT_CPP_LIB_NEBULACLIENTIMPL_H_
