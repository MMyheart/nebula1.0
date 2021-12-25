/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef NEBULA_GRAPH_ALIASMETHOD_H
#define NEBULA_GRAPH_ALIASMETHOD_H

#include "base/Base.h"

namespace nebula {

class AliasMethod {
public:
    void init(const std::vector<double>& weights);

    int64_t next() const;

private:
    std::vector<double> prob_;
    std::vector<int64_t> alias_;
};

}   // namespace nebula

#endif   // NEBULA_GRAPH_ALIASMETHOD_H
