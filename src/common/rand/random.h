/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef NEBULA_GRAPH_RANDOM_H
#define NEBULA_GRAPH_RANDOM_H

#include "base/Base.h"

namespace nebula {

double ThreadLocalRandom();

int32_t NextInt(int32_t n);

int64_t NextLong(int64_t n);

}   // namespace nebula

#endif   // NEBULA_GRAPH_RANDOM_H
