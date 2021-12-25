/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "rand/random.h"
#include "base/Base.h"

namespace nebula {

namespace {
static thread_local std::default_random_engine e(time(0));
static thread_local std::uniform_real_distribution<double> u(0., 1.);
}   // namespace

double ThreadLocalRandom() {
    return u(e);
}

int32_t NextInt(int32_t n) {
    return floor(n * ThreadLocalRandom());
}

int64_t NextLong(int64_t n) {
    return floor(n * ThreadLocalRandom());
}

}   // namespace nebula
