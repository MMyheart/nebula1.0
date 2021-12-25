/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "sample/AliasMethod.h"
#include "rand/random.h"

namespace nebula {

void AliasMethod::init(const std::vector<double>& weights) {
    prob_.resize(weights.size());
    alias_.resize(weights.size());
    std::vector<int64_t> small, large;
    std::vector<double> weights_(weights);
    double avg = 1 / static_cast<double>(weights_.size());
    for (size_t i = 0; i < weights_.size(); i++) {
        if (weights_[i] > avg) {
            large.push_back(i);
        } else {
            small.push_back(i);
        }
    }

    int64_t less, more;
    while (large.size() > 0 && small.size() > 0) {
        less = small.back();
        small.pop_back();
        more = large.back();
        large.pop_back();
        prob_[less] = weights_[less] * weights_.size();
        alias_[less] = more;
        weights_[more] = weights_[more] + weights_[less] - avg;
        if (weights_[more] > avg) {
            large.push_back(more);
        } else {
            small.push_back(more);
        }
    }   // while (large.size() > 0 && small.size() > 0)
    while (small.size() > 0) {
        less = small.back();
        small.pop_back();
        prob_[less] = 1.0;
    }

    while (large.size() > 0) {
        more = large.back();
        large.pop_back();
        prob_[more] = 1.0;
    }
}   // Init

int64_t AliasMethod::next() const {
    int64_t column = nebula::NextLong(prob_.size());
    bool coinToss = nebula::ThreadLocalRandom() < prob_[column];
    return coinToss ? column : alias_[column];
}

}   // namespace nebula
