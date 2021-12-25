/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef NEBULA_GRAPH_WEIGHTEDCOLLECTION_H
#define NEBULA_GRAPH_WEIGHTEDCOLLECTION_H

#include "base/Base.h"
#include "sample/AliasMethod.h"

namespace nebula {

template<class T>
class FastWeightedCollection {
public:
    bool init(const std::vector<T>& ids,
              const std::vector<double>& weights);

    bool init(const std::vector<std::pair<T, double>>& id_weight_pairs);

    std::pair<T, double> sample() const;

    size_t getSize() const;
private:
    std::vector<T> ids_;
    std::vector<double> weights_;
    AliasMethod alias_;
    double sum_weight_;
};

template<class T>
class CompactWeightedCollection {
public:
    CompactWeightedCollection() {
    }

    bool init(const std::vector<T>& ids,
              const std::vector<double>& weights);

    bool init(const std::vector<std::pair<T, double>>& id_weight_pairs);

    std::pair<T, double> sample() const;

    size_t getSize() const;
private:
    std::vector<T> ids_;
    std::vector<double> sum_weights_;
    double sum_weight_;
};

}  // namespace nebula

#endif   // NEBULA_GRAPH_WEIGHTEDCOLLECTION_H
