/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "sample/WeightedCollection.h"
#include "rand/random.h"

namespace nebula {

template class FastWeightedCollection<VertexID>;
template class FastWeightedCollection<std::tuple<VertexID, VertexID, EdgeType>>;

template class CompactWeightedCollection<TagID>;

template <class T>
bool FastWeightedCollection<T>::init(const std::vector<T>& ids,
                                     const std::vector<double>& weights) {
    if (ids.size() != weights.size()) {
        return false;
    }
    ids_.resize(ids.size());
    weights_.resize(weights.size());
    sum_weight_ = 0.0;
    for (size_t i = 0; i < weights.size(); i++) {
        sum_weight_ += weights[i];
        ids_[i] = ids[i];
        weights_[i] = weights[i];
    }
    std::vector<double> norm_weights(weights);
    for (size_t i = 0; i < norm_weights.size(); i++) {
        norm_weights[i] /= sum_weight_;
    }
    alias_.init(norm_weights);
    return true;
}

template <class T>
bool FastWeightedCollection<T>::init(const std::vector<std::pair<T, double>>& id_weight_pairs) {
    ids_.resize(id_weight_pairs.size());
    weights_.resize(id_weight_pairs.size());
    sum_weight_ = 0.0;
    for (size_t i = 0; i < id_weight_pairs.size(); i++) {
        sum_weight_ += id_weight_pairs[i].second;
        ids_[i] = id_weight_pairs[i].first;
        weights_[i] = id_weight_pairs[i].second;
    }
    std::vector<double> norm_weights(weights_);
    for (size_t i = 0; i < norm_weights.size(); i++) {
        norm_weights[i] /= sum_weight_;
    }
    alias_.init(norm_weights);
    return true;
}

template <class T>
std::pair<T, double> FastWeightedCollection<T>::sample() const {
    int64_t column = alias_.next();
    std::pair<T, double> id_weight_pair(ids_[column], weights_[column]);
    return id_weight_pair;
}

template <class T>
size_t FastWeightedCollection<T>::getSize() const {
    return ids_.size();
}

template <class T>
size_t RandomSelect(const std::vector<double>& sum_weights, size_t begin_pos, size_t end_pos) {
    double limit_begin = begin_pos == 0 ? 0 : sum_weights[begin_pos - 1];
    double limit_end = sum_weights[end_pos];
    double r = nebula::ThreadLocalRandom() * (limit_end - limit_begin) + limit_begin;
    size_t low = begin_pos, high = end_pos, mid = 0;
    bool finish = false;
    while (low <= high && !finish) {
        mid = (low + high) / 2;
        double interval_begin = mid == 0 ? 0 : sum_weights[mid - 1];
        double interval_end = sum_weights[mid];
        if (interval_begin <= r && r < interval_end) {
            finish = true;
        } else if (interval_begin > r) {
            high = mid - 1;
        } else if (interval_end <= r) {
            low = mid + 1;
        }
    }
    return mid;
}

template <class T>
bool CompactWeightedCollection<T>::init(const std::vector<T>& ids,
                                        const std::vector<double>& weights) {
    if (ids.size() != weights.size()) {
        return false;
    }
    sum_weight_ = 0.0;
    ids_.resize(ids.size());
    sum_weights_.resize(weights.size());
    for (size_t i = 0; i < ids.size(); ++i) {
        ids_[i] = ids[i];
        sum_weight_ += weights[i];
        sum_weights_[i] = sum_weight_;
    }
    return true;
}

template <class T>
bool CompactWeightedCollection<T>::init(const std::vector<std::pair<T, double>>& id_weight_pairs) {
    sum_weight_ = 0.0;
    ids_.resize(id_weight_pairs.size());
    sum_weights_.resize(id_weight_pairs.size());
    for (size_t i = 0; i < id_weight_pairs.size(); ++i) {
        ids_[i] = id_weight_pairs[i].first;
        sum_weight_ += id_weight_pairs[i].second;
        sum_weights_[i] = sum_weight_;
    }
    return true;
}

template <class T>
std::pair<T, double> CompactWeightedCollection<T>::sample() const {
    size_t mid = RandomSelect<T>(sum_weights_, 0, ids_.size() - 1);
    double pre_sum_weight = 0;
    if (mid > 0) {
        pre_sum_weight = sum_weights_[mid - 1];
    }
    std::pair<T, double> id_weight_pair(ids_[mid], sum_weights_[mid] - pre_sum_weight);
    return id_weight_pair;
}

template <class T>
size_t CompactWeightedCollection<T>::getSize() const {
    return ids_.size();
}

}   // namespace nebula
