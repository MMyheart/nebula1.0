/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_LISTSAMPLESTATUSPROCESSOR_H
#define META_LISTSAMPLESTATUSPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class ListSampleStatusProcessor : public BaseProcessor<cpp2::ListSampleStatusResp> {
public:
    static ListSampleStatusProcessor* instance(kvstore::KVStore* kvstore) {
        return new ListSampleStatusProcessor(kvstore);
    }

    void process(const cpp2::ListSampleStatusReq& req);

private:
    explicit ListSampleStatusProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::ListSampleStatusResp>(kvstore) {}

    void remove(GraphSpaceID spaceId);
};

}   // namespace meta
}   // namespace nebula

#endif   // META_LISTSAMPLESTATUSPROCESSOR_H
