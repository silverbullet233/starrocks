// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/buffer_control_block.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <butil/iobuf.h>

#include <condition_variable>
#include <deque>
#include <list>
#include <mutex>
#include <utility>

#include "common/status.h"
#include "common/statusor.h"
#include "exec/pipeline/schedule/observer.h"
#include "gen_cpp/Types_types.h"
#include "runtime/current_thread.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/query_statistics.h"
#include "util/race_detect.h"
#include "util/runtime_profile.h"

namespace arrow {
class RecordBatch;
}

namespace google::protobuf {
class Closure;
} // namespace google::protobuf

namespace brpc {
class Controller;
}

namespace butil {
class IOBuf;
}

namespace starrocks {

class TFetchDataResult;
class PFetchDataResult;

struct SerializeRes {
    butil::IOBuf attachment;
    size_t row_size;
};

struct GetResultBatchCtx {
    brpc::Controller* cntl = nullptr;
    PFetchDataResult* result = nullptr;
    google::protobuf::Closure* done = nullptr;

    GetResultBatchCtx(brpc::Controller* cntl_, PFetchDataResult* result_, google::protobuf::Closure* done_)
            : cntl(cntl_), result(result_), done(done_) {}

    void on_failure(const Status& status);
    void on_close(int64_t packet_seq, QueryStatistics* statistics = nullptr);
    void on_data(TFetchDataResult* t_result, int64_t packet_seq, bool eos = false);
    void on_data(SerializeRes* t_result, int64_t packet_seq, bool eos = false);
};

// buffer used for result customer and productor
class BufferControlBlock {
public:
    BufferControlBlock(const TUniqueId& id, int buffer_size);
    ~BufferControlBlock();

    Status init();
    // In order not to affect the current implementation of the non-pipeline engine,
    // this method is reserved and is only used in the non-pipeline engine
    Status add_batch(TFetchDataResult* result, bool need_free = true);
    Status add_batch(std::unique_ptr<TFetchDataResult>& result);
    Status add_arrow_batch(std::shared_ptr<arrow::RecordBatch>& result);

    // non-blocking version of add_batch
    Status add_to_result_buffer(std::vector<std::unique_ptr<TFetchDataResult>>&& results);
    bool is_full() const;
    // cancel all pending rpc. this is called from pipeline->cancelled
    void cancel_pending_rpc();

    // get result from batch, use timeout?
    Status get_batch(TFetchDataResult* result);

    void get_batch(GetResultBatchCtx* ctx);
    Status get_arrow_batch(std::shared_ptr<arrow::RecordBatch>* result);

    // close buffer block, set _status to exec_status and set _is_close to true;
    // called because data has been read or error happened.
    Status close(Status exec_status);
    // this is called by RPC, called from coordinator
    void cancel();

    const TUniqueId& fragment_id() const { return _fragment_id; }

    void set_query_statistics(std::shared_ptr<QueryStatistics> statistics) {
        _query_statistics = std::move(statistics);
    }

    void update_num_written_rows(int64_t num_rows) {
        // _query_statistics may be null when the result sink init failed
        // or some other failure.
        // and the number of written rows is only needed when all things go well.
        if (_query_statistics != nullptr) {
            _query_statistics->set_returned_rows(num_rows);
        }
    }

    void attach_observer(RuntimeState* state, pipeline::PipelineObserver* observer) {
        _observable.add_observer(state, observer);
    }

    auto defer_notify() {
        return DeferOp([query_ctx = _query_ctx, this]() {
            if (auto ctx = query_ctx.lock()) {
                this->_observable.notify_source_observers();
                CHECK(tls_thread_status.mem_tracker() == GlobalEnv::GetInstance()->process_mem_tracker());
            }
        });
    }

    void attach_query_ctx(const std::shared_ptr<pipeline::QueryContext>& query_ctx) {
        if (_query_ctx.use_count() == 0) {
            _query_ctx = query_ctx;
        }
    }

private:
    void _process_batch_without_lock(std::unique_ptr<SerializeRes>& result);

    void _process_arrow_batch_without_lock(std::shared_ptr<arrow::RecordBatch>& result);

    StatusOr<std::unique_ptr<SerializeRes>> _serialize_result(TFetchDataResult*);

    // as no idea of whether sending sorted results, can't use concurrentQueue here.
    typedef std::list<std::unique_ptr<SerializeRes>> ResultQueue;
    typedef std::list<std::shared_ptr<arrow::RecordBatch>> ArrowResultQueue;

    // result's query id
    TUniqueId _fragment_id;
    std::atomic_bool _is_close;
    std::atomic_bool _is_cancelled;
    Status _status;
    std::atomic_int64_t _buffer_bytes;
    int _buffer_limit;
    std::atomic<int64_t> _packet_num;
    int _arrow_rows_limit;
    int _arrow_rows;

    // blocking queue for batch
    ResultQueue _batch_queue;
    ArrowResultQueue _arrow_batch_queue;

    // protects all subsequent data in this block
    mutable std::mutex _lock;
    // signal arrival of new batch or the eos/cancelled condition
    std::condition_variable _data_arriaval;
    // signal removal of data by stream consumer
    std::condition_variable _data_removal;

    std::deque<GetResultBatchCtx*> _waiting_rpc;

    // It is shared with PlanFragmentExecutor and will be called in two different
    // threads. But their calls are all at different time, there is no problem of
    // multithreaded access.
    std::shared_ptr<QueryStatistics> _query_statistics;
    static const size_t _max_memory_usage = 1UL << 28; // 256MB

    std::weak_ptr<pipeline::QueryContext> _query_ctx;
    pipeline::Observable _observable;
};

} // namespace starrocks
