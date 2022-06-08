#pragma once

#include <atomic>
#include <vector>
#include <unordered_set>

#include "column/chunk.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/exchange/sink_buffer.h"
#include "gen_cpp/BackendService.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_state.h"
#include "util/brpc_stub_cache.h"

namespace starrocks {
namespace pipeline {


// using PTransmitChunkParamsPtr = std::shared_ptr<PTransmitChunkParams>;

// struct TransmitChunkInfo {
//     // For BUCKET_SHUFFLE_HASH_PARTITIONED, multiple channels may be related to
//     // a same exchange source fragment instance, so we should use fragment_instance_id
//     // of the destination as the key of destination instead of channel_id.
//     TUniqueId fragment_instance_id;
//     doris::PBackendService_Stub* brpc_stub;
//     PTransmitChunkParamsPtr params;
//     butil::IOBuf attachment;

struct TransmitChunkResult {
    int64_t send_timestamp;
    int64_t received_timestamp;
};

struct ExchangeBufferClosureContext {
    int64_t sequence;
    int64_t send_timestamp;
    bool is_eos;
};

class MultiExchangeBuffer;
// an ExchangeBuffer per instance
// lock-free
class ScheduleTask;
class ExchangeBuffer {
public:
    ExchangeBuffer(MultiExchangeBuffer* parent, TUniqueId instance_id, int num_writers);

    void add_request(TransmitChunkInfo& request);
    bool is_full() ;

    void set_finishing();
    bool is_finished();

    void cancel_one_sinker();

private:
    friend class ScheduleTask;
    friend class MultiExchangeBuffer;
    bool is_concurreny_exceed_limit();

    // return true if really sent request, otherwise return false
    bool try_to_send_rpc();

    void process_send_window(int64_t sequence);

    // return true if _last_acked_seqs has moved, otherwise return false
    bool process_rpc_result();

    std::string log_prefix();

    void log_seqs();

    void update_network_time(const int64_t send_timestamp, const int64_t receive_timestamp);
    
    MultiExchangeBuffer* _parent;
    std::atomic_bool _is_finishing{false};
    TUniqueId _instance_id;
    PUniqueId _finst_id;

    // fixed size ring buffer
    std::vector<TransmitChunkInfo> _buffer;
    std::vector<std::atomic_bool> _available_flags;
    std::vector<TransmitChunkResult> _results;
    std::vector<std::atomic_bool> _finish_flags;
    std::atomic_int64_t _last_arrived_seqs{-1};
    std::atomic_int64_t _last_in_flight_seqs{-1};
    std::atomic_int64_t _last_acked_seqs{-1};

    std::atomic_int64_t _max_continuous_acked_seqs{-1};// use _last_acked_seqs ?
    // int64_t _max_continuous_acked_seqs = -1;
    std::unordered_set<int64_t> _discontinuous_acked_seqs;

    std::atomic_int32_t _num_in_flight_rpcs{0};
    std::atomic_int32_t _num_remaining_eos{0};

    // int32_t _num_sinkers = 0;
    // std::atomic_int32_t _num_sinkers;
    std::atomic<int32_t> _num_uncancelled_sinkers;

    std::string _log_prefix;
    // RuntimeProfile counters
    std::atomic_int64_t _bytes_enqueued = 0;
    std::atomic_int64_t _requests_enqueued = 0;
    std::atomic_int64_t _bytes_sent = 0;
    std::atomic_int64_t _requests_sent = 0;
    // @TODO schedule times and schedule cost and pending schedule time
    int64_t _schedule_count = 0;
    int64_t _schedule_time = 0;
    int64_t _rewardless_schedule_count = 0;
    TimeTrace _network_time;

    static const uint32_t kBufferSize = 1024;
};

using ExchangeBufferPtr = std::shared_ptr<ExchangeBuffer>;

class MultiExchangeBuffer {
public:
    MultiExchangeBuffer(FragmentContext* fragment_ctx, const std::vector<TPlanFragmentDestination>& destinations, bool is_dest_merge, size_t num_sinkers);
    ~MultiExchangeBuffer();

    Status prepare();

    void add_request(TransmitChunkInfo& request);

    bool is_full();

    void set_finishing();

    bool is_finished();

    void cancel_one_sinker();

    void update_profile(RuntimeProfile* profile);
private:
    friend class ExchangeBuffer;

    int64_t network_time();

    FragmentContext* _fragment_ctx;
    const MemTracker* _mem_tracker;
    int32_t _exchange_sink_dop;
    const bool _is_dest_merge;
    int64_t _brpc_timeout_ms;
    std::atomic_bool _is_profile_updated{false};
    // instance_id -> buffer
    phmap::flat_hash_map<int64_t, ExchangeBufferPtr> _buffers;

    // runtime profiles
    std::atomic_int64_t _full_time = 0;
    std::atomic_int64_t _last_full_timestamp = -1;
    std::atomic_int64_t _pending_timestamp = -1;


};
}
}