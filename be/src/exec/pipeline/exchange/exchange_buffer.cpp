#include "exec/pipeline/exchange/exchange_buffer.h"

#include "util/uid_util.h"
#include "util/disposable_closure.h"
#include "common/logging.h"
#include "common/config.h"
#include "util/defer_op.h"
#include "util/priority_thread_pool.hpp"
#include "brpc/periodic_task.h"

namespace starrocks::pipeline {

class ScheduleTask: public brpc::PeriodicTask {
public:
    ScheduleTask(std::shared_ptr<ExchangeBuffer> buffer): _buffer(buffer) {}

    ~ScheduleTask() override {}

    bool OnTriggeringTask(timespec* next_abstime) override {
        _buffer->_schedule_count++;
        SCOPED_RAW_TIMER(&(_buffer->_schedule_time));
        while (true) {
            bool has_new_response = _buffer->process_rpc_result();
            bool has_sent_new_request = _buffer->try_to_send_rpc();
            if (_buffer->is_finished()) {
                LOG(INFO) << _buffer->log_prefix() << " is finished, stop schedule";
                return false;
            }
            if (!has_new_response && !has_sent_new_request) {
                *next_abstime = butil::milliseconds_from_now(1);
                _buffer->_rewardless_schedule_count++;
                return true;
            }
        }
        // _buffer->process_rpc_result();
        // _buffer->try_to_send_rpc();
        // if (_buffer->is_finished()) {
        //     // LOG(INFO) << _buffer->log_prefix() << " is finished, stop schedule";
        //     return false;
        // }
        *next_abstime = butil::milliseconds_from_now(1);
        return true;
    }

    void OnDestroyingTask() override {
        _buffer.reset();
        delete this;
    }
private:
    std::shared_ptr<ExchangeBuffer> _buffer;
};

ExchangeBuffer::ExchangeBuffer(MultiExchangeBuffer* parent, TUniqueId instance_id, int32_t num_writers)
    : _parent(parent),
      _instance_id(instance_id), 
      _buffer(kBufferSize),
      _available_flags(kBufferSize),
      _results(kBufferSize),
      _finish_flags(kBufferSize),
      _num_remaining_eos(num_writers),
      _num_uncancelled_sinkers(parent->_exchange_sink_dop) {
        //   LOG(INFO) << "new ExchangeBuffer for instance id: " << print_id(instance_id);
        _finst_id.set_hi(instance_id.hi);
        _finst_id.set_lo(instance_id.lo);
        for (size_t i = 0;i < kBufferSize;i++) {
            _finish_flags[i].store(false);
            _available_flags[i].store(false);
        }
        _log_prefix = log_prefix();
        // LOG(INFO) << log_prefix() << "New ExchangeBuffer, exchange_sink_dop: " << parent->_exchange_sink_dop << ", num_writers: " << _num_remaining_eos;
    }


void ExchangeBuffer::add_request(TransmitChunkInfo& request) {
    DCHECK(_num_remaining_eos > 0);
    if (_is_finishing) {
        return;
    }
    if (request.params->eos()) {
        // LOG(INFO) << log_prefix() << "receive eos request, remainning_eos: " << _num_remaining_eos;
        // if have chunk, add into buffer, else return
        bool should_reserve = false;
        if (request.params->chunks_size() > 0) {
            request.params->set_eos(false);
            should_reserve = true;
        } 
        if (--_num_remaining_eos > 0) {
            // only reserve the last eos
            if (!should_reserve) {
                return;
            }
        } else {
            request.params->set_eos(true);
        }
        // LOG(INFO) << log_prefix() << "add the last eos request with chunk_size: " << request.params->chunks_size();
    }
    if (!request.attachment.empty()) {
        _bytes_enqueued += request.attachment.size();
        _requests_enqueued++;
    }
    int64_t current_seqs = ++_last_arrived_seqs;
    // DCHECK_LT(current_seqs - _last_acked_seqs, kBufferSize);
    DCHECK_LT(current_seqs - _last_in_flight_seqs, kBufferSize);
    int index = current_seqs % kBufferSize;
    // seqs allocation and buffer assignment are not atomic, use _available_flags to solve it
    _buffer[index] = request;
    DCHECK_EQ(_available_flags[index], false);
    _available_flags[index].store(true);
    // LOG(INFO) << log_prefix() << "add new request, seq: " << current_seqs;
}

bool ExchangeBuffer::is_full() {
    // @TODO maybe we can use _last_arrived_seqs - _last_in_flight_seqs as size
    // int32_t size = _last_arrived_seqs - _last_acked_seqs;
    int32_t size = _last_arrived_seqs - _last_in_flight_seqs;
    // @TODO this may not be safe, it depends on chunk size and max_transmit_batched_bytes?
    return size + _parent->_exchange_sink_dop + 1 > kBufferSize;
}

void ExchangeBuffer::set_finishing() {
    // LOG(INFO) << log_prefix() << "invoke set_finishing";
}

bool ExchangeBuffer::is_finished() {
    return _is_finishing;
    // if (!_is_finishing) {
    //     return false;
    // }
    // // LOG_EVERY_N(INFO, 1000) << log_prefix() << "_num_in_flight_rpcs: " << _num_in_flight_rpcs
    // //     << ", _last_acked_seqs: " << _last_acked_seqs << ", _last_in_flight_seqs: " << _last_in_flight_seqs << ", _last_arrived_seqs: " << _last_arrived_seqs;
    // return _num_in_flight_rpcs == 0 && _last_acked_seqs == _last_in_flight_seqs && _last_acked_seqs == _last_arrived_seqs;
}

void ExchangeBuffer::log_seqs() {
    LOG(INFO) << log_prefix() << "_num_in_flight_rpcs: " << _num_in_flight_rpcs
        << ", _last_acked_seqs: " << _last_acked_seqs << ", _last_in_flight_seqs: " << _last_in_flight_seqs << ", _last_arrived_seqs: " << _last_arrived_seqs;
}

void ExchangeBuffer::cancel_one_sinker() {
    if (--_num_uncancelled_sinkers == 0) {
        _is_finishing = true;
        // LOG(INFO) << log_prefix() << "set finishing due to cancel sinker";
        // log_seqs();
    }
}

bool ExchangeBuffer::is_concurreny_exceed_limit() {
    // @TODO do we really need this?
    return false;
    // if (_parent->_is_dest_merge) {
    //     int64_t discontinuous_acked_window_size = _last_in_flight_seqs - _last_acked_seqs;
    //     return discontinuous_acked_window_size >= config::pipeline_sink_brpc_dop;
    // } else {
    //     return _num_in_flight_rpcs >= config::pipeline_sink_brpc_dop;
    // }
}

bool ExchangeBuffer::try_to_send_rpc() {
    // try to send request in (_last_in_flight_seqs, _last_arrived_seqs]
    int64_t last_in_flight_seqs = _last_in_flight_seqs;
    int64_t last_arrived_seqs = _last_arrived_seqs;
    // we should make sure _results won't overlap
    // int64_t target_seqs = last_arrived_seqs;
    int64_t target_seqs = std::min(last_arrived_seqs, _last_acked_seqs + kBufferSize);
    bool has_sent_new_request = false;
    // max send rpcs depends on kBufferSize
    for (int64_t seq = last_in_flight_seqs + 1; seq <= target_seqs; seq ++) {
        if (_is_finishing) {
            return has_sent_new_request;
        }
        // if (is_concurreny_exceed_limit()) {
            // LOG(INFO) << log_prefix() << "concurrency exceeds limit, current in flight rpcs: " << _num_in_flight_rpcs;
            // return;
        // }
        // LOG(INFO) << log_prefix() << "try to send rpc for seq: " << seq;
        int index = seq % kBufferSize;
        // wait until buffer is avaiable
        while (!_available_flags[index]) {
            // LOG_EVERY_N(INFO, 1000) << "buffer is not available, seq: " << seq;
        }
        // if (!_available_flags[index]) {
        //     LOG(INFO) << log_prefix() << " buffer is not available, return and wait next schedule";
        //     return;
        // }
        auto request = _buffer[index];
        // The order of data transmiting in IO level may not be strictly the same as
        // the order of submitting data packets
        // But we must guarantee that first packet must be received first
        if (_last_acked_seqs == -1 && _num_in_flight_rpcs > 0) {
            // LOG(INFO) << log_prefix() << "the first packed is not received";
            return false;
        }
        if (request.params->eos()) {
            if (_num_in_flight_rpcs > 0) {
                // The order of data transmiting in IO level may not be strictly the same as
                // the order of submitting data packets
                // But we must guarantee that eos packent must be the last packet
                return has_sent_new_request;
            }
            // _is_finishing should set in rpc callback?
            // _is_finishing = true;
        }

        *request.params->mutable_finst_id() = _finst_id;
        request.params->set_sequence(seq);
        if (!request.attachment.empty()) {
            _bytes_sent += request.attachment.size();
            _requests_sent++;
        }

        auto* closure = new DisposableClosure<PTransmitChunkResult, ExchangeBufferClosureContext>(
            {request.params->sequence(), GetCurrentTimeNanos(), request.params->eos()});

        closure->addFailedHandler([this] (const ExchangeBufferClosureContext& ctx) noexcept {
            --_num_in_flight_rpcs;
            _is_finishing = true;
            std::string err_msg = fmt::format("transmit chunk rpc failed: {}", print_id(_instance_id));
            _parent->_fragment_ctx->cancel(Status::InternalError(err_msg));
            LOG(WARNING) << err_msg;
        });
        closure->addSuccessHandler([this]  (const ExchangeBufferClosureContext& ctx, const PTransmitChunkResult& result) noexcept {
            --_num_in_flight_rpcs;
            Status status(result.status());
            if (!status.ok()) {
                _is_finishing = true;
                _parent->_fragment_ctx->cancel(result.status());
                LOG(WARNING) << fmt::format("transmit chunk rpc failed:{}, msg:{}", print_id(_instance_id), status.message());
            } else {
                // set result
                int64_t seq = ctx.sequence;
                int index = seq % ExchangeBuffer::kBufferSize;
                _results[index].send_timestamp = ctx.send_timestamp;
                _results[index].received_timestamp = result.receive_timestamp();
                _finish_flags[index].store(true);
                if (ctx.is_eos) {
                    _is_finishing = true;
                    // LOG(INFO) << log_prefix() << "set finishing due to eos request sent successfully";
                }
                // LOG(INFO) << log_prefix() << "set rpc result for seq: " << seq;
            }
        });

        ++_num_in_flight_rpcs;
        closure->cntl.Reset();
        closure->cntl.set_timeout_ms(_parent->_brpc_timeout_ms);
        closure->cntl.request_attachment().append(request.attachment);
        request.brpc_stub->transmit_chunk(&closure->cntl, request.params.get(), &closure->result, closure);
        // after rpc is sent, the item in buffer is no longer meanningful
        _available_flags[index].store(false);
        _buffer[index].attachment.clear();
        _last_in_flight_seqs++;
        has_sent_new_request = true;
        // LOG(INFO) << log_prefix() << "update _last_in_flight_seqs to " << _last_in_flight_seqs;
    }
    return has_sent_new_request;
}

void ExchangeBuffer::process_send_window(int64_t sequence) {
    if (!_parent->_is_dest_merge) {
        return;
    }
    auto& seqs = _discontinuous_acked_seqs;
    seqs.insert(sequence);
    std::unordered_set<int64_t>::iterator it;
    while ((it = seqs.find(_max_continuous_acked_seqs + 1)) != seqs.end()) {
        seqs.erase(it);
        ++_max_continuous_acked_seqs;
    }
}

bool ExchangeBuffer::process_rpc_result() {
    // iterate result in (_last_acked_seqs, _last_in_flight_seqs]
    // only one thread execute it, no other threads will update
    int64_t last_acked_seqs = _last_acked_seqs;
    int64_t last_in_flight_seqs = _last_in_flight_seqs;
    for (int64_t seq = last_acked_seqs + 1; seq <= last_in_flight_seqs;seq ++) {
        int index = seq % kBufferSize;
        if (_finish_flags[index].load()) {
            _last_acked_seqs++;
            // LOG(INFO) << log_prefix() << " update _last_acked_seqs to " << _last_acked_seqs;
            // process_send_window(seq);
            auto& result = _results[index];
            update_network_time(result.send_timestamp, result.received_timestamp);
            // _buffer[index].attachment.clear();
            _finish_flags[index].store(false);
        } else {
            break;
        }
    }
    return _last_acked_seqs > last_acked_seqs;
}

std::string ExchangeBuffer::log_prefix() {
    TUniqueId tmp;
    tmp.hi = _parent->_fragment_ctx->fragment_instance_id().lo;
    tmp.lo = _instance_id.lo;
    return "ExchangeBuffer(" + print_id(tmp)+ ") ";
}

void ExchangeBuffer::update_network_time(const int64_t send_timestamp, const int64_t receive_timestamp) {
    int32_t concurrency = _num_in_flight_rpcs;
    _network_time.update(receive_timestamp - send_timestamp, concurrency);
}

MultiExchangeBuffer::MultiExchangeBuffer(FragmentContext* fragment_ctx, const std::vector<TPlanFragmentDestination>& destinations, bool is_dest_merge, size_t num_sinkers)
    : _fragment_ctx(fragment_ctx),
      _mem_tracker(fragment_ctx->runtime_state()->instance_mem_tracker()),
      _exchange_sink_dop(num_sinkers),
      _is_dest_merge(is_dest_merge),
      _brpc_timeout_ms(std::min(3600, fragment_ctx->runtime_state()->query_options().query_timeout) * 1000) {
          // calculate _num_writers, this must be before create ExchangeBuffer
          phmap::flat_hash_map<int64_t, int32_t> num_writers;
          for (const auto& dest : destinations) {
              const auto& instance_id = dest.fragment_instance_id;
              if (instance_id.lo == -1) {
                  continue;
              }
              auto it = num_writers.find(instance_id.lo);
              if (it != num_writers.end()) {
                  it->second += num_sinkers;
              } else {
                  num_writers[instance_id.lo] = num_sinkers;
              }
          }
          for (const auto& dest : destinations) {
              const auto& instance_id = dest.fragment_instance_id;
              // instance_id.lo == -1 indicates that the destination is pseudo for bucket shuffle join.
              if (instance_id.lo == -1) {
                  continue;
              }
              if (_buffers.find(instance_id.lo) == _buffers.end()) {
                  auto exchange_buffer = std::make_shared<ExchangeBuffer>(this, instance_id, num_writers.at(instance_id.lo));
                  _buffers.insert({instance_id.lo, exchange_buffer});
              }
          }
          // submit schedule task
        for (auto& [_, buffer]: _buffers) {
            brpc::PeriodicTaskManager::StartTaskAt(new ScheduleTask(buffer), butil::microseconds_from_now(1));
        }
      }

MultiExchangeBuffer::~MultiExchangeBuffer() {}

Status MultiExchangeBuffer::prepare() {
    return Status::OK();
}

void MultiExchangeBuffer::add_request(TransmitChunkInfo& request) {
    auto& buffer = _buffers.at(request.fragment_instance_id.lo);
    buffer->add_request(request);
}

bool MultiExchangeBuffer::is_full() {
    bool is_full = false;
    for (auto& [_, buffer]: _buffers) {
        if (buffer->is_full()) {
            // LOG(INFO) << "MultiExchangeBuffer is full";
            is_full = true;
            break;
        }
    }

    int64_t last_full_timestamp = _last_full_timestamp;
    int64_t full_time = _full_time;

    if (is_full && last_full_timestamp == -1) {
        _last_full_timestamp.compare_exchange_weak(last_full_timestamp, MonotonicNanos());
    }
    if (!is_full && last_full_timestamp != -1) {
        // The following two update operations cannot guarantee atomicity as a whole without lock
        // But we can accept bias in estimatation
        _full_time.compare_exchange_weak(full_time, full_time + (MonotonicNanos() - last_full_timestamp));
        _last_full_timestamp.compare_exchange_weak(last_full_timestamp, -1);
    }

    return is_full;
}

void MultiExchangeBuffer::set_finishing() {
    int64_t expected = -1;
    if (_pending_timestamp.compare_exchange_strong(expected, MonotonicNanos())) {
        return;
    }
    for (auto& [_, buffer]: _buffers) {
        buffer->set_finishing();
    }
}

bool MultiExchangeBuffer::is_finished() {
    for (auto& [_, buffer]: _buffers) {
        if (!buffer->is_finished()) {
            return false;
        }
    }
    return true;
}

void MultiExchangeBuffer::cancel_one_sinker() {
    for (auto& [_, buffer]: _buffers) {
        buffer->cancel_one_sinker();
    }
}

void MultiExchangeBuffer::update_profile(RuntimeProfile* profile) {
    bool flag = false;
    if (!_is_profile_updated.compare_exchange_strong(flag, true)) {
        return;
    }
    // collect profile from each buffer
    auto* network_timer = ADD_TIMER(profile, "NetworkTime");
    COUNTER_SET(network_timer, network_time());
    auto* full_timer = ADD_TIMER(profile, "FullTime");
    COUNTER_SET(full_timer, _full_time);

    auto* pending_finish_timer = ADD_TIMER(profile, "PendingFinishTime");
    COUNTER_SET(pending_finish_timer, MonotonicNanos() - _pending_timestamp);

    // schedule related
    auto* schedule_counter = ADD_COUNTER(profile, "BufferScheduleTaskRunCount", TUnit::UNIT);
    auto* rewardless_schedule_counter = ADD_COUNTER(profile, "BufferScheduleTaskRewardlessRunCount", TUnit::UNIT);
    auto* schedule_timer = ADD_TIMER(profile, "BufferScheduleTaskRunTime");
    int64_t total_schedule_count = 0;
    int64_t total_rewardless_schedule_count = 0;
    int64_t total_schedule_time = 0;
    for (auto& [_, buffer]: _buffers) {
        total_schedule_count += buffer->_schedule_count;
        total_schedule_time += buffer->_schedule_time;
        total_rewardless_schedule_count += buffer->_rewardless_schedule_count;
    }
    // @TODO how to calculate?
    COUNTER_SET(schedule_counter, total_schedule_count);
    COUNTER_SET(schedule_timer, total_schedule_time);
    COUNTER_SET(rewardless_schedule_counter, total_rewardless_schedule_count);


    auto* bytes_sent_counter = ADD_COUNTER(profile, "BytesSent", TUnit::BYTES);
    auto* request_sent_counter = ADD_COUNTER(profile, "RequestSent", TUnit::UNIT);
    int64_t total_bytes_enqueued = 0;
    int64_t total_requests_enqueued = 0;
    for (auto& [_, buffer]: _buffers) {
        COUNTER_UPDATE(bytes_sent_counter, buffer->_bytes_sent);
        COUNTER_UPDATE(request_sent_counter, buffer->_requests_sent);
        total_bytes_enqueued += buffer->_bytes_enqueued;
        total_requests_enqueued += buffer->_requests_enqueued;
        if (buffer->_requests_enqueued > buffer->_requests_sent) {
            LOG(INFO) << buffer->log_prefix() << " has unsent request";
            buffer->log_seqs();
        }
    }
    if (total_bytes_enqueued > bytes_sent_counter->value()) {
        auto* bytes_unsent_counter = ADD_COUNTER(profile, "BytesUnsent", TUnit::BYTES);
        auto* request_unsent_counter = ADD_COUNTER(profile, "RequestUnsent", TUnit::UNIT);
        COUNTER_SET(bytes_unsent_counter, total_bytes_enqueued - bytes_sent_counter->value());
        COUNTER_SET(request_unsent_counter, total_requests_enqueued - request_sent_counter->value());
    }
    // @TODO add throughput counter
    profile->add_derived_counter(
            "OverallThroughput", TUnit::BYTES_PER_SECOND,
            [bytes_sent_counter, network_timer] {
                return RuntimeProfile::units_per_second(bytes_sent_counter, network_timer);
            },
            "");

}

int64_t MultiExchangeBuffer::network_time() {
    int64_t max = 0;
    for (auto& [_, buffer]: _buffers) {
        double avg_concurrency = static_cast<double>(buffer->_network_time.accumulated_concurrency / std::max(1, buffer->_network_time.times));
        int64_t avg_accumulated_time = static_cast<int64_t>(buffer->_network_time.accumulated_time / std::max(1.0, avg_concurrency));
        if (avg_accumulated_time > max) {
            max = avg_accumulated_time;
        }
    }
    return max;
}

}