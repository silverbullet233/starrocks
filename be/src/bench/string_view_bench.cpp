#include <benchmark/benchmark.h>
#include <testutil/assert.h>
#include <ratio>
#include <type_traits>
#include "util/random.h"
#include "util/slice.h"
#include "util/string_view.h"
#include "util/phmap/phmap.h"

namespace starrocks {

std::random_device rd;
std::default_random_engine rnd(rd());
std::vector<std::string> short_strings;
std::vector<std::string> mid_strings;
std::vector<std::string> long_strings;

std::string rand_str(int max_length) {
    char tmp;
    std::string str;

    int len = rnd() % max_length + 1;

    for (int j = 0; j < len; j++) {
        tmp = rnd() % 26;
        tmp += 'A';
        str += tmp;
    }
    return str;
}

void output_avg_len(const std::vector<std::string>& data) {
    size_t sum = 0;
    for (const auto& s: data) {
        sum += s.size();
    }
    std::cout << "data num: " << data.size() << ", avg len: " << (sum * 1.0 / data.size()) << std::endl;
}

std::vector<std::string> gen_strings(int32_t max_length) {
    std::vector<std::string> ret;
    const int num = 1000000;
    for (int i = 0;i < num;i++) {
        ret.push_back(rand_str(max_length));
    }
    output_avg_len(ret);
    return ret;
}



static void prepare_data() {
    if (short_strings.empty()) {
        short_strings = gen_strings(12);
    }
    if (mid_strings.empty()) {
        mid_strings = gen_strings(100);
    }
    if (long_strings.empty()) {
        long_strings = gen_strings(500);
    }

}

template<class Type>
class StringViewBench {
public:
    void SetUp() {
    }
    void TearDown() {}
    StringViewBench(const std::vector<std::string>& origin_data) {
        for (const std::string& data: origin_data) {
            _strings.emplace_back(Type(data));
        }
    }

    void do_sort(benchmark::State& state) {
        state.ResumeTiming();
        std::sort(_strings.begin(), _strings.end());
        state.PauseTiming();
    }

private:
    std::vector<Type> _strings;
};

static void BM_sort(benchmark::State& state) {
    prepare_data();
    int type = state.range(0);
    int size = state.range(1);
    std::vector<std::string>* src = (size == 1? &short_strings: (size == 2 ? &mid_strings: &long_strings));
    // prepare_data();
    for (auto _ :state) {
        if (type == 1) {
            StringViewBench<Slice> perf(*src);
            perf.do_sort(state);
        } else {
            StringViewBench<StringView> perf(*src);
            perf.do_sort(state);
        }
    }
}

static void process_args(benchmark::internal::Benchmark* b) {
    for (int i = 1;i <= 2;i++) {
        for (int j = 1;j <= 3;j++) {
            b->Args({i, j})->Iterations(20);
        }
    }

}

BENCHMARK(BM_sort)->Apply(process_args);
// BENCHMARK(BM_hash)->Apply(process_args);


template <class T>
void run_sort(const std::vector<std::string>& data) {
    std::vector<T> str_list;
    {
        auto start = std::chrono::high_resolution_clock::now();
        for (const auto& s: data) {
            str_list.emplace_back(T(s));
        }
        auto end = std::chrono::high_resolution_clock::now();
        auto build_time = std::chrono::duration<double, std::micro>(end - start).count();
        std::cout << "build time: " << build_time << "us";
    }
    {
        auto start = std::chrono::high_resolution_clock::now();
        std::sort(str_list.begin(), str_list.end());
        auto end = std::chrono::high_resolution_clock::now();
        auto build_time = std::chrono::duration<double, std::micro>(end - start).count();
        std::cout << "build time: " << build_time << "us";
    }
}

BENCHMARK_MAIN();
}