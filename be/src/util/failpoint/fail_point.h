#pragma once

#include <memory>
#include <mutex>
#include <unordered_map>
#include "common/status.h"
#include "common/statusor.h"
#include "fiu.h"
#include "fiu-control.h"
#include "gen_cpp/internal_service.pb.h"

namespace starrocks::failpoint {

class FailPoint {
public:
    FailPoint(const std::string& name);
    ~FailPoint() = default;

    bool shouldFail();

    void setMode(const PFailPointTriggerMode& p_trigger_mode);

    std::string name() const {
        return _name;
    }

private:
    std::string _name;
    std::mutex _mu;
    // trigger mode
    PFailPointTriggerMode _trigger_mode;
    int32_t _n_times = 0;
};

// @TODO need PausableFailPoint?

using FailPointPtr = std::shared_ptr<FailPoint>;

class FailPointRegistry {
// register all failpoints here
public:
    static FailPointRegistry* GetInstance() {
        static FailPointRegistry s_fp_registry;
        return &s_fp_registry;
    }

    Status add(FailPoint* fp);
    FailPoint* get(const std::string& name);

private:
    FailPointRegistry() {
        fiu_init(0);
    }
    ~FailPointRegistry() = default;

    std::unordered_map<std::string, FailPoint*> _fps;
};

class FailPointRegisterer {
public:
    explicit FailPointRegisterer(FailPoint* fp);
};

#define DEFINE_FAIL_POINT(NAME) \
    starrocks::failpoint::FailPoint fp##NAME(#NAME); \
    starrocks::failpoint::FailPointRegisterer fpr##NAME(&fp##NAME);

#define FAIL_POINT_TRIGGER_EXECUTE(NAME, stmt) \
    fiu_do_on(#NAME, stmt)
#define FAIL_POINT_TRIGGER_RETURN(NAME, retVal) \
    fiu_return_on(#NAME, retVal)
}