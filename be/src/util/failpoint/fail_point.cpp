
#include "util/failpoint/fail_point.h"
#include "fmt/format.h"

namespace starrocks::failpoint {

int check_fail_point(const char* name, int* failnum, void **failinfo, unsigned int *flags) {
    auto fp = FailPointRegistry::GetInstance()->get(name);
    if (fp == nullptr) {
        LOG(WARNING) << "cannot find failpoint with name " << name;
        return 0;
    }
    return fp->shouldFail();
}

FailPoint::FailPoint(const std::string& name): _name(name) {
    _trigger_mode.set_mode(FailPointTriggerModeType::DISABLE);
    // fiu_enable_external(_name.c_str(), 1, nullptr, 0, check_fail_point);
}

bool FailPoint::shouldFail() {
    std::lock_guard l(_mu);
    const auto mode = _trigger_mode.mode();
    switch (mode) {
        case FailPointTriggerModeType::ENABLE:
            return true;
        case FailPointTriggerModeType::DISABLE:
            return false;
        case FailPointTriggerModeType::PROBABILITY_ENABLE:
            if (drand48() <= static_cast<double>(_trigger_mode.probability())) {
                return true;
            }
            return false;
        case FailPointTriggerModeType::ENABLE_N_TIMES:
            if (_n_times-- > 0) {
                return true;
            }
            return false;
        default:
            DCHECK(false);
            break;
    }
    return false;
}

void FailPoint::setMode(const PFailPointTriggerMode& p_trigger_mode) {
    std::lock_guard l(_mu);
    _trigger_mode = p_trigger_mode;
    auto type = p_trigger_mode.mode();
    switch (type) {
        case FailPointTriggerModeType::ENABLE_N_TIMES:
            _n_times = p_trigger_mode.n_times();
            break;
        default:
            break;
    }
}

Status FailPointRegistry::add(FailPoint* fp) {
    auto name = fp->name();
    if (_fps.find(name) != _fps.end()) {
        return Status::AlreadyExist(fmt::format("failpoint {} already exists", name));
    }
    _fps.insert({name, fp});
    fiu_enable_external(name.c_str(), 1, nullptr, 0, check_fail_point);
    return Status::OK();
}

FailPoint* FailPointRegistry::get(const std::string& name) {
    auto iter = _fps.find(name);
    if (iter == _fps.end()) {
        return nullptr;
    }
    return iter->second;
}

FailPointRegisterer::FailPointRegisterer(FailPoint* fp) {
    FailPointRegistry::GetInstance()->add(fp);
}

}