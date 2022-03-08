// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <memory>
#include <mutex>
#include <unordered_map>

#include "common/status.h"

namespace starrocks {

struct JDBCDriverEntry;

class JDBCDriverManager {
public:
    using JDBCDriverEntryPtr = std::shared_ptr<JDBCDriverEntry>;

    JDBCDriverManager();
    ~JDBCDriverManager();

    static JDBCDriverManager* getInstance();

    Status init(const std::string& driver_dir);

    Status get_driver_location(const std::string& name, const std::string& url, const std::string& checksum, std::string* location);

private:

    Status _download_driver(const std::string& url, JDBCDriverEntryPtr& entry);

    bool _parse_from_file_name(std::string_view file_name, std::string* name, std::string* checksum, int64_t* frist_access_ts);

    std::string _generate_driver_location(const std::string& name, const std::string& checksum, int64_t first_access_ts);

    std::string _driver_dir;

    std::mutex _lock;
    std::unordered_map<std::string, JDBCDriverEntryPtr> _entry_map;

    static constexpr const char* TMP_FILE_SUFFIX = ".tmp";
    static constexpr const char* JAR_FILE_SUFFIX = ".jar";
};
}