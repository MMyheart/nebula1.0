/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "StatisticDumper.h"
#include "base/Base.h"
#include "fs/FileUtils.h"

void printHelp() {
    fprintf(stderr,
            R"(  ./statistic_dump --space=<space id>
 
required:
       --space=<space id>
         A space id must be given.
 
optional:
       --db_path=<path to rocksdb>
         Path to the rocksdb data directory. If nebula was installed in /usr/local/nebula,
         the db_path would be /usr/local/nebula/data/storage/nebula/
         Default: ./
 
       --tags=<list of tag id>
         A list of tag id seperated by comma.
 
       --edges=<list of edge id>
         A list of edge id seperated by comma.
 
       --limit=<N>
         A positive number that limits the output.
         Would output all if set 0 or negative number.
         Default: 1000
 
 
)");
}

void printParams() {
    std::cout << "===========================PARAMS============================\n";
    std::cout << "space: " << FLAGS_space << "\n";
    std::cout << "path: " << FLAGS_db_path << "\n";
    std::cout << "tags: " << FLAGS_tags << "\n";
    std::cout << "edges: " << FLAGS_edges << "\n";
    std::cout << "limit: " << FLAGS_limit << "\n";
    std::cout << "===========================PARAMS============================\n\n";
}

int main(int argc, char *argv[]) {
    if (argc == 1) {
        printHelp();
        return EXIT_FAILURE;
    } else {
        folly::init(&argc, &argv, true);
    }

    google::SetStderrLogging(google::FATAL);

    printParams();

    nebula::storage::StatisticDumper dumper;
    auto status = dumper.init();
    if (!status.ok()) {
        std::cerr << "Error: " << status << "\n\n";
        return EXIT_FAILURE;
    }
    dumper.run();
}
