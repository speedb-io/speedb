// Copyright (C) 2023 Speedb Ltd. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <getopt.h>
// without this flag make format will force stdio to be after readline
// which may cause compilation error on clang
// clang-format off
#include <stdio.h>
// clang-format on
#include <readline/history.h>
#include <readline/readline.h>
#include <signal.h>

#include <iostream>
#include <iterator>
#include <sstream>

#include "rocksdb/ldb_tool.h"

void SignalHandler(int sigint) {
  std::cout << std::endl << "Ciao" << std::endl;
  exit(0);
}
void ToArgv(std::string const& input, std::vector<std::string>& temp) {
  std::istringstream buffer(input);
  std::copy(std::istream_iterator<std::string>(buffer),
            std::istream_iterator<std::string>(), std::back_inserter(temp));
}
int main(int argc, char** argv) {
  signal(SIGINT, &SignalHandler);
  ROCKSDB_NAMESPACE::LDBTool tool;
  std::string prompt = "beezcli> ";
  const char* const short_opts = "dis\0";
  const option long_opts[] = {{"db", required_argument, 0, 'd'},
                              {"interactive", no_argument, nullptr, 'i'},
                              {"secondary_path", required_argument, 0, 's'},
                              {0, 0, 0, 0}};
  int opt;
  std::string db_path = "";
  std::string secondary_path = "";
  bool i = false;
  bool d = false;
  bool s [[maybe_unused]] = false;
  opterr = 0;
  opt = getopt_long(argc, argv, short_opts, long_opts, nullptr);
  while (opt != -1) {
    switch (opt) {
      case 'd':
        db_path = std::string(optarg);
        std::cout << db_path << std::endl;
        d = true;
        break;
      case 'i':
        i = true;
        break;
      case 's':
        secondary_path = std::string(optarg);
        s = true;
        break;
    }
    opt = getopt_long(argc, argv, short_opts, long_opts, nullptr);
  }
  char* line;
  if (i && !d) {
    std::cerr << "interactive flag provided without --db" << std::endl;
    return EINVAL;
  }
  while (i && d && (line = readline(prompt.c_str())) && line) {
    if (line[0] != '\0') add_history(line);
    std::string input(line);
    free(line);
    line = nullptr;
    if (input == "help") {
      char** help = new char*[2];
      help[0] = argv[0];
      help[1] = const_cast<char*>("--help");
      tool.Run(2, help, ROCKSDB_NAMESPACE::Options(),
               ROCKSDB_NAMESPACE::LDBOptions(), nullptr, false);
      continue;
    }
    if (input == "quit" || input == "exit") {
      SignalHandler(0);
    }
    if (!input.empty()) {
      if (!s) {
        std::vector<std::string> vec;
        ToArgv(std::string(argv[0]) + " " + input + " --db=" + db_path, vec);
        std::vector<char*> cstrings{};
        for (const auto& string : vec) {
          cstrings.push_back(const_cast<char*>(string.c_str()));
        }
        tool.Run(cstrings.size(), cstrings.data(), ROCKSDB_NAMESPACE::Options(),
                 ROCKSDB_NAMESPACE::LDBOptions(), nullptr, false);
      } else {
        std::vector<std::string> vec;
        ToArgv(std::string(argv[0]) + " " + input + " --db=" + db_path +
                   " --secondary_path=" + secondary_path,
               vec);
        std::vector<char*> cstrings{};
        for (const auto& string : vec) {
          cstrings.push_back(const_cast<char*>(string.c_str()));
        }
        tool.Run(cstrings.size(), cstrings.data(), ROCKSDB_NAMESPACE::Options(),
                 ROCKSDB_NAMESPACE::LDBOptions(), nullptr, false);
      }
    }
  }
  if (line == nullptr && i && d) {
    SignalHandler(0);
  }
  tool.Run(argc, argv);
  return 0;
}
