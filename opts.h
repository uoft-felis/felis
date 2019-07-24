#ifndef _OPTS_H
#define _OPTS_H

#include <string>
#include <vector>

namespace felis {

static inline long long ParseLargeNumber(std::string s)
{
  size_t pos;
  long long l = std::stoll(s, &pos);
  if (s.length() > pos) {
    auto ch = std::toupper(s[pos]);
    if (ch == 'G') l <<= 30;
    else if (ch == 'M') l <<= 20;
    else if (ch == 'K') l <<= 10;
  }
  return l;
}

struct Option {
  static inline std::vector<Option *> g_options;
  static inline std::vector<std::string> g_suffices;
  static inline std::vector<bool> g_present;

  int id;
  std::string prefix;
  Option(const char *name) : prefix(name), id(g_options.size()) {
    g_options.push_back(this);
    g_suffices.emplace_back();
    g_present.emplace_back(false);
  }

  operator bool() const { return g_present[id]; }
  std::string Get(const char *def = nullptr) const {
    if (*this)
      return g_suffices[id];
    else
      return def ? std::string(def) : std::string();
  }
  int ToInt(const char *def = nullptr) const {
    return std::stoi(Get(def));
  }
  long long ToLargeNumber(const char *def = nullptr) const {
    return ParseLargeNumber(Get(def));
  }
};

struct Options {

  // Long live the JVM style command line arguments!
  static inline const auto kCpu = Option("cpu");
  static inline const auto kMem = Option("mem");
  static inline const auto kOutputDir = Option("OutputDir");
  static inline const auto kDataMigration = Option("DataMigrationMode");
  static inline const auto kMaxNodeLimit = Option("MaxNodeLimit");

  static inline const auto kCoreShifting = Option("CoreShifting");

  static inline const auto kNrEpoch = Option("NrEpoch");
  static inline const auto kEpochQueueLength = Option("EpochQueueLength");
  static inline const auto kVHandleLockElision = Option("VHandleLockElision");
  static inline const auto kVHandleBatchAppend = Option("VHandleBatchAppend");
  static inline const auto kCongestionControl = Option("CongestionControl");

  static inline const auto kTpccWarehouses = Option("TpccWarehouses");

  static inline const auto kYcsbTableSize = Option("YcsbTableSize");
  static inline const auto kYcsbSkewFactor = Option("YcsbSkewFactor");
  static inline const auto kYcsbEnablePartition = Option("YcsbEnablePartition");

  static inline bool ParseExtentedOptions(std::string arg)
  {
    for (auto o: Option::g_options) {
      if (arg.compare(0, o->prefix.length(), o->prefix) == 0) {
        Option::g_suffices[o->id] = arg.substr(o->prefix.length());
        Option::g_present[o->id] = true;
        return true;
      }
    }
    return false;
  }

};

}

#endif
