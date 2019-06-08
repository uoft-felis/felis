#ifndef _OPTS_H
#define _OPTS_H

#include <string>
#include <vector>

namespace felis {

struct Option {
  static inline std::vector<Option *> g_options;
  static inline std::vector<std::string> g_suffices;
  static inline std::vector<bool> g_present;
  static std::string Get(const Option &o) { return g_suffices[o.id]; }
  static std::string GetOrDefault(const Option &o, const char *def) {
    if (IsPresent(o))
      return g_suffices[o.id];
    else
      return def;
  }
  static bool IsPresent(const Option &o) { return g_present[o.id]; }
  int id;
  std::string prefix;
  Option(const char *name) : prefix(name), id(g_options.size()) {
    g_options.push_back(this);
    g_suffices.emplace_back();
    g_present.emplace_back(false);
  }
};

struct Options {

// Long live the JVM style command line arguments!
static inline const auto kCpu = Option("cpu");
static inline const auto kMem = Option("mem");
static inline const auto kDataMigration = Option("DataMigrationMode");

static inline const auto kCoreShifting = Option("CoreShifting");

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

};;

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

}

#endif
