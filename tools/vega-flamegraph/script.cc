#include <iostream>
#include <map>
#include <stack>

static std::map<long, std::string> g_sym_table;
static long g_tree_id = 0;

class TreeNode {
  long id = 0;
  long addr; // 0 for root
  long count = 0;
  std::map<long, TreeNode *> children;
 public:
  TreeNode(long addr) : addr(addr), id(g_tree_id++) {}

  TreeNode *Touch(long child_addr) {
    auto it = children.find(child_addr);
    if (it == children.end()) {
      it = children.insert(std::make_pair(child_addr, new TreeNode(child_addr))).first;
    }
    // it->second->Increment();
    return it->second;
  }

  void Increment() { count++; }
  void OutputJson(std::ostream &out) {
    out << "[" << std::endl;
    if (addr != 0) std::abort();
    Output(out, -1);
    out << "]" << std::endl;
  }
 private:
  void Output(std::ostream &out, long parent_id) {
    if (parent_id != -1)
      out << ",";
    out << "{\"id\":" << id << ",\"count\":" << count << "," << "\"name\":\"" << g_sym_table[addr] << "\"";
    if (parent_id != -1)
      out << ",\"parent\":" << parent_id;
    out << "}" << std::endl;

    long s = 0;
    for (auto &p: children) {
      p.second->Output(out, id);
      s += p.second->count;
    }
  }
};

TreeNode *root = new TreeNode(0);

void ProcessSampleStack(std::stack<long> &sample)
{
  if (sample.empty()) return;
  TreeNode *p = root;
  int dep = 0;
  while (!sample.empty() && dep++ < 64) {
    p = p->Touch(sample.top());
    sample.pop();
  }
  p->Increment();
}

bool IsSampleLine(std::string line)
{
  return line.length() > 0 && (line[0] == ' ' || line[0] == '\t');
}

static bool ParseError(bool cond, std::string line)
{
  if (cond)
    std::cerr << "Cannot process line: " << line << std::endl;
  return cond;
}

long ParseLine(std::string line)
{
  int sym_end = -1, off_end = -1, addr_end = -1, addr_start = 0;
  for (int i = line.length() - 1; i >= 0; i--) {
    if (line[i] == '(') {
      off_end = i - 1;
      break;
    }
  }
  if (ParseError(off_end < 0, line))
      return -1;

  for (int i = off_end - 1; i >= 0; i--) {
    if (line[i] == '+' || line[i] == ']') {
      sym_end = i;
      break;
    }
  }

  if (sym_end < 0) {
    sym_end = off_end;
    off_end++;
  }

  bool seen_char = false;
  for (int i = 0; i < sym_end; i++) {
    if (line[i] == ' ' || line[i] == '\t') {
      if (seen_char) {
        addr_end = i;
        break;
      } else {
        addr_start = i + 1;
      }
    } else {
      seen_char = true;
    }
  }

  if (ParseError(addr_end < 0, line))
    return -1;

  auto addr_str = line.substr(addr_start, addr_end - addr_start);
  auto sym_str = line.substr(addr_end + 1, sym_end - addr_end - 1);
  auto off_str = line.substr(sym_end + 1, off_end - sym_end - 1);
  if (off_str == "") off_str = "0";

  // std::cerr << addr_str << ' ' << off_str << std::endl;
  auto sym_addr = std::stoull(addr_str, 0, 16) - std::stoull(off_str, 0, 16);
  // std::cout << sym_addr << " = " << sym_str << std::endl;
  g_sym_table[sym_addr] = sym_str;
  return sym_addr;
}

int main()
{
  g_sym_table[0] = "root";
  std::string line;
  std::stack<long> stk;
  while (!std::cin.eof()) {
    std::getline(std::cin, line);
    if (IsSampleLine(line)) {
      auto p = ParseLine(line);
      if (p >= 0)
        stk.push(p);
    } else {
      ProcessSampleStack(stk);
      stk = std::stack<long>();
    }
  }
  root->OutputJson(std::cout);
  return 0;
}
