#include "console.h"

namespace felis {

Console::Console()
{
  handlers["status_change"] = &Console::HandleStatusChange;
  handlers["get_status"] = &Console::HandleGetStatus;
}

static const auto kJsonResponseError = json11::Json::object({
    {"type", "error"},
  });

json11::Json Console::HandleJsonAPI(json11::Json j)
{
  auto req_type = j.object_items().find("type")->second.string_value();
  auto it = handlers.find(req_type);
  if (it != handlers.end()) {
    return it->second(this, j);
  } else {
    return kJsonResponseError;
  }
}

static const std::string kStatusNames[] = {
  "booting", "configuring", "listening", "connecting", "running", "exiting",
};
static const size_t kNrStatusNames = 6;

json11::Json Console::HandleStatusChange(json11::Json j)
{
  std::string propsed_status = j.object_items().find("status")->second.string_value();
  auto it = std::find(kStatusNames, kStatusNames + kNrStatusNames, propsed_status);
  if (it == kStatusNames + kNrStatusNames) {
    return kJsonResponseError;
  }

  // Since we are configuring, we just overwrite the entire configuration.
  if (propsed_status == "configuring")
    conf = j;

  auto status = (Console::ServerStatus) ((int) (it - kStatusNames));
  UpdateServerStatus(status);

  return JsonResponse();
}

json11::Json Console::HandleGetStatus(json11::Json j)
{
  if (server_status < kNrStatusNames) {
    std::string status = kStatusNames[server_status];
    return JsonResponse({{"status", status}});
  }
  return kJsonResponseError;
}

}
