#include "console.h"

namespace dolly {

std::string Console::HandleAPI(std::string uri)
{
  if (uri == "/client/start/") {

    return "OK";
  }
  return uri;
}

}
