#include "bridge.h"

#include "metkit/mars/MarsExpansion.h"
#include "metkit/mars/MarsRequest.h"
#include "metkit/mars/RequestEnvironment.h"
#include "eckit/runtime/Main.h"

#include <map>
#include <mutex>
#include <string>
#include <vector>

namespace {
void ensure_initialized() {
    static std::once_flag flag;
    std::call_once(flag, []() {
        if (!eckit::Main::ready()) {
            static const char* argv[] = {"metkit-rust", nullptr};
            static int argc = 1;
            eckit::Main::initialise(argc, const_cast<char**>(argv));
        }
        std::map<std::string, std::string> env{{"client", "metkit-rust"}};
        metkit::mars::RequestEnvironment::initialize(env);
    });
}
}  // namespace

namespace metkit::bridge {

void expand_request(rust::Str verb,
                    rust::Vec<rust::String> keys,
                    rust::Vec<rust::String> values,
                    rust::Vec<rust::String>& out_keys,
                    rust::Vec<rust::String>& out_values) {
    ensure_initialized();

    std::string verb_str(verb);
    metkit::mars::MarsRequest request(verb_str);

    std::map<std::string, std::vector<std::string>> grouped;
    for (std::size_t i = 0; i < keys.size(); ++i) {
        grouped[std::string(keys[i])].push_back(std::string(values[i]));
    }

    for (const auto& entry : grouped) {
        request.values(entry.first, entry.second);
    }

    metkit::mars::MarsExpansion expand(true, false);
    auto expanded = expand.expand(request);

    for (const auto& param : expanded.params()) {
        const auto& vals = expanded.values(param);
        for (const auto& val : vals) {
            out_keys.push_back(rust::String(param));
            out_values.push_back(rust::String(val));
        }
    }
}

}  // namespace metkit::bridge
