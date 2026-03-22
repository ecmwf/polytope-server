#pragma once

#include "rust/cxx.h"

#include <cstdint>

namespace metkit::bridge {

void expand_request(rust::Str verb,
                    rust::Vec<rust::String> keys,
                    rust::Vec<rust::String> values,
                    rust::Vec<rust::String>& out_keys,
                    rust::Vec<rust::String>& out_values);

}  // namespace metkit::bridge
