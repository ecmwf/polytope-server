/*
 * SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
 *
 * SPDX-License-Identifier: Apache-2.0
 */

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
