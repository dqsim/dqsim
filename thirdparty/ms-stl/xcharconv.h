// xcharconv.h internal header

// Copyright (c) Microsoft Corporation.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#pragma once
#ifndef _XCHARCONV_H
#define _XCHARCONV_H

#include <system_error>

namespace msstl {

// ENUM CLASS chars_format
enum class chars_format {
    scientific = 0b001,
    fixed      = 0b010,
    hex        = 0b100,
    general    = fixed | scientific,
};

// _BITMASK_OPS(chars_format)

// STRUCT to_chars_result
struct to_chars_result {
    char* ptr;
    std::errc ec;
};

}
#endif
