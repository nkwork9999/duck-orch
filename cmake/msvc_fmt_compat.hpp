#pragma once

#ifdef _MSC_VER
#include <yvals_core.h>

// DuckDB v1.5.3 vendors an older fmt release that uses
// stdext::checked_array_iterator whenever _SECURE_SCL is defined. VS 18
// still defines the compatibility macro but no longer provides stdext.
#ifdef _SECURE_SCL
#undef _SECURE_SCL
#endif
#endif
