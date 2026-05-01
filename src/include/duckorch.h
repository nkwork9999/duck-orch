#pragma once

#include <cstddef>
#include <cstdint>

// C FFI functions exported from the Rust static library `duckorch_core`.
//
// Phase 0: hello world only. Future phases add task registration, DAG execution,
// lineage extraction, and OpenLineage event emission.

extern "C" {

// Phase 0 hello world. Writes a greeting into out_buf. Returns bytes written, or -1.
int32_t orch_hello(const uint8_t *name_ptr, size_t name_len,
                   uint8_t *out_buf, size_t out_cap);

}
