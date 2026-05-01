#pragma once

#include <cstddef>
#include <cstdint>

extern "C" {

// Phase 0 hello world.
int32_t orch_hello(const uint8_t *name_ptr, size_t name_len, uint8_t *out_buf, size_t out_cap);

// Heap-allocated buffer release. Used by all functions below.
void orch_string_free(uint8_t *ptr, size_t len);

// All functions below: write a heap buffer into (out_ptr, out_len).
// Return 0 on success, negative on error. Caller MUST call orch_string_free.

// Parse a single SQL with @task headers. file_path may be empty.
int32_t orch_parse_task(const uint8_t *sql_ptr, size_t sql_len,
                        const uint8_t *file_path_ptr, size_t file_path_len,
                        uint8_t **out_ptr, size_t *out_len);

// Walk directory, parse all .sql files. Returns {tasks: [...], errors: [...]}.
int32_t orch_load_directory(const uint8_t *path_ptr, size_t path_len,
                             uint8_t **out_ptr, size_t *out_len);

// Extract inputs/outputs from a SQL string. Returns {inputs: [...], outputs: [...]}.
int32_t orch_extract_io(const uint8_t *sql_ptr, size_t sql_len,
                         uint8_t **out_ptr, size_t *out_len);

// Build DAG. Input: JSON array of Tasks. Returns DagResult JSON.
int32_t orch_build_dag(const uint8_t *tasks_json_ptr, size_t tasks_json_len,
                        uint8_t **out_ptr, size_t *out_len);

// Render Mermaid. mode: 0=lineage, 1=dag, 2=combined.
int32_t orch_render_mermaid(const uint8_t *dag_json_ptr, size_t dag_json_len,
                             int32_t mode,
                             const uint8_t *statuses_json_ptr, size_t statuses_json_len,
                             uint8_t **out_ptr, size_t *out_len);

// Compute downstream task names from a failed task.
int32_t orch_downstream_of(const uint8_t *tasks_json_ptr, size_t tasks_json_len,
                            const uint8_t *failed_ptr, size_t failed_len,
                            uint8_t **out_ptr, size_t *out_len);

// Replace {{ var }} placeholders. vars_json: {"key":"value", ...}.
int32_t orch_substitute_vars(const uint8_t *sql_ptr, size_t sql_len,
                              const uint8_t *vars_json_ptr, size_t vars_json_len,
                              uint8_t **out_ptr, size_t *out_len);

}
