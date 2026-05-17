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

// Extract column-level lineage. schema_json is optional (empty string ok),
// used to resolve `SELECT *`. Returns JSON array of ExtractResult.
int32_t orch_extract_column_lineage(
    const uint8_t *sql_ptr, size_t sql_len,
    const uint8_t *schema_json_ptr, size_t schema_json_len,
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

// OpenLineage emitter (Phase 9)
int32_t orch_ol_set_url(const uint8_t *ptr, size_t len);
int32_t orch_ol_set_api_key(const uint8_t *ptr, size_t len);
int32_t orch_ol_set_debug(int32_t d);
int32_t orch_ol_emit(const uint8_t *ptr, size_t len);

// Topological layers for parallel execution (Phase 5)
int32_t orch_topo_layers(const uint8_t *tasks_json_ptr, size_t tasks_json_len,
                          uint8_t **out_ptr, size_t *out_len);

// Phase 13: canonical SQL code_version (FNV-1a 64-bit hex) for Asset rows.
int32_t orch_sql_code_version(const uint8_t *sql_ptr, size_t sql_len,
                               uint8_t **out_ptr, size_t *out_len);

// Phase 13 m2: render Mermaid centered on `focal_asset`. `edges_json` is a
// JSON array of `{upstream_asset, downstream_asset, via_task, edge_type}`
// rows pre-filtered by the caller (no transitive closure done Rust-side).
int32_t orch_render_asset_lineage(const uint8_t *focal_ptr, size_t focal_len,
                                   const uint8_t *edges_json_ptr,
                                   size_t edges_json_len, uint8_t **out_ptr,
                                   size_t *out_len);

// Phase 14: expand a PartitionDef into concrete keys. `def_json` is the
// serde-serialized PartitionDef carried on Task.partitions. `range_json`
// is optional `{"from":"YYYY-MM-DD","to":"YYYY-MM-DD"}` for daily; pass
// empty string to use the natural range. Output: JSON array of
// `{key, dimension_values: {...}}` rows.
int32_t orch_partition_expand(const uint8_t *def_json_ptr, size_t def_json_len,
                               const uint8_t *range_json_ptr, size_t range_json_len,
                               uint8_t **out_ptr, size_t *out_len);

// Phase 14: split a partition key into per-dimension `(name, value)` pairs.
// Returns a JSON array; for non-Multi defs it's a single element with
// name="partition_key".
int32_t orch_partition_split_key(const uint8_t *def_json_ptr, size_t def_json_len,
                                  const uint8_t *key_ptr, size_t key_len,
                                  uint8_t **out_ptr, size_t *out_len);

// Phase 14: render the calendar-style ASCII for an Asset. `rows_json` is
// `[{"key":..., "status": null|"success"|"failed"|"in_progress"}, ...]`.
int32_t orch_render_partition_calendar(const uint8_t *asset_ptr, size_t asset_len,
                                        const uint8_t *def_json_ptr, size_t def_json_len,
                                        const uint8_t *rows_json_ptr, size_t rows_json_len,
                                        uint8_t **out_ptr, size_t *out_len);

}
