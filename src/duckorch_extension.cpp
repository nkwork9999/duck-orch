#define DUCKDB_EXTENSION_MAIN

#include "duckorch_extension.hpp"
#include "duckorch.h"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/ternary_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "yyjson.hpp"

#include <atomic>
#include <chrono>
#include <iomanip>
#include <map>
#include <mutex>
#include <set>
#include <sstream>
#include <thread>

namespace duckdb {

// Pipeline namespace, used for OpenLineage events. Set via SET orch_namespace.
static string g_orch_namespace = "duckdb";
static std::atomic<int> g_max_parallel{1};

// ParserExtension: capture column lineage for queries that don't go through
// PRAGMA orch_run (PreparedStatements, ad-hoc INSERT/CTAS, dynamic SQL).
// Disabled by default. Toggled via SET orch_capture_interactive=true.
static std::atomic<bool> g_capture_interactive{false};
// Database pointer stashed at LoadInternal time so the parser callback (which
// runs without a ClientContext) can open its own Connection.
static DatabaseInstance *g_db_for_capture = nullptr;
// Thread-local recursion guard so internal con.Query() calls don't loop.
static thread_local bool g_inside_capture = false;

struct OlDataset {
	string ns;
	string name;
};

// Forward declarations
static string OlEventJson(const string &event_type, const string &event_time,
                          const string &run_id, const string &pipeline_run_id,
                          const string &job_namespace, const string &job_name,
                          const std::vector<OlDataset> &inputs,
                          const std::vector<OlDataset> &outputs,
                          const string &error_message);
static void EmitOlEvent(const string &json);
static string JsonEscape(const string &s);
static string ResolveDatasetNamespace(ClientContext &context, const string &table_name);
static string RecordColumnLineage(Connection &con, const string &task_sql,
                                   const string &task_name,
                                   const std::vector<OlDataset> &task_inputs);
static string BuildColumnLineageFacet(const string &cl_extractor_json,
                                       const std::vector<OlDataset> &task_inputs);

// ========================================================================
// FFI helpers
// ========================================================================

// Call a Rust FFI that returns (ptr, len) into out-params and convert to std::string.
// On error returns empty string.
template <typename CallFn>
static std::string CallRustString(CallFn &&fn, bool &ok) {
	uint8_t *ptr = nullptr;
	size_t len = 0;
	int32_t rc = fn(&ptr, &len);
	std::string result;
	if (rc == 0 && ptr != nullptr && len > 0) {
		result.assign(reinterpret_cast<const char *>(ptr), len);
		ok = true;
	} else {
		if (ptr != nullptr && len > 0) {
			// Error JSON in buffer; surface as exception payload
			result.assign(reinterpret_cast<const char *>(ptr), len);
		}
		ok = false;
	}
	if (ptr != nullptr) {
		orch_string_free(ptr, len);
	}
	return result;
}

// ========================================================================
// Scalar functions: pure transforms
// ========================================================================

static void OrchHelloFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::ExecuteWithNulls<string_t, string_t>(
	    args.data[0], result, args.size(),
	    [&](string_t input, ValidityMask &mask, idx_t idx) -> string_t {
		    uint8_t buf[256];
		    int32_t n = orch_hello(reinterpret_cast<const uint8_t *>(input.GetData()),
		                            input.GetSize(), buf, sizeof(buf));
		    if (n < 0) {
			    mask.SetInvalid(idx);
			    return string_t();
		    }
		    return StringVector::AddString(result, reinterpret_cast<const char *>(buf), n);
	    });
}

static void OrchExtractIoFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::ExecuteWithNulls<string_t, string_t>(
	    args.data[0], result, args.size(),
	    [&](string_t sql, ValidityMask &mask, idx_t idx) -> string_t {
		    bool ok = false;
		    auto json = CallRustString(
		        [&](uint8_t **op, size_t *ol) {
			        return orch_extract_io(
			            reinterpret_cast<const uint8_t *>(sql.GetData()), sql.GetSize(), op, ol);
		            },
		        ok);
		    if (!ok) {
			    mask.SetInvalid(idx);
			    return string_t();
		    }
		    return StringVector::AddString(result, json);
	    });
}

static void OrchParseTaskFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    args.data[0], args.data[1], result, args.size(),
	    [&](string_t sql, string_t fp) -> string_t {
		    bool ok = false;
		    auto json = CallRustString(
		        [&](uint8_t **op, size_t *ol) {
			        return orch_parse_task(
			            reinterpret_cast<const uint8_t *>(sql.GetData()), sql.GetSize(),
			            reinterpret_cast<const uint8_t *>(fp.GetData()), fp.GetSize(), op, ol);
		            },
		        ok);
		    return StringVector::AddString(result, json);
	    });
}

static void OrchLoadDirectoryFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<string_t, string_t>(
	    args.data[0], result, args.size(),
	    [&](string_t path) -> string_t {
		    bool ok = false;
		    auto json = CallRustString(
		        [&](uint8_t **op, size_t *ol) {
			        return orch_load_directory(
			            reinterpret_cast<const uint8_t *>(path.GetData()), path.GetSize(), op, ol);
		            },
		        ok);
		    return StringVector::AddString(result, json);
	    });
}

static void OrchBuildDagFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<string_t, string_t>(
	    args.data[0], result, args.size(),
	    [&](string_t json) -> string_t {
		    bool ok = false;
		    auto out = CallRustString(
		        [&](uint8_t **op, size_t *ol) {
			        return orch_build_dag(
			            reinterpret_cast<const uint8_t *>(json.GetData()), json.GetSize(), op, ol);
		            },
		        ok);
		    return StringVector::AddString(result, out);
	    });
}

static void OrchRenderMermaidFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	TernaryExecutor::Execute<string_t, int32_t, string_t, string_t>(
	    args.data[0], args.data[1], args.data[2], result, args.size(),
	    [&](string_t dag_json, int32_t mode, string_t statuses_json) -> string_t {
		    bool ok = false;
		    auto out = CallRustString(
		        [&](uint8_t **op, size_t *ol) {
			        return orch_render_mermaid(
			            reinterpret_cast<const uint8_t *>(dag_json.GetData()), dag_json.GetSize(),
			            mode,
			            reinterpret_cast<const uint8_t *>(statuses_json.GetData()),
			            statuses_json.GetSize(), op, ol);
		            },
		        ok);
		    return StringVector::AddString(result, out);
	    });
}

static void OrchDownstreamOfFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    args.data[0], args.data[1], result, args.size(),
	    [&](string_t json, string_t failed) -> string_t {
		    bool ok = false;
		    auto out = CallRustString(
		        [&](uint8_t **op, size_t *ol) {
			        return orch_downstream_of(
			            reinterpret_cast<const uint8_t *>(json.GetData()), json.GetSize(),
			            reinterpret_cast<const uint8_t *>(failed.GetData()), failed.GetSize(),
			            op, ol);
		            },
		        ok);
		    return StringVector::AddString(result, out);
	    });
}

// ========================================================================
// PRAGMA: orch_init — create __orch__ schema and tables
// ========================================================================

// Schema setup SQL, broken into individual statements for direct execution.
static const char *kOrchSchemaSql = R"(
CREATE SCHEMA IF NOT EXISTS __orch__;

CREATE TABLE IF NOT EXISTS __orch__.tasks (
    name VARCHAR PRIMARY KEY,
    description VARCHAR,
    owner VARCHAR,
    sql VARCHAR NOT NULL,
    inputs VARCHAR[],
    outputs VARCHAR[],
    depends_on VARCHAR[],
    schedule_cron VARCHAR,
    retries INT DEFAULT 0,
    timeout_seconds INT,
    incremental_by VARCHAR,
    tags VARCHAR[],
    file_path VARCHAR,
    registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Phase 14: serialized PartitionDef (serde JSON) when the task carries
    -- a `-- @partitions_by ...` header. NULL = unpartitioned task.
    partitions_json VARCHAR,
    -- Phase 14: declared `-- @param name:TYPE` specs as a JSON array of
    -- `{name, ty}`. Re-hydrated at execution time so RunSingleTask knows
    -- which params to bind via DuckDB PREPARE.
    params_json VARCHAR
);
-- Tolerate older databases that pre-date Phase 14: add the new columns
-- if they are missing. NULL default keeps the unpartitioned path intact.
ALTER TABLE __orch__.tasks ADD COLUMN IF NOT EXISTS partitions_json VARCHAR;
ALTER TABLE __orch__.tasks ADD COLUMN IF NOT EXISTS params_json VARCHAR;

CREATE TABLE IF NOT EXISTS __orch__.runs (
    run_id UUID PRIMARY KEY,
    pipeline_run_id UUID,
    task_name VARCHAR,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    status VARCHAR,
    rows_count BIGINT,
    error_message VARCHAR,
    error_context_json VARCHAR,
    retry_count INT DEFAULT 0,
    last_processed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS __orch__.lineage_edges (
    src_dataset VARCHAR,
    dst_dataset VARCHAR,
    via_task VARCHAR,
    transform_type VARCHAR,
    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source VARCHAR,
    PRIMARY KEY (src_dataset, dst_dataset, via_task)
);

CREATE TABLE IF NOT EXISTS __orch__.task_edges (
    upstream VARCHAR,
    downstream VARCHAR,
    PRIMARY KEY (upstream, downstream)
);

CREATE TABLE IF NOT EXISTS __orch__.tests (
    task_name VARCHAR,
    test_idx INT,
    query VARCHAR,
    assertion VARCHAR,
    PRIMARY KEY (task_name, test_idx)
);

DROP TABLE IF EXISTS __orch__.column_lineage;
CREATE TABLE __orch__.column_lineage (
    src_dataset VARCHAR,
    src_column VARCHAR,
    dst_dataset VARCHAR,
    dst_column VARCHAR,
    via_task VARCHAR,
    transform_kind VARCHAR,
    subtype VARCHAR,
    description VARCHAR,
    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Phase 13: Asset 一級化 — Asset, Materialization history, Asset edges.
CREATE TABLE IF NOT EXISTS __orch__.assets (
    name VARCHAR PRIMARY KEY,
    kind VARCHAR,
    location VARCHAR,
    group_name VARCHAR,
    owner VARCHAR,
    description VARCHAR,
    code_version VARCHAR,
    defined_by_task VARCHAR,
    tags VARCHAR[],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS __orch__.asset_materializations (
    asset_name VARCHAR,
    partition_key VARCHAR DEFAULT '__default__',
    materialized_at TIMESTAMP,
    run_id UUID,
    rows BIGINT,
    bytes BIGINT,
    status VARCHAR,
    PRIMARY KEY (asset_name, partition_key, materialized_at)
);

CREATE TABLE IF NOT EXISTS __orch__.asset_edges (
    upstream_asset VARCHAR,
    downstream_asset VARCHAR,
    via_task VARCHAR,
    edge_type VARCHAR,
    PRIMARY KEY (upstream_asset, downstream_asset, via_task)
);

-- Phase 14: per-Asset partition registry. One row per (asset, key) tuple
-- expanded from `@partitions_by` at registration time. `dimension_values`
-- is a JSON string (`{"date":"2026-05-17","region":"jp"}` for Multi,
-- `{"partition_key":"jp"}` for Static, `{"partition_key":"2026-05-17"}`
-- for Daily). Re-registration is idempotent via INSERT OR IGNORE.
CREATE TABLE IF NOT EXISTS __orch__.asset_partitions (
    asset_name VARCHAR,
    partition_key VARCHAR,
    dimension_values VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (asset_name, partition_key)
);
)";

static void EnsureOrchSchema(Connection &con) {
	// Several pragmas (orch_*_health, orch_asset_partitions_calendar, etc.)
	// emit `to_json(list(...))` which lives in the json extension. It ships
	// bundled with DuckDB but is not auto-loaded in `-unsigned` sessions, so
	// load it explicitly here. Cheap (~no-op after first call).
	con.Query("INSTALL json; LOAD json;");
	auto r = con.Query(kOrchSchemaSql);
	if (r->HasError()) {
		throw InvalidInputException("orch schema setup failed: " + r->GetError());
	}
}

static void OrchInitPragma(ClientContext &context, const FunctionParameters &parameters) {
	Connection con(*context.db);
	EnsureOrchSchema(con);
}

// ========================================================================
// PRAGMA: orch_register — load directory, INSERT into __orch__.tasks
// ========================================================================

namespace yyjson_ns = duckdb_yyjson;

static string SqlEscape(const string &s) {
	string out;
	out.reserve(s.size() + 2);
	out += '\'';
	for (char c : s) {
		if (c == '\'') {
			out += "''";
		} else {
			out += c;
		}
	}
	out += '\'';
	return out;
}

static string SqlArrayLiteral(yyjson_ns::yyjson_val *arr) {
	if (!arr || !yyjson_ns::yyjson_is_arr(arr)) {
		return "[]::VARCHAR[]";
	}
	std::ostringstream oss;
	oss << "[";
	size_t idx, max;
	yyjson_ns::yyjson_val *v;
	bool first = true;
	yyjson_arr_foreach(arr, idx, max, v) {
		if (!first) oss << ",";
		first = false;
		const char *s = yyjson_ns::yyjson_get_str(v);
		oss << SqlEscape(s ? string(s) : string());
	}
	oss << "]::VARCHAR[]";
	return oss.str();
}

// Phase 13: compute the canonical code_version (FNV-1a 64-bit hex) for a SQL
// body via the Rust shim, so the Asset row reflects a stable fingerprint of
// the task definition. Returns empty string on Rust-side failure.
static string ComputeCodeVersion(const string &sql_body) {
	bool ok = false;
	auto v = CallRustString(
	    [&](uint8_t **op, size_t *ol) {
		    return orch_sql_code_version(
		        reinterpret_cast<const uint8_t *>(sql_body.c_str()), sql_body.size(), op, ol);
	        },
	    ok);
	return ok ? v : string();
}

static void OrchRegisterPragma(ClientContext &context, const FunctionParameters &parameters) {
	if (parameters.values.empty()) {
		throw InvalidInputException("orch_register requires a directory path");
	}
	string path = parameters.values[0].GetValue<string>();
	Connection user_con(*context.db);
	EnsureOrchSchema(user_con);

	bool ok = false;
	auto json = CallRustString(
	    [&](uint8_t **op, size_t *ol) {
		    return orch_load_directory(reinterpret_cast<const uint8_t *>(path.c_str()),
		                                path.size(), op, ol);
	        },
	    ok);
	if (!ok) {
		throw InvalidInputException("orch_register failed: " + json);
	}

	auto doc = yyjson_ns::yyjson_read(json.c_str(), json.size(), 0);
	if (!doc) {
		throw InvalidInputException("orch_register: invalid JSON from Rust");
	}
	auto root = yyjson_ns::yyjson_doc_get_root(doc);
	auto tasks = yyjson_ns::yyjson_obj_get(root, "tasks");

	std::ostringstream sql;
	sql << "DELETE FROM __orch__.tasks WHERE file_path LIKE "
	    << SqlEscape(path + "%") << ";\n";

	if (tasks && yyjson_ns::yyjson_is_arr(tasks)) {
		size_t idx, max;
		yyjson_ns::yyjson_val *t;
		yyjson_arr_foreach(tasks, idx, max, t) {
			auto get_str = [&](const char *k) -> string {
				auto v = yyjson_ns::yyjson_obj_get(t, k);
				if (!v || !yyjson_ns::yyjson_is_str(v)) return string();
				return string(yyjson_ns::yyjson_get_str(v));
			};
			auto get_int = [&](const char *k) -> int64_t {
				auto v = yyjson_ns::yyjson_obj_get(t, k);
				if (!v) return 0;
				if (yyjson_ns::yyjson_is_int(v)) return yyjson_ns::yyjson_get_int(v);
				if (yyjson_ns::yyjson_is_uint(v)) return (int64_t)yyjson_ns::yyjson_get_uint(v);
				return 0;
			};

			string name = get_str("name");
			if (name.empty()) continue;

			// Phase 14: serialize partitions + params back to JSON for
			// storage on __orch__.tasks. The Rust side already shaped them
			// inside the task JSON; we extract the sub-object and re-emit
			// it so RunSingleTask can rehydrate at execution time.
			auto get_sub_json = [&](const char *k) -> string {
				auto v = yyjson_ns::yyjson_obj_get(t, k);
				if (!v || yyjson_ns::yyjson_is_null(v)) return string();
				size_t l = 0;
				char *raw = yyjson_ns::yyjson_val_write(v, 0, &l);
				string out = raw ? string(raw, l) : string();
				free(raw);
				return out;
			};
			string partitions_json = get_sub_json("partitions");
			string params_json = get_sub_json("params");

			sql << "INSERT OR REPLACE INTO __orch__.tasks "
			    << "(name, description, owner, sql, inputs, outputs, depends_on, schedule_cron, "
			    << "retries, timeout_seconds, incremental_by, tags, file_path, "
			    << "partitions_json, params_json) VALUES ("
			    << SqlEscape(name) << ", "
			    << SqlEscape(get_str("description")) << ", "
			    << SqlEscape(get_str("owner")) << ", "
			    << SqlEscape(get_str("sql")) << ", "
			    << SqlArrayLiteral(yyjson_ns::yyjson_obj_get(t, "inputs")) << ", "
			    << SqlArrayLiteral(yyjson_ns::yyjson_obj_get(t, "outputs")) << ", "
			    << SqlArrayLiteral(yyjson_ns::yyjson_obj_get(t, "depends_on")) << ", "
			    << SqlEscape(get_str("schedule")) << ", "
			    << get_int("retries") << ", ";

			auto timeout_v = yyjson_ns::yyjson_obj_get(t, "timeout_seconds");
			if (timeout_v && (yyjson_ns::yyjson_is_int(timeout_v) || yyjson_ns::yyjson_is_uint(timeout_v))) {
				sql << get_int("timeout_seconds");
			} else {
				sql << "NULL";
			}
			sql << ", " << SqlEscape(get_str("incremental_by")) << ", "
			    << SqlArrayLiteral(yyjson_ns::yyjson_obj_get(t, "tags")) << ", "
			    << SqlEscape(get_str("file_path")) << ", "
			    << (partitions_json.empty() || partitions_json == "null"
			            ? string("NULL")
			            : SqlEscape(partitions_json))
			    << ", "
			    << (params_json.empty() || params_json == "null"
			            ? string("NULL")
			            : SqlEscape(params_json))
			    << ");\n";

			// Save tests
			sql << "DELETE FROM __orch__.tests WHERE task_name = " << SqlEscape(name) << ";\n";
			auto tests = yyjson_ns::yyjson_obj_get(t, "tests");
			if (tests && yyjson_ns::yyjson_is_arr(tests)) {
				size_t tidx, tmax;
				yyjson_ns::yyjson_val *tv;
				int counter = 0;
				yyjson_arr_foreach(tests, tidx, tmax, tv) {
					auto q = yyjson_ns::yyjson_obj_get(tv, "query");
					auto a = yyjson_ns::yyjson_obj_get(tv, "assertion");
					if (!q || !a) continue;
					sql << "INSERT INTO __orch__.tests (task_name, test_idx, query, assertion) VALUES ("
					    << SqlEscape(name) << ", " << counter << ", "
					    << SqlEscape(yyjson_ns::yyjson_get_str(q) ? yyjson_ns::yyjson_get_str(q) : "")
					    << ", "
					    << SqlEscape(yyjson_ns::yyjson_get_str(a) ? yyjson_ns::yyjson_get_str(a) : "")
					    << ");\n";
					counter++;
				}
			}

			// ----------------------------------------------------------
			// Phase 13: Asset auto-population.
			// If the task carries an explicit `@asset` header, upsert one
			// Asset row. Otherwise, fall back to `@outputs`: one Asset row
			// per output table (kind='table'), preserving Phase 0-9
			// behavior as a zero-config default.
			// ----------------------------------------------------------
			string task_sql = get_str("sql");
			string code_version = ComputeCodeVersion(task_sql);
			string task_desc = get_str("description");
			string task_owner = get_str("owner");
			string asset_name = get_str("asset_name");
			string asset_kind = get_str("asset_kind");
			string asset_group = get_str("asset_group");
			string asset_owner = get_str("asset_owner");
			string asset_desc = get_str("asset_description");
			auto asset_tags_json = yyjson_ns::yyjson_obj_get(t, "asset_tags");
			auto outputs_json = yyjson_ns::yyjson_obj_get(t, "outputs");

			// Helper to render the upsert for one Asset row. Re-registering a
			// task UPDATEs the row (preserves created_at via DO UPDATE).
			auto emit_asset_upsert = [&](const string &a_name,
			                              const string &a_kind,
			                              const string &a_group,
			                              const string &a_owner,
			                              const string &a_desc,
			                              const string &tags_literal) {
				sql << "INSERT INTO __orch__.assets "
				    << "(name, kind, location, group_name, owner, description, "
				    << "code_version, defined_by_task, tags) VALUES ("
				    << SqlEscape(a_name) << ", "
				    << SqlEscape(a_kind) << ", "
				    << "NULL, "
				    << (a_group.empty() ? string("NULL") : SqlEscape(a_group)) << ", "
				    << (a_owner.empty() ? string("NULL") : SqlEscape(a_owner)) << ", "
				    << (a_desc.empty() ? string("NULL") : SqlEscape(a_desc)) << ", "
				    << SqlEscape(code_version) << ", "
				    << SqlEscape(name) << ", "
				    << tags_literal
				    << ") ON CONFLICT (name) DO UPDATE SET "
				    << "kind=EXCLUDED.kind, "
				    << "group_name=EXCLUDED.group_name, "
				    << "owner=EXCLUDED.owner, "
				    << "description=EXCLUDED.description, "
				    << "code_version=EXCLUDED.code_version, "
				    << "defined_by_task=EXCLUDED.defined_by_task, "
				    << "tags=EXCLUDED.tags;\n";
			};

			// Determine the "primary asset" for this task so Phase 14 can
			// expand partition keys against it. Mirrors the asset upsert
			// fan-out: explicit `@asset name=...` wins, else the first
			// `@outputs` entry.
			string primary_asset;
			if (!asset_name.empty()) {
				string kind = asset_kind.empty() ? string("table") : asset_kind;
				string owner = asset_owner.empty() ? task_owner : asset_owner;
				string desc = asset_desc.empty() ? task_desc : asset_desc;
				string tags_lit = SqlArrayLiteral(asset_tags_json);
				emit_asset_upsert(asset_name, kind, asset_group, owner, desc, tags_lit);
				primary_asset = asset_name;
			} else if (outputs_json && yyjson_ns::yyjson_is_arr(outputs_json)) {
				// Backward-compat: each @outputs entry becomes its own Asset.
				size_t oidx, omax;
				yyjson_ns::yyjson_val *ov;
				yyjson_arr_foreach(outputs_json, oidx, omax, ov) {
					const char *s = yyjson_ns::yyjson_get_str(ov);
					if (!s || !*s) continue;
					string out_name(s);
					string tags_lit = SqlArrayLiteral(asset_tags_json);
					emit_asset_upsert(out_name, string("table"), asset_group,
					                   task_owner, task_desc, tags_lit);
					if (primary_asset.empty()) primary_asset = out_name;
				}
			}

			// Phase 14: if the task is partitioned and has at least one
			// asset, expand the partition definition into concrete keys
			// and INSERT OR IGNORE them into __orch__.asset_partitions.
			// Re-registration is idempotent on (asset_name, partition_key).
			if (!primary_asset.empty() && !partitions_json.empty() &&
			    partitions_json != "null") {
				bool exp_ok = false;
				auto rows_json = CallRustString(
				    [&](uint8_t **op, size_t *ol) {
					    return orch_partition_expand(
					        reinterpret_cast<const uint8_t *>(partitions_json.c_str()),
					        partitions_json.size(), nullptr, 0, op, ol);
				        },
				    exp_ok);
				if (exp_ok) {
					auto rdoc = yyjson_ns::yyjson_read(rows_json.c_str(), rows_json.size(), 0);
					if (rdoc) {
						auto rroot = yyjson_ns::yyjson_doc_get_root(rdoc);
						if (rroot && yyjson_ns::yyjson_is_arr(rroot)) {
							size_t ridx, rmax;
							yyjson_ns::yyjson_val *r;
							yyjson_arr_foreach(rroot, ridx, rmax, r) {
								auto kv = yyjson_ns::yyjson_obj_get(r, "key");
								auto dv = yyjson_ns::yyjson_obj_get(r, "dimension_values");
								const char *ks = kv ? yyjson_ns::yyjson_get_str(kv) : nullptr;
								if (!ks) continue;
								// Re-emit dimension_values as a JSON string.
								string dim_str;
								if (dv) {
									auto *mut = yyjson_ns::yyjson_val_mut_copy(nullptr, dv);
									if (mut) {
										size_t l = 0;
										char *raw =
										    yyjson_ns::yyjson_mut_val_write(mut, 0, &l);
										if (raw) {
											dim_str = string(raw, l);
											free(raw);
										}
									}
								}
								sql << "INSERT OR IGNORE INTO __orch__.asset_partitions "
								    << "(asset_name, partition_key, dimension_values) VALUES ("
								    << SqlEscape(primary_asset) << ", "
								    << SqlEscape(string(ks)) << ", "
								    << (dim_str.empty() ? string("NULL") : SqlEscape(dim_str))
								    << ");\n";
							}
						}
						yyjson_ns::yyjson_doc_free(rdoc);
					}
				}
			}
		}
	}
	yyjson_ns::yyjson_doc_free(doc);

	// Execute all the generated INSERT/DELETE statements directly.
	auto exec_result = user_con.Query(sql.str());
	if (exec_result->HasError()) {
		throw InvalidInputException("orch_register exec failed: " + exec_result->GetError());
	}
}

// ========================================================================
// PRAGMA: orch_run — execute all registered tasks in DAG order
// ========================================================================

static string IsoNow() {
	auto now = std::chrono::system_clock::now();
	auto t = std::chrono::system_clock::to_time_t(now);
	auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
	std::tm tm;
#ifdef _WIN32
	gmtime_s(&tm, &t);
#else
	gmtime_r(&t, &tm);
#endif
	char buf[32];
	std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S", &tm);
	std::ostringstream s;
	s << buf << "." << std::setfill('0') << std::setw(3) << ms.count() << "Z";
	return s.str();
}

// Encode a string for JSON (escape quotes, control chars).
static string JsonEscape(const string &s) {
	std::ostringstream o;
	o << '"';
	for (char c : s) {
		switch (c) {
			case '"': o << "\\\""; break;
			case '\\': o << "\\\\"; break;
			case '\n': o << "\\n"; break;
			case '\r': o << "\\r"; break;
			case '\t': o << "\\t"; break;
			default:
				if (static_cast<unsigned char>(c) < 0x20) {
					o << "\\u" << std::hex << std::setw(4) << std::setfill('0') << (int)c << std::dec;
				} else {
					o << c;
				}
		}
	}
	o << '"';
	return o.str();
}

// Render a Value of type VARCHAR[] as a JSON array literal.
static string ListValueToJson(const Value &v) {
	if (v.IsNull()) return "[]";
	auto &children = ListValue::GetChildren(v);
	std::ostringstream o;
	o << "[";
	bool first = true;
	for (auto &c : children) {
		if (!first) o << ",";
		first = false;
		o << JsonEscape(c.ToString());
	}
	o << "]";
	return o.str();
}

// Build a JSON array of tasks from the __orch__.tasks table for Rust DAG building.
static string TasksToJson(Connection &con) {
	auto result = con.Query(
	    "SELECT name, sql, inputs, outputs, depends_on, retries, incremental_by "
	    "FROM __orch__.tasks");
	if (result->HasError()) {
		throw InvalidInputException("failed to read __orch__.tasks: " + result->GetError());
	}
	std::ostringstream oss;
	oss << "[";
	bool first = true;
	for (idx_t i = 0; i < result->RowCount(); i++) {
		if (!first) oss << ",";
		first = false;
		auto retries_v = result->GetValue(5, i);
		auto incr_v = result->GetValue(6, i);
		oss << "{"
		    << "\"name\":" << JsonEscape(result->GetValue(0, i).ToString())
		    << ",\"sql\":" << JsonEscape(result->GetValue(1, i).ToString())
		    << ",\"inputs\":" << ListValueToJson(result->GetValue(2, i))
		    << ",\"outputs\":" << ListValueToJson(result->GetValue(3, i))
		    << ",\"depends_on\":" << ListValueToJson(result->GetValue(4, i))
		    << ",\"retries\":" << (retries_v.IsNull() ? 0 : retries_v.GetValue<int32_t>())
		    << ",\"incremental_by\":"
		    << (incr_v.IsNull() ? "null" : JsonEscape(incr_v.ToString()))
		    << "}";
	}
	oss << "]";
	return oss.str();
}

struct TaskRow {
	string name;
	string sql;
	int retries = 0;
	string incremental_by;
	std::vector<string> tests; // [query, assertion, query, assertion, ...]
	// Phase 14: hydrated from __orch__.tasks. Both NULL/empty when the task
	// is unpartitioned / declares no @param headers.
	string partitions_json;
	string params_json;
};

static std::vector<TaskRow> LoadTaskRows(Connection &con) {
	auto result = con.Query(
	    "SELECT name, sql, retries, incremental_by, partitions_json, params_json "
	    "FROM __orch__.tasks");
	if (result->HasError()) {
		throw InvalidInputException("failed: " + result->GetError());
	}
	std::vector<TaskRow> out;
	for (idx_t i = 0; i < result->RowCount(); i++) {
		TaskRow r;
		r.name = result->GetValue(0, i).ToString();
		r.sql = result->GetValue(1, i).ToString();
		auto rv = result->GetValue(2, i);
		r.retries = rv.IsNull() ? 0 : rv.GetValue<int32_t>();
		auto iv = result->GetValue(3, i);
		r.incremental_by = iv.IsNull() ? string() : iv.ToString();
		auto pv = result->GetValue(4, i);
		r.partitions_json = pv.IsNull() ? string() : pv.ToString();
		auto pmv = result->GetValue(5, i);
		r.params_json = pmv.IsNull() ? string() : pmv.ToString();
		out.push_back(std::move(r));
	}
	return out;
}

// Look up last successful last_processed_at for a task. Returns "1970-01-01 00:00:00" if none.
static string LookupLastProcessedAt(Connection &con, const string &task_name) {
	std::ostringstream q;
	q << "SELECT COALESCE(max(last_processed_at), TIMESTAMP '1970-01-01 00:00:00')::VARCHAR "
	  << "FROM __orch__.runs WHERE task_name = " << SqlEscape(task_name)
	  << " AND status = 'success'";
	auto r = con.Query(q.str());
	if (r->HasError() || r->RowCount() == 0) return "1970-01-01 00:00:00";
	auto v = r->GetValue(0, 0);
	return v.IsNull() ? "1970-01-01 00:00:00" : v.ToString();
}

// Run @test queries for a task. Returns empty string on success, error message on failure.
static string RunTaskTests(Connection &con, const string &task_name) {
	auto r = con.Query(
	    "SELECT tests FROM __orch__.tasks WHERE name = " + SqlEscape(task_name));
	if (r->HasError() || r->RowCount() == 0) return "";
	// Tests stored as JSON array of {query, assertion} objects via separate column.
	// MVP: tests are stored as serialized JSON in a VARCHAR column.
	// For Phase 7 we just skip this — actual test execution is in OrchTestPragma.
	return "";
}

// Phase 14: helper that bridges a Rust-side ParamType token to a DuckDB
// Value. The Value::FromString equivalents would also work but going via
// LogicalType keeps us explicit about coercion failures.
static Value CoerceParam(const string &raw, const string &ty_token) {
	try {
		if (ty_token == "Date") return Value::DATE(Date::FromString(raw));
		if (ty_token == "Timestamp") {
			return Value::TIMESTAMP(Timestamp::FromString(raw, false));
		}
		if (ty_token == "Integer") return Value::INTEGER((int32_t)std::stoll(raw));
		if (ty_token == "BigInt") return Value::BIGINT((int64_t)std::stoll(raw));
		if (ty_token == "Double") return Value::DOUBLE(std::stod(raw));
		if (ty_token == "Boolean") {
			string l = raw;
			std::transform(l.begin(), l.end(), l.begin(),
			               [](unsigned char c) { return std::tolower(c); });
			return Value::BOOLEAN(l == "true" || l == "1");
		}
	} catch (...) {
		// Fall through to VARCHAR (DuckDB will cast at execute time).
	}
	return Value(raw);
}

// Phase 14: build the parameter map for PREPARE binding. Sources values
// from `bound_params` (typically `{partition_key: "2026-05-17"}`). Returns
// a name→Value map keyed by declared ParamSpec.name; declared params with
// no provided value are skipped (DuckDB will fail prepare if SQL truly
// needs them).
static unordered_map<string, Value>
BuildParamValues(const string &params_json,
                 const std::map<string, string> &bound_params) {
	unordered_map<string, Value> out;
	if (params_json.empty() || params_json == "null") {
		return out;
	}
	auto doc = yyjson_ns::yyjson_read(params_json.c_str(), params_json.size(), 0);
	if (!doc) return out;
	auto root = yyjson_ns::yyjson_doc_get_root(doc);
	if (root && yyjson_ns::yyjson_is_arr(root)) {
		size_t i, m;
		yyjson_ns::yyjson_val *p;
		yyjson_arr_foreach(root, i, m, p) {
			auto nv = yyjson_ns::yyjson_obj_get(p, "name");
			auto tv = yyjson_ns::yyjson_obj_get(p, "ty");
			const char *ns = nv ? yyjson_ns::yyjson_get_str(nv) : nullptr;
			const char *ts = tv ? yyjson_ns::yyjson_get_str(tv) : "Varchar";
			if (!ns) continue;
			auto it = bound_params.find(string(ns));
			if (it == bound_params.end()) continue;
			out.emplace(string(ns), CoerceParam(it->second, string(ts ? ts : "Varchar")));
		}
	}
	yyjson_ns::yyjson_doc_free(doc);
	return out;
}

// Run a single task in `con`. Updates state tables and emits OL events.
// `partition_key` is `__default__` for unpartitioned tasks and a concrete
// key (e.g. `2026-05-17`, `2026-05-17|jp`) for partitioned runs. When set
// to a non-default value AND the task carries `params`, the SQL is run via
// `PREPARE`+bind so `$partition_key` / `$partition_<dim>` resolve natively.
// Returns true on success, false on failure.
static bool RunSingleTask(Connection &con, const TaskRow &task, const string &pipeline_run_id,
                          const string &tasks_json,
                          const string &partition_key = "__default__") {
	auto run_uuid = con.Query("SELECT uuid()::VARCHAR")->GetValue(0, 0).ToString();
	string started = IsoNow();

	// Phase 9 + DuckLake: lookup task inputs/outputs and resolve per-dataset namespace
	// (uses Catalog::GetAttached().tags["data_path"] when available).
	std::vector<OlDataset> task_inputs, task_outputs;
	{
		auto tr = con.Query("SELECT inputs, outputs FROM __orch__.tasks WHERE name = " +
		                    SqlEscape(task.name));
		if (!tr->HasError() && tr->RowCount() > 0) {
			auto iv = tr->GetValue(0, 0);
			auto ov = tr->GetValue(1, 0);
			if (!iv.IsNull()) {
				for (auto &c : ListValue::GetChildren(iv)) {
					string nm = c.ToString();
					task_inputs.push_back({ResolveDatasetNamespace(*con.context, nm), nm});
				}
			}
			if (!ov.IsNull()) {
				for (auto &c : ListValue::GetChildren(ov)) {
					string nm = c.ToString();
					task_outputs.push_back({ResolveDatasetNamespace(*con.context, nm), nm});
				}
			}
		}
	}
	EmitOlEvent(OlEventJson("START", started, run_uuid, pipeline_run_id, g_orch_namespace,
	                         task.name, task_inputs, task_outputs, ""));

	string sql_to_run = task.sql;
	string last_at = LookupLastProcessedAt(con, task.name);
	string now_ts;
	{
		auto nr = con.Query("SELECT current_timestamp::VARCHAR");
		now_ts = nr->GetValue(0, 0).ToString();
	}
	std::ostringstream vars;
	vars << "{\"last_processed_at\":" << JsonEscape(last_at) << ",\"now\":" << JsonEscape(now_ts)
	     << ",\"run_id\":" << JsonEscape(run_uuid) << "}";
	string vars_json = vars.str();
	bool sub_ok = false;
	auto substituted = CallRustString(
	    [&](uint8_t **op, size_t *ol) {
		    return orch_substitute_vars(
		        reinterpret_cast<const uint8_t *>(task.sql.c_str()), task.sql.size(),
		        reinterpret_cast<const uint8_t *>(vars_json.c_str()), vars_json.size(), op, ol);
	        },
	    sub_ok);
	if (sub_ok) sql_to_run = substituted;

	// Phase 14: when running a specific partition, build the bind map for
	// `$partition_key` and (for Multi) `$partition_<dim>` placeholders so
	// the SQL is executed via DuckDB's PREPARE + bind path rather than
	// raw textual interpolation. Unpartitioned tasks (partition_key=
	// "__default__") fall through to the plain Query path, preserving
	// Phase 13 behaviour.
	std::map<string, string> bound;
	bool use_prepared = false;
	if (partition_key != "__default__" && !task.params_json.empty()) {
		use_prepared = true;
		bound["partition_key"] = partition_key;
		if (!task.partitions_json.empty() && task.partitions_json != "null") {
			bool split_ok = false;
			auto split_json = CallRustString(
			    [&](uint8_t **op, size_t *ol) {
				    return orch_partition_split_key(
				        reinterpret_cast<const uint8_t *>(task.partitions_json.c_str()),
				        task.partitions_json.size(),
				        reinterpret_cast<const uint8_t *>(partition_key.c_str()),
				        partition_key.size(), op, ol);
			        },
			    split_ok);
			if (split_ok) {
				auto sd = yyjson_ns::yyjson_read(split_json.c_str(), split_json.size(), 0);
				if (sd) {
					auto sroot = yyjson_ns::yyjson_doc_get_root(sd);
					if (sroot && yyjson_ns::yyjson_is_arr(sroot)) {
						size_t i, m;
						yyjson_ns::yyjson_val *p;
						yyjson_arr_foreach(sroot, i, m, p) {
							auto nv = yyjson_ns::yyjson_obj_get(p, "name");
							auto vv = yyjson_ns::yyjson_obj_get(p, "value");
							const char *ns = nv ? yyjson_ns::yyjson_get_str(nv) : nullptr;
							const char *vs = vv ? yyjson_ns::yyjson_get_str(vv) : nullptr;
							if (!ns || !vs) continue;
							// Both `partition_key` and dimensioned aliases
							// (e.g. `partition_date`, `partition_region`)
							// can appear in @param headers — bind whichever
							// the user declared.
							bound[string(ns)] = string(vs);
							bound[string("partition_") + string(ns)] = string(vs);
						}
					}
					yyjson_ns::yyjson_doc_free(sd);
				}
			}
		}
	}

	int retries_left = task.retries;
	int retry_count = 0;
	bool success = false;
	string error_msg;
	while (true) {
		bool ran_ok = false;
		string this_err;
		if (use_prepared) {
			// PREPARE supports only one statement, but task SQL often has
			// multiple (CREATE; DELETE; INSERT;). Split via DuckDB's parser
			// and prepare/execute each statement, binding the named params
			// only to statements that actually declare them.
			auto stmts = con.ExtractStatements(sql_to_run);
			auto values = BuildParamValues(task.params_json, bound);
			ran_ok = !stmts.empty();
			for (auto &stmt : stmts) {
				string one = stmt->query.substr(stmt->stmt_location, stmt->stmt_length);
				auto prepared = con.Prepare(one);
				if (prepared->HasError()) {
					this_err = prepared->GetError();
					ran_ok = false;
					break;
				}
				case_insensitive_map_t<BoundParameterData> bind_map;
				for (auto &kv : values) {
					if (prepared->named_param_map.find(kv.first) !=
					    prepared->named_param_map.end()) {
						bind_map.emplace(kv.first, BoundParameterData(kv.second));
					}
				}
				auto qres = prepared->Execute(bind_map);
				if (!qres || qres->HasError()) {
					this_err = qres ? qres->GetError() : "prepared execute returned null";
					ran_ok = false;
					break;
				}
			}
		} else {
			auto qres = con.Query(sql_to_run);
			if (!qres->HasError()) {
				ran_ok = true;
			} else {
				this_err = qres->GetError();
			}
		}
		if (ran_ok) {
			success = true;
			break;
		}
		error_msg = this_err;
		if (retries_left <= 0) break;
		retries_left--;
		retry_count++;
	}

	string finished = IsoNow();
	if (success) {
		string new_watermark;
		if (!task.incremental_by.empty()) {
			auto out_r = con.Query("SELECT outputs[1] FROM __orch__.tasks WHERE name = " +
			                        SqlEscape(task.name));
			if (!out_r->HasError() && out_r->RowCount() > 0 && !out_r->GetValue(0, 0).IsNull()) {
				string out_table = out_r->GetValue(0, 0).ToString();
				auto wm = con.Query("SELECT max(" + task.incremental_by + ")::VARCHAR FROM " +
				                     out_table);
				if (!wm->HasError() && wm->RowCount() > 0 && !wm->GetValue(0, 0).IsNull()) {
					new_watermark = wm->GetValue(0, 0).ToString();
				}
			}
		}
		std::ostringstream ins;
		ins << "INSERT INTO __orch__.runs (run_id, pipeline_run_id, task_name, started_at, "
		    << "finished_at, status, rows_count, retry_count, last_processed_at) VALUES ("
		    << SqlEscape(run_uuid) << ", " << SqlEscape(pipeline_run_id) << ", "
		    << SqlEscape(task.name) << ", '" << started << "', '" << finished
		    << "', 'success', 0, " << retry_count << ", "
		    << (new_watermark.empty() ? string("NULL") : ("'" + new_watermark + "'")) << ");";
		con.Query(ins.str());

		// Phase 13: record successful materialization for every Asset this
		// task produces. Sourced from __orch__.assets where defined_by_task
		// matches — covers both the explicit `@asset` row and the per-output
		// backward-compat fan-out from `@outputs`. Phase 14: partition_key
		// reflects the specific partition just run (defaults to
		// `__default__` for unpartitioned tasks).
		{
			std::ostringstream mat;
			mat << "INSERT OR IGNORE INTO __orch__.asset_materializations "
			    << "(asset_name, partition_key, materialized_at, run_id, rows, bytes, status) "
			    << "SELECT name, " << SqlEscape(partition_key)
			    << ", TIMESTAMP '" << finished << "', "
			    << "CAST(" << SqlEscape(run_uuid) << " AS UUID), NULL, NULL, 'success' "
			    << "FROM __orch__.assets WHERE defined_by_task = "
			    << SqlEscape(task.name) << ";";
			con.Query(mat.str());
		}
		// Phase 14 fix: populate __orch__.lineage_edges from each
		// successful run too, not just from OrchRunPragma's DAG-build path.
		// Partition-driven runs (OrchRunPartition/OrchBackfill) bypass the
		// DAG executor entirely, so without this they'd never produce
		// lineage edges (and therefore no asset_edges either).
		for (auto &out_ds : task_outputs) {
			string out_full = out_ds.name;  // OlDataset.name is the bare table name
			for (auto &in_ds : task_inputs) {
				string in_full = in_ds.name;
				std::ostringstream le;
				le << "INSERT OR IGNORE INTO __orch__.lineage_edges "
				   << "(src_dataset, dst_dataset, via_task, source) VALUES ("
				   << SqlEscape(in_full) << ", " << SqlEscape(out_full)
				   << ", " << SqlEscape(task.name) << ", 'sql_parser');";
				con.Query(le.str());
			}
		}
		// Project newly-inserted lineage_edges into asset_edges (idempotent).
		con.Query(
		    "INSERT OR IGNORE INTO __orch__.asset_edges "
		    "(upstream_asset, downstream_asset, via_task, edge_type) "
		    "SELECT le.src_dataset, le.dst_dataset, le.via_task, "
		    "       COALESCE(NULLIF(le.transform_type, ''), 'direct') "
		    "FROM __orch__.lineage_edges le "
		    "WHERE EXISTS (SELECT 1 FROM __orch__.assets a WHERE a.name = le.src_dataset) "
		    "  AND EXISTS (SELECT 1 FROM __orch__.assets a WHERE a.name = le.dst_dataset);");

		// Phase column-lineage: extract column-level dependencies from the task SQL
		// (uses DuckDB's catalog DESCRIBE for SELECT * resolution).
		string cl_json;
		try {
			cl_json = RecordColumnLineage(con, sql_to_run, task.name, task_inputs);
		} catch (...) {
			// best-effort; never let lineage extraction fail the task
		}
		// Emit COMPLETE with optional columnLineage facet attached to the first output.
		string complete_event = OlEventJson("COMPLETE", finished, run_uuid, pipeline_run_id,
		                                     g_orch_namespace, task.name, task_inputs,
		                                     task_outputs, "");
		if (!cl_json.empty() && !task_outputs.empty()) {
			string facet = BuildColumnLineageFacet(cl_json, task_inputs);
			if (!facet.empty()) {
				// Patch the first output to include the columnLineage facet.
				// OL output JSON shape: {"namespace":...,"name":...} — we add ",facets":{"columnLineage":<facet>}
				// We do textual splice: replace the closing `}` of the first output dataset.
				string needle = "{\"namespace\":" + JsonEscape(task_outputs[0].ns) + ",\"name\":" +
				                 JsonEscape(task_outputs[0].name) + "}";
				size_t pos = complete_event.find(needle);
				if (pos != string::npos) {
					string replacement = "{\"namespace\":" + JsonEscape(task_outputs[0].ns) +
					                      ",\"name\":" + JsonEscape(task_outputs[0].name) +
					                      ",\"facets\":{\"columnLineage\":" + facet + "}}";
					complete_event.replace(pos, needle.length(), replacement);
				}
			}
		}
		EmitOlEvent(complete_event);
	} else {
		std::ostringstream ins;
		ins << "INSERT INTO __orch__.runs (run_id, pipeline_run_id, task_name, started_at, "
		    << "finished_at, status, error_message, retry_count) VALUES ("
		    << SqlEscape(run_uuid) << ", " << SqlEscape(pipeline_run_id) << ", "
		    << SqlEscape(task.name) << ", '" << started << "', '" << finished << "', 'failed', "
		    << SqlEscape(error_msg) << ", " << retry_count << ");";
		con.Query(ins.str());

		// Phase 13: record a 'failed' materialization row for each declared
		// Asset so failures show up in `asset materializations` history.
		// Phase 14: scoped to the partition that just failed.
		{
			std::ostringstream mat;
			mat << "INSERT OR IGNORE INTO __orch__.asset_materializations "
			    << "(asset_name, partition_key, materialized_at, run_id, rows, bytes, status) "
			    << "SELECT name, " << SqlEscape(partition_key)
			    << ", TIMESTAMP '" << finished << "', "
			    << "CAST(" << SqlEscape(run_uuid) << " AS UUID), NULL, NULL, 'failed' "
			    << "FROM __orch__.assets WHERE defined_by_task = "
			    << SqlEscape(task.name) << ";";
			con.Query(mat.str());
		}

		EmitOlEvent(OlEventJson("FAIL", finished, run_uuid, pipeline_run_id, g_orch_namespace,
		                         task.name, task_inputs, task_outputs, error_msg));
	}
	return success;
}

static void OrchRunPragma(ClientContext &context, const FunctionParameters &parameters) {
	Connection con(*context.db);
	EnsureOrchSchema(con);

	auto tasks_json = TasksToJson(con);
	bool ok = false;
	auto dag_json = CallRustString(
	    [&](uint8_t **op, size_t *ol) {
		    return orch_build_dag(reinterpret_cast<const uint8_t *>(tasks_json.c_str()),
		                          tasks_json.size(), op, ol);
	        },
	    ok);
	if (!ok) {
		throw InvalidInputException("DAG build failed: " + dag_json);
	}

	// Build layers for parallel execution
	auto layers_json = CallRustString(
	    [&](uint8_t **op, size_t *ol) {
		    return orch_topo_layers(reinterpret_cast<const uint8_t *>(tasks_json.c_str()),
		                             tasks_json.size(), op, ol);
	        },
	    ok);
	std::vector<std::vector<string>> layers;
	if (ok) {
		auto ld = yyjson_ns::yyjson_read(layers_json.c_str(), layers_json.size(), 0);
		auto lr = yyjson_ns::yyjson_doc_get_root(ld);
		if (lr && yyjson_ns::yyjson_is_arr(lr)) {
			size_t i, m;
			yyjson_ns::yyjson_val *layer;
			yyjson_arr_foreach(lr, i, m, layer) {
				std::vector<string> names;
				if (yyjson_ns::yyjson_is_arr(layer)) {
					size_t j, mm;
					yyjson_ns::yyjson_val *v;
					yyjson_arr_foreach(layer, j, mm, v) {
						const char *s = yyjson_ns::yyjson_get_str(v);
						if (s) names.emplace_back(s);
					}
				}
				layers.push_back(std::move(names));
			}
		}
		yyjson_ns::yyjson_doc_free(ld);
	}

	auto rows = LoadTaskRows(con);
	std::map<string, TaskRow> by_name;
	for (auto &r : rows) by_name[r.name] = r;

	auto uuid_result = con.Query("SELECT uuid()::VARCHAR");
	string pipeline_run_id = uuid_result->GetValue(0, 0).ToString();

	std::mutex sk_mu;
	std::set<string> failed_tasks;
	std::set<string> skipped_tasks;
	std::vector<std::pair<string, string>> statuses;

	int max_par = g_max_parallel.load();

	for (auto &layer : layers) {
		// Filter out skipped tasks in this layer
		std::vector<string> to_run;
		std::vector<string> to_skip;
		for (auto &name : layer) {
			std::lock_guard<std::mutex> lk(sk_mu);
			if (skipped_tasks.count(name)) {
				to_skip.push_back(name);
			} else {
				to_run.push_back(name);
			}
		}

		// Mark skipped tasks
		for (auto &name : to_skip) {
			auto run_uuid = con.Query("SELECT uuid()::VARCHAR")->GetValue(0, 0).ToString();
			std::ostringstream ins;
			ins << "INSERT INTO __orch__.runs (run_id, pipeline_run_id, task_name, started_at, "
			    << "finished_at, status, rows_count, retry_count) VALUES ("
			    << SqlEscape(run_uuid) << ", " << SqlEscape(pipeline_run_id) << ", "
			    << SqlEscape(name) << ", '" << IsoNow() << "', '" << IsoNow()
			    << "', 'skipped', 0, 0);";
			con.Query(ins.str());
			statuses.push_back({name, "skipped"});
		}

		// Run layer: parallel if max_par > 1 and layer has > 1 task
		if (to_run.size() > 1 && max_par > 1) {
			std::vector<std::thread> threads;
			std::mutex stat_mu;
			size_t batch = (size_t)max_par;
			for (size_t start = 0; start < to_run.size(); start += batch) {
				size_t end = std::min(start + batch, to_run.size());
				for (size_t i = start; i < end; i++) {
					string name = to_run[i];
					threads.emplace_back([&, name]() {
						try {
							Connection thread_con(*context.db);
							auto it = by_name.find(name);
							if (it == by_name.end()) return;
							bool s = RunSingleTask(thread_con, it->second, pipeline_run_id,
							                        tasks_json);
							{
								std::lock_guard<std::mutex> lk(stat_mu);
								statuses.push_back({name, s ? "success" : "failed"});
								if (!s) {
									std::lock_guard<std::mutex> lk2(sk_mu);
									failed_tasks.insert(name);
								}
							}
						} catch (...) {
							std::lock_guard<std::mutex> lk(sk_mu);
							failed_tasks.insert(name);
						}
					});
				}
				for (auto &t : threads) {
					if (t.joinable()) t.join();
				}
				threads.clear();
			}
		} else {
			for (auto &name : to_run) {
				auto it = by_name.find(name);
				if (it == by_name.end()) continue;
				bool s = RunSingleTask(con, it->second, pipeline_run_id, tasks_json);
				statuses.push_back({name, s ? "success" : "failed"});
				if (!s) {
					std::lock_guard<std::mutex> lk(sk_mu);
					failed_tasks.insert(name);
				}
			}
		}

		// Compute downstream skips for any failed tasks
		std::set<string> failed_now;
		{
			std::lock_guard<std::mutex> lk(sk_mu);
			failed_now = failed_tasks;
		}
		for (auto &name : failed_now) {
			bool ok2 = false;
			auto down_json = CallRustString(
			    [&](uint8_t **op, size_t *ol) {
				    return orch_downstream_of(
				        reinterpret_cast<const uint8_t *>(tasks_json.c_str()), tasks_json.size(),
				        reinterpret_cast<const uint8_t *>(name.c_str()), name.size(), op, ol);
			        },
			    ok2);
			if (ok2) {
				auto d = yyjson_ns::yyjson_read(down_json.c_str(), down_json.size(), 0);
				auto dr = yyjson_ns::yyjson_doc_get_root(d);
				if (dr && yyjson_ns::yyjson_is_arr(dr)) {
					size_t idx, m;
					yyjson_ns::yyjson_val *v;
					std::lock_guard<std::mutex> lk(sk_mu);
					yyjson_arr_foreach(dr, idx, m, v) {
						const char *s = yyjson_ns::yyjson_get_str(v);
						if (s) skipped_tasks.insert(s);
					}
				}
				yyjson_ns::yyjson_doc_free(d);
			}
		}
	}


	// Update lineage_edges
	auto doc2 = yyjson_ns::yyjson_read(dag_json.c_str(), dag_json.size(), 0);
	auto root2 = yyjson_ns::yyjson_doc_get_root(doc2);
	auto le = yyjson_ns::yyjson_obj_get(root2, "lineage_edges");
	if (le && yyjson_ns::yyjson_is_arr(le)) {
		size_t idx, max;
		yyjson_ns::yyjson_val *e;
		yyjson_arr_foreach(le, idx, max, e) {
			auto src_v = yyjson_ns::yyjson_obj_get(e, "src_dataset");
			auto dst_v = yyjson_ns::yyjson_obj_get(e, "dst_dataset");
			auto via_v = yyjson_ns::yyjson_obj_get(e, "via_task");
			if (!src_v || !dst_v || !via_v) continue;
			std::ostringstream upd;
			upd << "INSERT OR IGNORE INTO __orch__.lineage_edges "
			    << "(src_dataset, dst_dataset, via_task, source) VALUES ("
			    << SqlEscape(yyjson_ns::yyjson_get_str(src_v)) << ", "
			    << SqlEscape(yyjson_ns::yyjson_get_str(dst_v)) << ", "
			    << SqlEscape(yyjson_ns::yyjson_get_str(via_v)) << ", 'sql_parser');";
			con.Query(upd.str());
		}
	}
	auto te = yyjson_ns::yyjson_obj_get(root2, "task_edges");
	if (te && yyjson_ns::yyjson_is_arr(te)) {
		size_t idx, max;
		yyjson_ns::yyjson_val *e;
		yyjson_arr_foreach(te, idx, max, e) {
			auto from_v = yyjson_ns::yyjson_obj_get(e, "from");
			auto to_v = yyjson_ns::yyjson_obj_get(e, "to");
			if (!from_v || !to_v) continue;
			std::ostringstream upd;
			upd << "INSERT OR IGNORE INTO __orch__.task_edges (upstream, downstream) VALUES ("
			    << SqlEscape(yyjson_ns::yyjson_get_str(from_v)) << ", "
			    << SqlEscape(yyjson_ns::yyjson_get_str(to_v)) << ");";
			con.Query(upd.str());
		}
	}
	yyjson_ns::yyjson_doc_free(doc2);

	// Phase 13 m2: project __orch__.lineage_edges into __orch__.asset_edges.
	// Both ends must be registered Assets (auto-derived from @outputs or
	// explicitly via @asset) to count as a real Asset edge — anonymous
	// upstreams (e.g. raw source files not declared as Assets) get dropped
	// here. Idempotent: INSERT OR IGNORE keys on
	// (upstream_asset, downstream_asset, via_task).
	{
		const char *kProject =
		    "INSERT OR IGNORE INTO __orch__.asset_edges "
		    "(upstream_asset, downstream_asset, via_task, edge_type) "
		    "SELECT le.src_dataset, le.dst_dataset, le.via_task, "
		    "       COALESCE(NULLIF(le.transform_type, ''), 'direct') AS edge_type "
		    "FROM __orch__.lineage_edges le "
		    "WHERE EXISTS (SELECT 1 FROM __orch__.assets a WHERE a.name = le.src_dataset) "
		    "  AND EXISTS (SELECT 1 FROM __orch__.assets a WHERE a.name = le.dst_dataset);";
		con.Query(kProject);
	}
}

// ========================================================================
// PRAGMA: orch_test — run @test assertions
// ========================================================================

static bool EvalAssertion(Connection &con, const string &query, const string &assertion,
                          string &error_out) {
	auto r = con.Query(query);
	if (r->HasError()) {
		error_out = "test query error: " + r->GetError();
		return false;
	}
	// Parse assertion: "expect 0", "expect_gt 5", "expect_empty", "expect_non_empty"
	std::istringstream as(assertion);
	string verb;
	as >> verb;

	if (verb == "expect_empty") {
		if (r->RowCount() == 0) return true;
		error_out = "expected empty, got " + std::to_string(r->RowCount()) + " rows";
		return false;
	}
	if (verb == "expect_non_empty") {
		if (r->RowCount() > 0) return true;
		error_out = "expected non-empty";
		return false;
	}
	int64_t bound = 0;
	as >> bound;
	if (r->RowCount() == 0) {
		error_out = "expected single value, got 0 rows";
		return false;
	}
	auto v = r->GetValue(0, 0);
	int64_t actual = v.IsNull() ? 0 : v.GetValue<int64_t>();
	if (verb == "expect") {
		if (actual == bound) return true;
		error_out = "expected " + std::to_string(bound) + ", got " + std::to_string(actual);
		return false;
	}
	if (verb == "expect_gt") {
		if (actual > bound) return true;
		error_out = "expected > " + std::to_string(bound) + ", got " + std::to_string(actual);
		return false;
	}
	if (verb == "expect_lt") {
		if (actual < bound) return true;
		error_out = "expected < " + std::to_string(bound) + ", got " + std::to_string(actual);
		return false;
	}
	error_out = "unknown assertion: " + assertion;
	return false;
}

static void OrchTestPragma(ClientContext &context, const FunctionParameters &parameters) {
	Connection con(*context.db);
	EnsureOrchSchema(con);
	auto tests = con.Query(
	    "SELECT task_name, test_idx, query, assertion FROM __orch__.tests "
	    "ORDER BY task_name, test_idx");
	if (tests->HasError()) return;
	int passed = 0, failed = 0;
	for (idx_t i = 0; i < tests->RowCount(); i++) {
		string task = tests->GetValue(0, i).ToString();
		string q = tests->GetValue(2, i).ToString();
		string a = tests->GetValue(3, i).ToString();
		string err;
		bool ok = EvalAssertion(con, q, a, err);
		if (ok) {
			passed++;
		} else {
			failed++;
			Printer::Print("FAIL " + task + ": " + a + " — " + err);
		}
	}
	Printer::Print("Tests: " + std::to_string(passed) + " passed, " +
	               std::to_string(failed) + " failed");
}

// ========================================================================
// PRAGMA: orch_visualize — return Mermaid diagram via PRINT statement
// ========================================================================
//
// Pragma functions can't directly print, but they can return a SELECT statement.
// We return: SELECT '<mermaid>' AS mermaid;

static string OrchVisualizePragma(ClientContext &context, const FunctionParameters &parameters) {
	Connection con(*context.db);
	EnsureOrchSchema(con);
	auto tasks_json = TasksToJson(con);

	bool ok = false;
	auto dag_json = CallRustString(
	    [&](uint8_t **op, size_t *ol) {
		    return orch_build_dag(reinterpret_cast<const uint8_t *>(tasks_json.c_str()),
		                          tasks_json.size(), op, ol);
	        },
	    ok);
	if (!ok) {
		throw InvalidInputException("DAG build failed: " + dag_json);
	}

	int32_t mode = 0; // default lineage
	if (!parameters.values.empty()) {
		string mode_str = parameters.values[0].GetValue<string>();
		if (mode_str == "dag") mode = 1;
		else if (mode_str == "combined") mode = 2;
	}

	// Build statuses array from latest run
	std::ostringstream stats_sql;
	stats_sql << "SELECT to_json(list({task_name, status})) FROM ("
	          << "SELECT task_name, status FROM __orch__.runs "
	          << "QUALIFY row_number() OVER (PARTITION BY task_name ORDER BY started_at DESC) = 1)";
	string statuses_json = "[]";
	auto stat_result = con.Query(stats_sql.str());
	if (!stat_result->HasError() && stat_result->RowCount() > 0) {
		auto v = stat_result->GetValue(0, 0).ToString();
		if (!v.empty() && v != "NULL") {
			// Convert from {task_name: x, status: y} to [x, y]
			auto d = yyjson_ns::yyjson_read(v.c_str(), v.size(), 0);
			if (d) {
				auto r = yyjson_ns::yyjson_doc_get_root(d);
				std::ostringstream out;
				out << "[";
				size_t idx, max;
				yyjson_ns::yyjson_val *e;
				bool first = true;
				yyjson_arr_foreach(r, idx, max, e) {
					if (!first) out << ",";
					first = false;
					auto n = yyjson_ns::yyjson_obj_get(e, "task_name");
					auto s = yyjson_ns::yyjson_obj_get(e, "status");
					out << "[" << "\"" << (n ? yyjson_ns::yyjson_get_str(n) : "")
					    << "\",\"" << (s ? yyjson_ns::yyjson_get_str(s) : "") << "\"]";
				}
				out << "]";
				statuses_json = out.str();
				yyjson_ns::yyjson_doc_free(d);
			}
		}
	}

	auto mermaid = CallRustString(
	    [&](uint8_t **op, size_t *ol) {
		    return orch_render_mermaid(
		        reinterpret_cast<const uint8_t *>(dag_json.c_str()), dag_json.size(), mode,
		        reinterpret_cast<const uint8_t *>(statuses_json.c_str()), statuses_json.size(),
		        op, ol);
	        },
	    ok);
	if (!ok) {
		return "SELECT 'mermaid render failed' AS mermaid;";
	}

	std::ostringstream sql;
	sql << "SELECT " << SqlEscape(mermaid) << " AS mermaid;";
	return sql.str();
}

// ========================================================================
// Phase 13 m2: Asset read-side pragmas (`pragma_query_t`).
//
// All return a SELECT statement so duckdb prints rows like any other query
// and the Rust CLI can pipe through `.mode json`. Match the existing
// `orch_visualize` pattern — no new TableFunction infrastructure needed.
// ========================================================================

// Make sure the Asset schema exists before each query. Cheap (CREATE IF NOT
// EXISTS only) and protects against callers who hit asset pragmas before
// `orch_init` / `orch_register` have run.
static void EnsureAssetSchemaCheap(ClientContext &context) {
	Connection con(*context.db);
	EnsureOrchSchema(con);
}

// PRAGMA orch_asset_list()                — all assets.
// PRAGMA orch_asset_list_group(group_name) — filter by group_name (empty
//                                            string = all). Two-pragma split
//                                            sidesteps DuckDB pragma optional
//                                            positional-arg quirks.
static string OrchAssetListPragma(ClientContext &context, const FunctionParameters &parameters) {
	EnsureAssetSchemaCheap(context);
	(void)parameters;
	return "SELECT name, kind, group_name, owner, description, "
	       "code_version, defined_by_task, tags, created_at "
	       "FROM __orch__.assets ORDER BY name;";
}

static string OrchAssetListGroupPragma(ClientContext &context,
                                        const FunctionParameters &parameters) {
	EnsureAssetSchemaCheap(context);
	string group_filter;
	if (!parameters.values.empty() && !parameters.values[0].IsNull()) {
		group_filter = parameters.values[0].GetValue<string>();
	}
	std::ostringstream sql;
	sql << "SELECT name, kind, group_name, owner, description, "
	    << "code_version, defined_by_task, tags, created_at "
	    << "FROM __orch__.assets";
	if (!group_filter.empty()) {
		sql << " WHERE group_name = " << SqlEscape(group_filter);
	}
	sql << " ORDER BY name;";
	return sql.str();
}

static string OrchAssetShowPragma(ClientContext &context, const FunctionParameters &parameters) {
	EnsureAssetSchemaCheap(context);
	if (parameters.values.empty() || parameters.values[0].IsNull()) {
		throw InvalidInputException("orch_asset_show requires an asset name");
	}
	string name = parameters.values[0].GetValue<string>();
	std::ostringstream sql;
	sql << "SELECT name, kind, location, group_name, owner, description, "
	    << "code_version, defined_by_task, tags, created_at "
	    << "FROM __orch__.assets WHERE name = " << SqlEscape(name) << ";";
	return sql.str();
}

static string OrchAssetMaterializationsPragma(ClientContext &context,
                                                const FunctionParameters &parameters) {
	EnsureAssetSchemaCheap(context);
	if (parameters.values.empty() || parameters.values[0].IsNull()) {
		throw InvalidInputException(
		    "orch_asset_materializations requires an asset name");
	}
	string name = parameters.values[0].GetValue<string>();
	int64_t limit = 50;
	if (parameters.values.size() > 1 && !parameters.values[1].IsNull()) {
		limit = parameters.values[1].GetValue<int64_t>();
		if (limit <= 0) limit = 50;
	}
	std::ostringstream sql;
	sql << "SELECT asset_name, partition_key, materialized_at, run_id, "
	    << "rows, bytes, status FROM __orch__.asset_materializations "
	    << "WHERE asset_name = " << SqlEscape(name)
	    << " ORDER BY materialized_at DESC LIMIT " << limit << ";";
	return sql.str();
}

// Render Mermaid centered on `focal`. Pulls upstream + downstream edges
// (one hop each direction) from `__orch__.asset_edges` and hands off to the
// Rust Mermaid renderer via FFI. Returns a single-row SELECT '...' AS mermaid.
static string OrchAssetLineagePragma(ClientContext &context,
                                      const FunctionParameters &parameters) {
	EnsureAssetSchemaCheap(context);
	if (parameters.values.empty() || parameters.values[0].IsNull()) {
		throw InvalidInputException("orch_asset_lineage requires an asset name");
	}
	string focal = parameters.values[0].GetValue<string>();
	Connection con(*context.db);

	std::ostringstream eq;
	eq << "SELECT to_json(list({"
	   << "upstream_asset: upstream_asset, "
	   << "downstream_asset: downstream_asset, "
	   << "via_task: via_task, "
	   << "edge_type: edge_type})) "
	   << "FROM __orch__.asset_edges "
	   << "WHERE upstream_asset = " << SqlEscape(focal)
	   << " OR downstream_asset = " << SqlEscape(focal) << ";";
	string edges_json = "[]";
	auto er = con.Query(eq.str());
	if (!er->HasError() && er->RowCount() > 0 && !er->GetValue(0, 0).IsNull()) {
		auto v = er->GetValue(0, 0).ToString();
		if (!v.empty() && v != "NULL") {
			edges_json = v;
		}
	}

	bool ok = false;
	auto mermaid = CallRustString(
	    [&](uint8_t **op, size_t *ol) {
		    return orch_render_asset_lineage(
		        reinterpret_cast<const uint8_t *>(focal.c_str()), focal.size(),
		        reinterpret_cast<const uint8_t *>(edges_json.c_str()),
		        edges_json.size(), op, ol);
	        },
	    ok);
	if (!ok) {
		return "SELECT 'asset lineage render failed' AS mermaid;";
	}
	std::ostringstream sql;
	sql << "SELECT " << SqlEscape(mermaid) << " AS mermaid;";
	return sql.str();
}

// Best-effort per-Asset health: last materialization status + age (seconds),
// total/successful/failed runs in the last 24h. NULL columns for Assets that
// have never materialized.
static string OrchAssetHealthPragma(ClientContext &context,
                                     const FunctionParameters &parameters) {
	EnsureAssetSchemaCheap(context);
	(void)parameters;
	std::ostringstream sql;
	sql << "WITH latest AS ("
	    << "  SELECT asset_name, status AS last_status, materialized_at AS last_at "
	    << "  FROM __orch__.asset_materializations "
	    << "  QUALIFY row_number() OVER (PARTITION BY asset_name ORDER BY materialized_at DESC) = 1"
	    << "), recent AS ("
	    << "  SELECT asset_name, "
	    << "         count(*) AS runs_24h, "
	    << "         count(*) FILTER (WHERE status = 'failed') AS failed_24h, "
	    << "         count(*) FILTER (WHERE status = 'success') AS success_24h "
	    << "  FROM __orch__.asset_materializations "
	    << "  WHERE materialized_at >= current_timestamp::TIMESTAMP - INTERVAL 24 HOUR "
	    << "  GROUP BY asset_name"
	    << ") "
	    << "SELECT a.name AS asset_name, a.kind, a.group_name, "
	    << "       l.last_status, l.last_at, "
	    << "       CASE WHEN l.last_at IS NULL THEN NULL "
	    << "            ELSE epoch(current_timestamp::TIMESTAMP - l.last_at) END AS age_seconds, "
	    << "       COALESCE(r.runs_24h, 0)    AS runs_24h, "
	    << "       COALESCE(r.success_24h, 0) AS success_24h, "
	    << "       COALESCE(r.failed_24h, 0)  AS failed_24h "
	    << "FROM __orch__.assets a "
	    << "LEFT JOIN latest l ON l.asset_name = a.name "
	    << "LEFT JOIN recent r ON r.asset_name = a.name "
	    << "ORDER BY a.name;";
	return sql.str();
}

// ========================================================================
// Phase 14: Partition read-side pragmas + backfill executor.
// ========================================================================

// `PRAGMA orch_asset_partitions('asset.name')` — list every registered
// partition for an Asset alongside its most-recent materialization status
// (NULL when never materialized).
static string OrchAssetPartitionsPragma(ClientContext &context,
                                          const FunctionParameters &parameters) {
	EnsureAssetSchemaCheap(context);
	if (parameters.values.empty() || parameters.values[0].IsNull()) {
		throw InvalidInputException("orch_asset_partitions requires an asset name");
	}
	string name = parameters.values[0].GetValue<string>();
	std::ostringstream sql;
	sql << "WITH latest AS ("
	    << "  SELECT asset_name, partition_key, status, materialized_at "
	    << "  FROM __orch__.asset_materializations "
	    << "  WHERE asset_name = " << SqlEscape(name)
	    << "  QUALIFY row_number() OVER (PARTITION BY asset_name, partition_key "
	    << "                              ORDER BY materialized_at DESC) = 1"
	    << ") "
	    << "SELECT p.asset_name, p.partition_key, p.dimension_values, "
	    << "       l.status AS last_status, l.materialized_at AS last_materialized_at "
	    << "FROM __orch__.asset_partitions p "
	    << "LEFT JOIN latest l "
	    << "  ON l.asset_name = p.asset_name AND l.partition_key = p.partition_key "
	    << "WHERE p.asset_name = " << SqlEscape(name)
	    << " ORDER BY p.partition_key;";
	return sql.str();
}

// `PRAGMA orch_asset_partitions_calendar('asset.name')` — render the
// calendar-style ASCII string as a single SELECT row. The CLI surfaces
// this via `duck-orch asset partitions`.
static string OrchAssetPartitionsCalendarPragma(ClientContext &context,
                                                  const FunctionParameters &parameters) {
	EnsureAssetSchemaCheap(context);
	if (parameters.values.empty() || parameters.values[0].IsNull()) {
		throw InvalidInputException(
		    "orch_asset_partitions_calendar requires an asset name");
	}
	string name = parameters.values[0].GetValue<string>();
	Connection con(*context.db);

	// Look up the partition definition stored on the task that defines
	// this Asset.
	string def_json;
	{
		std::ostringstream q;
		q << "SELECT t.partitions_json FROM __orch__.tasks t "
		  << "JOIN __orch__.assets a ON a.defined_by_task = t.name "
		  << "WHERE a.name = " << SqlEscape(name)
		  << " AND t.partitions_json IS NOT NULL LIMIT 1;";
		auto r = con.Query(q.str());
		if (!r->HasError() && r->RowCount() > 0 && !r->GetValue(0, 0).IsNull()) {
			def_json = r->GetValue(0, 0).ToString();
		}
	}
	if (def_json.empty()) {
		std::ostringstream s;
		s << "SELECT 'Asset " << name << " is not partitioned' AS calendar;";
		return s.str();
	}

	// Join partition registry with the last status per key.
	string rows_json = "[]";
	{
		std::ostringstream q;
		q << "WITH latest AS ("
		  << "  SELECT partition_key, status "
		  << "  FROM __orch__.asset_materializations "
		  << "  WHERE asset_name = " << SqlEscape(name)
		  << "  QUALIFY row_number() OVER (PARTITION BY partition_key "
		  << "                              ORDER BY materialized_at DESC) = 1"
		  << ") "
		  << "SELECT to_json(list({key: p.partition_key, status: l.status})) "
		  << "FROM __orch__.asset_partitions p "
		  << "LEFT JOIN latest l USING (partition_key) "
		  << "WHERE p.asset_name = " << SqlEscape(name) << ";";
		auto r = con.Query(q.str());
		if (!r->HasError() && r->RowCount() > 0 && !r->GetValue(0, 0).IsNull()) {
			rows_json = r->GetValue(0, 0).ToString();
		}
	}

	bool ok = false;
	auto cal = CallRustString(
	    [&](uint8_t **op, size_t *ol) {
		    return orch_render_partition_calendar(
		        reinterpret_cast<const uint8_t *>(name.c_str()), name.size(),
		        reinterpret_cast<const uint8_t *>(def_json.c_str()), def_json.size(),
		        reinterpret_cast<const uint8_t *>(rows_json.c_str()), rows_json.size(),
		        op, ol);
	        },
	    ok);
	if (!ok) {
		return "SELECT 'calendar render failed' AS calendar;";
	}
	std::ostringstream sql;
	sql << "SELECT " << SqlEscape(cal) << " AS calendar;";
	return sql.str();
}

// Look up the task name that defines `asset_name` (via __orch__.assets
// defined_by_task). Returns empty string if not found.
static string LookupDefiningTask(Connection &con, const string &asset_name) {
	std::ostringstream q;
	q << "SELECT defined_by_task FROM __orch__.assets WHERE name = "
	  << SqlEscape(asset_name) << " LIMIT 1;";
	auto r = con.Query(q.str());
	if (r->HasError() || r->RowCount() == 0 || r->GetValue(0, 0).IsNull()) {
		return string();
	}
	return r->GetValue(0, 0).ToString();
}

// Shared backfill driver: re-run `task` for every partition in `keys`.
// Sequential; failures are recorded but do not stop iteration so the rest
// of the backfill completes (CLI surfaces the per-partition status).
static void RunBackfillKeys(Connection &con, const TaskRow &task,
                             const std::vector<string> &keys) {
	auto pipeline_uuid = con.Query("SELECT uuid()::VARCHAR")->GetValue(0, 0).ToString();
	for (auto &k : keys) {
		RunSingleTask(con, task, pipeline_uuid, string(), k);
	}
}

// Load partitions for an asset filtered by an optional [from, to] range.
// For Daily partitions the bounds are inclusive `YYYY-MM-DD`. For Static /
// Multi the filter is ignored (the full registered set is returned).
static std::vector<string> KeysFromRegistry(Connection &con, const string &asset_name,
                                              const string &from, const string &to) {
	std::vector<string> out;
	std::ostringstream q;
	q << "SELECT partition_key FROM __orch__.asset_partitions WHERE asset_name = "
	  << SqlEscape(asset_name);
	if (!from.empty()) {
		q << " AND partition_key >= " << SqlEscape(from);
	}
	if (!to.empty()) {
		q << " AND partition_key <= " << SqlEscape(to);
	}
	q << " ORDER BY partition_key;";
	auto r = con.Query(q.str());
	if (r->HasError()) return out;
	for (idx_t i = 0; i < r->RowCount(); i++) {
		out.push_back(r->GetValue(0, i).ToString());
	}
	return out;
}

// Filter `keys` to only those not yet successfully materialized.
static std::vector<string> FilterMissing(Connection &con, const string &asset_name,
                                           const std::vector<string> &keys) {
	if (keys.empty()) return keys;
	std::ostringstream q;
	q << "SELECT partition_key FROM __orch__.asset_materializations "
	  << "WHERE asset_name = " << SqlEscape(asset_name)
	  << " AND status = 'success';";
	std::set<string> done;
	auto r = con.Query(q.str());
	if (!r->HasError()) {
		for (idx_t i = 0; i < r->RowCount(); i++) {
			done.insert(r->GetValue(0, i).ToString());
		}
	}
	std::vector<string> out;
	for (auto &k : keys) {
		if (!done.count(k)) out.push_back(k);
	}
	return out;
}

// Look up a TaskRow by name. Throws on missing.
static TaskRow LoadTaskRowByName(Connection &con, const string &task_name) {
	auto rows = LoadTaskRows(con);
	for (auto &r : rows) {
		if (r.name == task_name) return r;
	}
	throw InvalidInputException("orch_backfill: task `" + task_name + "` not found");
}

// `PRAGMA orch_backfill('asset.name', 'YYYY-MM-DD', 'YYYY-MM-DD')`
//   Run every partition in [from, to]. Both bounds optional (NULL = open).
static void OrchBackfillPragma(ClientContext &context,
                                 const FunctionParameters &parameters) {
	Connection con(*context.db);
	EnsureOrchSchema(con);
	if (parameters.values.empty() || parameters.values[0].IsNull()) {
		throw InvalidInputException("orch_backfill requires an asset name");
	}
	string asset = parameters.values[0].GetValue<string>();
	string from = parameters.values.size() > 1 && !parameters.values[1].IsNull()
	                  ? parameters.values[1].GetValue<string>()
	                  : string();
	string to = parameters.values.size() > 2 && !parameters.values[2].IsNull()
	                ? parameters.values[2].GetValue<string>()
	                : string();
	string task_name = LookupDefiningTask(con, asset);
	if (task_name.empty()) {
		throw InvalidInputException("orch_backfill: no task defines asset `" + asset + "`");
	}
	auto task = LoadTaskRowByName(con, task_name);
	auto keys = KeysFromRegistry(con, asset, from, to);
	RunBackfillKeys(con, task, keys);
}

// `PRAGMA orch_backfill_missing('asset.name')` — same as orch_backfill
// but skips partitions whose latest status is already 'success'.
static void OrchBackfillMissingPragma(ClientContext &context,
                                        const FunctionParameters &parameters) {
	Connection con(*context.db);
	EnsureOrchSchema(con);
	if (parameters.values.empty() || parameters.values[0].IsNull()) {
		throw InvalidInputException("orch_backfill_missing requires an asset name");
	}
	string asset = parameters.values[0].GetValue<string>();
	string task_name = LookupDefiningTask(con, asset);
	if (task_name.empty()) {
		throw InvalidInputException(
		    "orch_backfill_missing: no task defines asset `" + asset + "`");
	}
	auto task = LoadTaskRowByName(con, task_name);
	auto keys = KeysFromRegistry(con, asset, "", "");
	keys = FilterMissing(con, asset, keys);
	RunBackfillKeys(con, task, keys);
}

// `PRAGMA orch_run_partition('asset.name', 'partition_key')` — run a
// single partition. Convenience for `duck-orch run <task> --partition K`.
static void OrchRunPartitionPragma(ClientContext &context,
                                     const FunctionParameters &parameters) {
	Connection con(*context.db);
	EnsureOrchSchema(con);
	if (parameters.values.size() < 2 || parameters.values[0].IsNull() ||
	    parameters.values[1].IsNull()) {
		throw InvalidInputException(
		    "orch_run_partition requires (asset_name, partition_key)");
	}
	string asset = parameters.values[0].GetValue<string>();
	string key = parameters.values[1].GetValue<string>();
	string task_name = LookupDefiningTask(con, asset);
	if (task_name.empty()) {
		throw InvalidInputException(
		    "orch_run_partition: no task defines asset `" + asset + "`");
	}
	auto task = LoadTaskRowByName(con, task_name);
	auto pipeline_uuid = con.Query("SELECT uuid()::VARCHAR")->GetValue(0, 0).ToString();
	RunSingleTask(con, task, pipeline_uuid, string(), key);
}

// ========================================================================
// Configuration callbacks
// ========================================================================

static void SetOlUrl(ClientContext &context, SetScope scope, Value &param) {
	auto v = param.GetValue<string>();
	orch_ol_set_url(reinterpret_cast<const uint8_t *>(v.c_str()), v.size());
}

static void SetOlApiKey(ClientContext &context, SetScope scope, Value &param) {
	auto v = param.GetValue<string>();
	orch_ol_set_api_key(reinterpret_cast<const uint8_t *>(v.c_str()), v.size());
}

static void SetOlDebug(ClientContext &context, SetScope scope, Value &param) {
	orch_ol_set_debug(param.GetValue<bool>() ? 1 : 0);
}

static void SetOrchNamespace(ClientContext &context, SetScope scope, Value &param) {
	g_orch_namespace = param.GetValue<string>();
}

static void SetOrchMaxParallel(ClientContext &context, SetScope scope, Value &param) {
	int n = (int)param.GetValue<int64_t>();
	if (n < 1) n = 1;
	g_max_parallel.store(n);
}

static void SetOrchCaptureInteractive(ClientContext &context, SetScope scope, Value &param) {
	g_capture_interactive.store(param.GetValue<bool>());
}

// ========================================================================
// OpenLineage event helpers
// ========================================================================

static string OlEventJson(const string &event_type, const string &event_time,
                          const string &run_id, const string &pipeline_run_id,
                          const string &job_namespace, const string &job_name,
                          const std::vector<OlDataset> &inputs,
                          const std::vector<OlDataset> &outputs,
                          const string &error_message) {
	std::ostringstream o;
	o << "{"
	  << "\"eventType\":" << JsonEscape(event_type)
	  << ",\"eventTime\":" << JsonEscape(event_time)
	  << ",\"producer\":\"https://github.com/nkwork9999/duck-orch\""
	  << ",\"schemaURL\":\"https://openlineage.io/spec/2-0-2/OpenLineage.json\""
	  << ",\"run\":{\"runId\":" << JsonEscape(run_id) << ",\"facets\":{";
	if (!pipeline_run_id.empty()) {
		o << "\"parent\":{\"_producer\":\"https://github.com/nkwork9999/duck-orch\","
		     "\"_schemaURL\":\"https://openlineage.io/spec/facets/1-0-0/ParentRunFacet.json\","
		     "\"run\":{\"runId\":" << JsonEscape(pipeline_run_id) << "},"
		     "\"job\":{\"namespace\":" << JsonEscape(job_namespace)
		  << ",\"name\":\"pipeline\"}}";
	}
	o << "}}"
	  << ",\"job\":{\"namespace\":" << JsonEscape(job_namespace)
	  << ",\"name\":" << JsonEscape(job_name) << ",\"facets\":{}}";
	o << ",\"inputs\":[";
	for (size_t i = 0; i < inputs.size(); i++) {
		if (i > 0) o << ",";
		o << "{\"namespace\":" << JsonEscape(inputs[i].ns)
		  << ",\"name\":" << JsonEscape(inputs[i].name) << "}";
	}
	o << "],\"outputs\":[";
	for (size_t i = 0; i < outputs.size(); i++) {
		if (i > 0) o << ",";
		o << "{\"namespace\":" << JsonEscape(outputs[i].ns)
		  << ",\"name\":" << JsonEscape(outputs[i].name) << "}";
	}
	o << "]}";
	(void)error_message;
	return o.str();
}

// Build a JSON schema map for the given input tables (table_name -> [columns]).
// Used by orch_extract_column_lineage to resolve SELECT *.
static string BuildSchemaJson(Connection &con, const std::vector<OlDataset> &tables) {
	std::ostringstream o;
	o << "{";
	bool first = true;
	for (const auto &t : tables) {
		auto r = con.Query("DESCRIBE " + t.name);
		if (r->HasError()) continue;
		if (!first) o << ",";
		first = false;
		o << JsonEscape(t.name) << ":[";
		bool first_col = true;
		for (idx_t i = 0; i < r->RowCount(); i++) {
			if (!first_col) o << ",";
			first_col = false;
			o << JsonEscape(r->GetValue(0, i).ToString());
		}
		o << "]";
	}
	o << "}";
	return o.str();
}

// Build the OpenLineage `columnLineage` facet object literal from the
// raw extractor JSON. Returns the inner facet body (without leading
// "columnLineage:" key) so callers can splice it into a larger event.
static string BuildColumnLineageFacet(const string &cl_extractor_json,
                                       const std::vector<OlDataset> &task_inputs) {
	// Map dataset name -> resolved namespace (already computed by ResolveDatasetNamespace
	// at task start, available in task_inputs).
	std::map<string, string> ns_lookup;
	for (auto &t : task_inputs) ns_lookup[t.name] = t.ns;

	auto doc = yyjson_ns::yyjson_read(cl_extractor_json.c_str(), cl_extractor_json.size(), 0);
	if (!doc) return "";
	auto root = yyjson_ns::yyjson_doc_get_root(doc);
	std::ostringstream o;
	o << "{\"_producer\":\"https://github.com/nkwork9999/duck-orch\","
	     "\"_schemaURL\":\"https://openlineage.io/spec/facets/1-1-0/ColumnLineageDatasetFacet.json\","
	     "\"fields\":{";
	bool first_field = true;
	if (root && yyjson_ns::yyjson_is_arr(root)) {
		size_t i, m;
		yyjson_ns::yyjson_val *res;
		yyjson_arr_foreach(root, i, m, res) {
			auto cols = yyjson_ns::yyjson_obj_get(res, "columns");
			if (!cols || !yyjson_ns::yyjson_is_arr(cols)) continue;
			size_t j, n;
			yyjson_ns::yyjson_val *col;
			yyjson_arr_foreach(cols, j, n, col) {
				auto out_field_v = yyjson_ns::yyjson_obj_get(col, "output_field");
				auto inputs = yyjson_ns::yyjson_obj_get(col, "inputs");
				if (!out_field_v || !inputs || !yyjson_ns::yyjson_is_arr(inputs)) continue;
				if (!first_field) o << ",";
				first_field = false;
				const char *of = yyjson_ns::yyjson_get_str(out_field_v);
				o << JsonEscape(of ? of : "") << ":{\"inputFields\":[";
				bool first_in = true;
				size_t k, p;
				yyjson_ns::yyjson_val *in;
				yyjson_arr_foreach(inputs, k, p, in) {
					auto in_ds_v = yyjson_ns::yyjson_obj_get(in, "dataset");
					auto in_field_v = yyjson_ns::yyjson_obj_get(in, "field");
					auto trans = yyjson_ns::yyjson_obj_get(in, "transformations");
					if (!in_ds_v || !in_field_v) continue;
					if (!first_in) o << ",";
					first_in = false;
					const char *ids = yyjson_ns::yyjson_get_str(in_ds_v);
					const char *fld = yyjson_ns::yyjson_get_str(in_field_v);
					string ns = "duckdb";
					auto it = ns_lookup.find(ids ? ids : "");
					if (it != ns_lookup.end()) ns = it->second;
					o << "{\"namespace\":" << JsonEscape(ns)
					  << ",\"name\":" << JsonEscape(ids ? ids : "")
					  << ",\"field\":" << JsonEscape(fld ? fld : "")
					  << ",\"transformations\":";
					if (trans && yyjson_ns::yyjson_is_arr(trans)) {
						o << yyjson_ns::yyjson_val_write(trans, 0, nullptr);
					} else {
						o << "[]";
					}
					o << "}";
				}
				o << "]}";
			}
		}
	}
	o << "}}";
	yyjson_ns::yyjson_doc_free(doc);
	return o.str();
}

// Use DuckDB's binder via Connection::Prepare() to discover the actual output
// columns of a SQL statement, including those produced by `SELECT *` over
// subqueries / views / CTEs that pure AST analysis can't expand.
//
// Returns an empty vector if Prepare fails (e.g. tables don't exist yet).
static std::vector<string> PrepareOutputColumns(Connection &con, const string &task_sql) {
	std::vector<string> names;
	// CREATE TABLE x AS SELECT ... — strip the prefix and prepare just the SELECT,
	// because prepare on a CREATE statement returns no result columns.
	string sql_to_prepare = task_sql;
	auto skip_ws = [](const string &s, size_t i) {
		while (i < s.size() && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r')) i++;
		return i;
	};
	auto starts_with_ci = [](const string &s, size_t i, const char *kw) {
		size_t j = 0;
		while (kw[j] && i + j < s.size()) {
			char c = s[i + j];
			if (c >= 'a' && c <= 'z') c = (char)(c - 32);
			if (c != kw[j]) return false;
			j++;
		}
		return kw[j] == '\0';
	};
	size_t i = skip_ws(task_sql, 0);
	if (starts_with_ci(task_sql, i, "CREATE")) {
		// Skip until "AS" and prepare from there
		auto pos = task_sql.find(" AS ");
		if (pos == string::npos) pos = task_sql.find(" as ");
		if (pos != string::npos) {
			sql_to_prepare = task_sql.substr(pos + 4);
		} else {
			return names; // CREATE TABLE without AS, no columns to derive
		}
	} else if (starts_with_ci(task_sql, i, "INSERT")) {
		// Skip "INSERT INTO <tbl>" and prepare from the SELECT
		auto pos_select = task_sql.find("SELECT");
		if (pos_select == string::npos) pos_select = task_sql.find("select");
		if (pos_select != string::npos) {
			sql_to_prepare = task_sql.substr(pos_select);
		}
	}
	try {
		auto stmt = con.Prepare(sql_to_prepare);
		if (stmt && !stmt->HasError()) {
			names = stmt->GetNames();
		}
	} catch (...) {
	}
	return names;
}

// Insert column lineage rows for a successful task. Returns the raw extractor
// JSON so callers (e.g. OpenLineage emitter) can build a facet from it.
static string RecordColumnLineage(Connection &con, const string &task_sql,
                                   const string &task_name,
                                   const std::vector<OlDataset> &task_inputs) {
	string schema_json = BuildSchemaJson(con, task_inputs);
	bool ok = false;
	auto cl_json = CallRustString(
	    [&](uint8_t **op, size_t *ol) {
		    return orch_extract_column_lineage(
		        reinterpret_cast<const uint8_t *>(task_sql.c_str()), task_sql.size(),
		        reinterpret_cast<const uint8_t *>(schema_json.c_str()), schema_json.size(),
		        op, ol);
	        },
	    ok);
	if (!ok || cl_json.empty()) return string();

	auto doc = yyjson_ns::yyjson_read(cl_json.c_str(), cl_json.size(), 0);
	if (!doc) return string();
	auto root = yyjson_ns::yyjson_doc_get_root(doc);
	if (!root || !yyjson_ns::yyjson_is_arr(root)) {
		yyjson_ns::yyjson_doc_free(doc);
		return string();
	}
	con.Query("DELETE FROM __orch__.column_lineage WHERE via_task = " + SqlEscape(task_name));

	std::ostringstream batch;
	bool any = false;
	size_t i, m;
	yyjson_ns::yyjson_val *res;
	yyjson_arr_foreach(root, i, m, res) {
		auto out_ds_v = yyjson_ns::yyjson_obj_get(res, "output_dataset");
		auto cols = yyjson_ns::yyjson_obj_get(res, "columns");
		if (!out_ds_v || !cols || !yyjson_ns::yyjson_is_arr(cols)) continue;
		string dst_dataset = yyjson_ns::yyjson_get_str(out_ds_v) ? yyjson_ns::yyjson_get_str(out_ds_v) : "";
		size_t j, n;
		yyjson_ns::yyjson_val *col;
		yyjson_arr_foreach(cols, j, n, col) {
			auto out_field_v = yyjson_ns::yyjson_obj_get(col, "output_field");
			auto inputs = yyjson_ns::yyjson_obj_get(col, "inputs");
			if (!out_field_v || !inputs || !yyjson_ns::yyjson_is_arr(inputs)) continue;
			string dst_column = yyjson_ns::yyjson_get_str(out_field_v) ? yyjson_ns::yyjson_get_str(out_field_v) : "";
			size_t k, p;
			yyjson_ns::yyjson_val *in;
			yyjson_arr_foreach(inputs, k, p, in) {
				auto in_ds_v = yyjson_ns::yyjson_obj_get(in, "dataset");
				auto in_field_v = yyjson_ns::yyjson_obj_get(in, "field");
				auto trans = yyjson_ns::yyjson_obj_get(in, "transformations");
				if (!in_ds_v || !in_field_v) continue;
				string src_dataset = yyjson_ns::yyjson_get_str(in_ds_v) ? yyjson_ns::yyjson_get_str(in_ds_v) : "";
				string src_column = yyjson_ns::yyjson_get_str(in_field_v) ? yyjson_ns::yyjson_get_str(in_field_v) : "";
				string kind = "DIRECT", subtype = "IDENTITY", desc;
				if (trans && yyjson_ns::yyjson_is_arr(trans)) {
					size_t l, q;
					yyjson_ns::yyjson_val *t;
					yyjson_arr_foreach(trans, l, q, t) {
						auto k_v = yyjson_ns::yyjson_obj_get(t, "type");
						auto s_v = yyjson_ns::yyjson_obj_get(t, "subtype");
						auto d_v = yyjson_ns::yyjson_obj_get(t, "description");
						if (k_v && yyjson_ns::yyjson_get_str(k_v)) kind = yyjson_ns::yyjson_get_str(k_v);
						if (s_v && yyjson_ns::yyjson_get_str(s_v)) subtype = yyjson_ns::yyjson_get_str(s_v);
						if (d_v && yyjson_ns::yyjson_get_str(d_v)) desc = yyjson_ns::yyjson_get_str(d_v);
						break; // first transformation only for the row
					}
				}
				if (any) batch << ",";
				else batch << "INSERT INTO __orch__.column_lineage (src_dataset, src_column, dst_dataset, dst_column, via_task, transform_kind, subtype, description) VALUES ";
				batch << "(" << SqlEscape(src_dataset) << ", " << SqlEscape(src_column) << ", "
				      << SqlEscape(dst_dataset) << ", " << SqlEscape(dst_column) << ", "
				      << SqlEscape(task_name) << ", " << SqlEscape(kind) << ", " << SqlEscape(subtype)
				      << ", " << SqlEscape(desc) << ")";
				any = true;
			}
		}
	}
	yyjson_ns::yyjson_doc_free(doc);
	if (any) {
		batch << ";";
		auto r = con.Query(batch.str());
		if (r->HasError()) {
			Printer::Print("[duckorch] column_lineage insert failed: " + r->GetError());
		}
	}

	// Prepare-based fallback: discover output columns the AST extractor couldn't
	// resolve (subquery / view / CTE wildcards). Insert placeholder rows so
	// downstream consumers at least know the output column names exist.
	auto prepare_cols = PrepareOutputColumns(con, task_sql);
	if (!prepare_cols.empty()) {
		// Find which columns we already have lineage for (per dst_column)
		std::set<string> already_covered;
		auto existing = con.Query(
		    "SELECT DISTINCT dst_column FROM __orch__.column_lineage WHERE via_task = " +
		    SqlEscape(task_name));
		if (!existing->HasError()) {
			for (idx_t i2 = 0; i2 < existing->RowCount(); i2++) {
				already_covered.insert(existing->GetValue(0, i2).ToString());
			}
		}
		string dst_dataset_for_unresolved;
		// pull the first output dataset from the extractor JSON for placeholder rows
		auto doc2 = yyjson_ns::yyjson_read(cl_json.c_str(), cl_json.size(), 0);
		if (doc2) {
			auto root2 = yyjson_ns::yyjson_doc_get_root(doc2);
			if (root2 && yyjson_ns::yyjson_is_arr(root2) && yyjson_ns::yyjson_arr_size(root2) > 0) {
				auto first = yyjson_ns::yyjson_arr_get(root2, 0);
				auto dsv = yyjson_ns::yyjson_obj_get(first, "output_dataset");
				if (dsv && yyjson_ns::yyjson_get_str(dsv)) {
					dst_dataset_for_unresolved = yyjson_ns::yyjson_get_str(dsv);
				}
			}
			yyjson_ns::yyjson_doc_free(doc2);
		}
		std::ostringstream extra;
		bool any_extra = false;
		for (auto &col : prepare_cols) {
			if (already_covered.count(col) > 0) continue;
			if (!any_extra) {
				extra << "INSERT INTO __orch__.column_lineage "
				      << "(src_dataset, src_column, dst_dataset, dst_column, via_task, "
				      << "transform_kind, subtype, description) VALUES ";
			} else {
				extra << ",";
			}
			any_extra = true;
			extra << "(" << SqlEscape("__unresolved__") << ", " << SqlEscape("") << ", "
			      << SqlEscape(dst_dataset_for_unresolved) << ", " << SqlEscape(col) << ", "
			      << SqlEscape(task_name) << ", " << SqlEscape("INDIRECT") << ", "
			      << SqlEscape("TRANSFORMATION") << ", "
			      << SqlEscape("output column found via Connection::Prepare(); source unresolved by static AST")
			      << ")";
		}
		if (any_extra) {
			extra << ";";
			auto rr = con.Query(extra.str());
			if (rr->HasError()) {
				Printer::Print("[duckorch] prepare fallback insert failed: " + rr->GetError());
			}
		}
	}

	return cl_json;
}

// Phase 9 + DuckLake: resolve OpenLineage `namespace` for a dataset.
// If the table_name's catalog has a `data_path` tag (DuckLake convention,
// e.g. "s3://my-bucket/lake"), use that as the namespace so cross-engine
// observers can correlate events. Otherwise fall back to g_orch_namespace.
static string ResolveDatasetNamespace(ClientContext &context, const string &table_name) {
	auto first_dot = table_name.find('.');
	if (first_dot == string::npos) {
		return g_orch_namespace;
	}
	string maybe_catalog = table_name.substr(0, first_dot);
	try {
		auto &catalog = Catalog::GetCatalog(context, maybe_catalog);
		if (catalog.IsSystemCatalog() || catalog.IsTemporaryCatalog()) {
			return g_orch_namespace;
		}
		auto &attached_db = catalog.GetAttached();
		if (attached_db.tags.find("data_path") != attached_db.tags.end()) {
			string path = attached_db.tags["data_path"];
			if (!path.empty() && path.back() == '/') path.pop_back();
			if (!path.empty()) return path;
		}
	} catch (...) {
		// catalog lookup failed — fall through to default
	}
	return g_orch_namespace;
}

static void EmitOlEvent(const string &json) {
	orch_ol_emit(reinterpret_cast<const uint8_t *>(json.c_str()), json.size());
}

// ========================================================================
// OptimizerExtension: capture column lineage for ad-hoc / dynamic SQL
// ========================================================================
//
// pre_optimize_function fires for every successfully-parsed query. We use
// ClientContext::GetCurrentQuery() to retrieve the original SQL text, then
// reuse our sqlparser-rs based extractor. Disabled by default; toggled via
// SET orch_capture_interactive=true.
//
// PreparedStatements have no original SQL at execute-time, so they fall back
// to "no lineage emitted" (same limitation as plan-only observers).

static void OrchPreOptimize(OptimizerExtensionInput &input,
                             unique_ptr<LogicalOperator> &plan) {
	if (!g_capture_interactive.load() || g_inside_capture) return;
	if (!plan) return;
	string query;
	try {
		query = input.context.GetCurrentQuery();
	} catch (...) {
		return; // PreparedStatement etc.
	}
	if (query.empty()) return;
	if (query.size() < 20) return;
	if (query.find("__orch__") != string::npos) return;

	auto is_writeish = [](const string &q) {
		size_t i = 0;
		while (i < q.size() && (q[i] == ' ' || q[i] == '\t' || q[i] == '\n' || q[i] == '\r')) i++;
		auto starts_with = [&](const char *kw) {
			size_t j = 0;
			while (kw[j] && i + j < q.size()) {
				char c = q[i + j];
				if (c >= 'a' && c <= 'z') c = (char)(c - 32);
				if (c != kw[j]) return false;
				j++;
			}
			return kw[j] == '\0';
		};
		return starts_with("INSERT") || starts_with("CREATE") || starts_with("UPDATE")
		       || starts_with("REPLACE");
	};
	if (!is_writeish(query)) return;

	g_inside_capture = true;
	try {
		Connection con(*input.context.db);
		std::vector<OlDataset> empty_inputs;
		(void)RecordColumnLineage(con, query, "__interactive__", empty_inputs);
	} catch (...) {
	}
	g_inside_capture = false;
}

// ========================================================================
// Extension entry
// ========================================================================

static void LoadInternal(ExtensionLoader &loader) {
	auto &config = loader.GetDatabaseInstance().config;
	config.AddExtensionOption("orch_openlineage_url",
	                          "OpenLineage backend URL (e.g. http://localhost:5000/api/v1/lineage)",
	                          LogicalType::VARCHAR, Value(""), SetOlUrl);
	config.AddExtensionOption("orch_openlineage_api_key", "OpenLineage API key",
	                          LogicalType::VARCHAR, Value(""), SetOlApiKey);
	config.AddExtensionOption("orch_openlineage_debug", "Log OpenLineage events to stderr",
	                          LogicalType::BOOLEAN, Value(false), SetOlDebug);
	config.AddExtensionOption("orch_namespace", "Job namespace for OpenLineage events",
	                          LogicalType::VARCHAR, Value("duckdb"), SetOrchNamespace);
	config.AddExtensionOption("orch_max_parallel",
	                          "Maximum parallel tasks per DAG layer",
	                          LogicalType::BIGINT, Value::BIGINT(1), SetOrchMaxParallel);
	config.AddExtensionOption(
	    "orch_capture_interactive",
	    "Capture column lineage for ad-hoc INSERT/CTAS queries via ParserExtension",
	    LogicalType::BOOLEAN, Value(false), SetOrchCaptureInteractive);

	// OptimizerExtension hook for ad-hoc query column lineage capture.
	g_db_for_capture = &loader.GetDatabaseInstance();
	OptimizerExtension oext;
	oext.pre_optimize_function = OrchPreOptimize;
	OptimizerExtension::Register(config, std::move(oext));

	loader.RegisterFunction(
	    ScalarFunction("orch_hello", {LogicalType::VARCHAR}, LogicalType::VARCHAR, OrchHelloFunc));
	loader.RegisterFunction(ScalarFunction("orch_extract_io", {LogicalType::VARCHAR},
	                                        LogicalType::VARCHAR, OrchExtractIoFunc));
	loader.RegisterFunction(ScalarFunction("orch_parse_task",
	                                        {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                        LogicalType::VARCHAR, OrchParseTaskFunc));
	loader.RegisterFunction(ScalarFunction("orch_load_directory_json", {LogicalType::VARCHAR},
	                                        LogicalType::VARCHAR, OrchLoadDirectoryFunc));
	loader.RegisterFunction(ScalarFunction("orch_build_dag", {LogicalType::VARCHAR},
	                                        LogicalType::VARCHAR, OrchBuildDagFunc));
	loader.RegisterFunction(ScalarFunction(
	    "orch_render_mermaid",
	    {LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::VARCHAR},
	    LogicalType::VARCHAR, OrchRenderMermaidFunc));
	loader.RegisterFunction(ScalarFunction("orch_downstream_of",
	                                        {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                        LogicalType::VARCHAR, OrchDownstreamOfFunc));

	// PRAGMAs: side-effect ones use pragma_function_t (void return).
	// orch_visualize stays pragma_query_t since it returns a SELECT statement.
	loader.RegisterFunction(PragmaFunction::PragmaStatement(
	    "orch_init", static_cast<pragma_function_t>(OrchInitPragma)));
	loader.RegisterFunction(PragmaFunction::PragmaCall(
	    "orch_register", static_cast<pragma_function_t>(OrchRegisterPragma),
	    {LogicalType::VARCHAR}));
	loader.RegisterFunction(PragmaFunction::PragmaStatement(
	    "orch_run", static_cast<pragma_function_t>(OrchRunPragma)));
	loader.RegisterFunction(PragmaFunction::PragmaCall(
	    "orch_visualize", OrchVisualizePragma, {LogicalType::VARCHAR}));
	loader.RegisterFunction(PragmaFunction::PragmaStatement(
	    "orch_test", static_cast<pragma_function_t>(OrchTestPragma)));

	// Phase 13 m2: Asset read-side pragmas. All `pragma_query_t` — they
	// return a SELECT statement so duckdb prints rows natively.
	loader.RegisterFunction(PragmaFunction::PragmaStatement(
	    "orch_asset_list", OrchAssetListPragma));
	loader.RegisterFunction(PragmaFunction::PragmaCall(
	    "orch_asset_list_group", OrchAssetListGroupPragma, {LogicalType::VARCHAR}));
	loader.RegisterFunction(PragmaFunction::PragmaCall(
	    "orch_asset_show", OrchAssetShowPragma, {LogicalType::VARCHAR}));
	loader.RegisterFunction(PragmaFunction::PragmaCall(
	    "orch_asset_materializations", OrchAssetMaterializationsPragma,
	    {LogicalType::VARCHAR, LogicalType::BIGINT}));
	loader.RegisterFunction(PragmaFunction::PragmaCall(
	    "orch_asset_lineage", OrchAssetLineagePragma, {LogicalType::VARCHAR}));
	loader.RegisterFunction(PragmaFunction::PragmaStatement(
	    "orch_asset_health", OrchAssetHealthPragma));

	// Phase 14: Partition + backfill pragmas.
	loader.RegisterFunction(PragmaFunction::PragmaCall(
	    "orch_asset_partitions", OrchAssetPartitionsPragma, {LogicalType::VARCHAR}));
	loader.RegisterFunction(PragmaFunction::PragmaCall(
	    "orch_asset_partitions_calendar", OrchAssetPartitionsCalendarPragma,
	    {LogicalType::VARCHAR}));
	loader.RegisterFunction(PragmaFunction::PragmaCall(
	    "orch_backfill", static_cast<pragma_function_t>(OrchBackfillPragma),
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}));
	loader.RegisterFunction(PragmaFunction::PragmaCall(
	    "orch_backfill_missing",
	    static_cast<pragma_function_t>(OrchBackfillMissingPragma),
	    {LogicalType::VARCHAR}));
	loader.RegisterFunction(PragmaFunction::PragmaCall(
	    "orch_run_partition", static_cast<pragma_function_t>(OrchRunPartitionPragma),
	    {LogicalType::VARCHAR, LogicalType::VARCHAR}));
}

void DuckorchExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string DuckorchExtension::Name() {
	return "duckorch";
}

std::string DuckorchExtension::Version() const {
#ifdef EXT_VERSION_DUCKORCH
	return EXT_VERSION_DUCKORCH;
#else
	return "0.1.0";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(duckorch, loader) {
	duckdb::LoadInternal(loader);
}
}
