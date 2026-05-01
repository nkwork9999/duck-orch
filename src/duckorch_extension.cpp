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
    registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

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
)";

static void EnsureOrchSchema(Connection &con) {
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

			sql << "INSERT OR REPLACE INTO __orch__.tasks "
			    << "(name, description, owner, sql, inputs, outputs, depends_on, schedule_cron, "
			    << "retries, timeout_seconds, incremental_by, tags, file_path) VALUES ("
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
			    << SqlEscape(get_str("file_path")) << ");\n";

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
};

static std::vector<TaskRow> LoadTaskRows(Connection &con) {
	auto result = con.Query(
	    "SELECT name, sql, retries, incremental_by FROM __orch__.tasks");
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

// Run a single task in `con`. Updates state tables and emits OL events.
// Returns true on success, false on failure.
static bool RunSingleTask(Connection &con, const TaskRow &task, const string &pipeline_run_id,
                          const string &tasks_json) {
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

	int retries_left = task.retries;
	int retry_count = 0;
	bool success = false;
	string error_msg;
	while (true) {
		auto exec_result = con.Query(sql_to_run);
		if (!exec_result->HasError()) {
			success = true;
			break;
		}
		error_msg = exec_result->GetError();
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
