#define DUCKDB_EXTENSION_MAIN

#include "duckorch_extension.hpp"
#include "duckorch.h"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"

namespace duckdb {

// Phase 0 hello-world scalar function: orch_hello(VARCHAR) -> VARCHAR
// Bridges to the Rust `orch_hello` FFI.
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

static void LoadInternal(ExtensionLoader &loader) {
	ScalarFunction hello("orch_hello", {LogicalType::VARCHAR}, LogicalType::VARCHAR, OrchHelloFunc);
	loader.RegisterFunction(hello);
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
