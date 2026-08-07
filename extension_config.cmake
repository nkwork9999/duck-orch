if(MSVC)
    if(NOT DEFINED CMAKE_CXX_STANDARD OR CMAKE_CXX_STANDARD LESS 17)
        set(CMAKE_CXX_STANDARD 17 CACHE STRING "C++ standard to enforce" FORCE)
        set(CMAKE_CXX_STANDARD_REQUIRED ON CACHE BOOL "Require the configured C++ standard" FORCE)
    endif()
    if(MSVC_VERSION GREATER_EQUAL 1950)
        add_compile_options("/FI${CMAKE_CURRENT_LIST_DIR}/cmake/msvc_fmt_compat.hpp")
    endif()
endif()

duckdb_extension_load(duckorch
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)
