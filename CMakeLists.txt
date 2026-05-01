cmake_minimum_required(VERSION 3.5...3.29)

set(TARGET_NAME duckorch)
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# ---------------------------------------------------------------------------
# Ensure Rust/Cargo is available (install via rustup if missing in CI)
# ---------------------------------------------------------------------------
find_program(CARGO_EXECUTABLE cargo)
if(NOT CARGO_EXECUTABLE)
    message(STATUS "cargo not found — installing Rust toolchain via rustup")
    if(WIN32)
        file(DOWNLOAD "https://win.rustup.rs/x86_64" "${CMAKE_BINARY_DIR}/rustup-init.exe" TLS_VERIFY ON)
        execute_process(
            COMMAND "${CMAKE_BINARY_DIR}/rustup-init.exe" -y --default-toolchain stable
            RESULT_VARIABLE RUSTUP_RESULT
        )
        if(NOT RUSTUP_RESULT EQUAL 0)
            message(FATAL_ERROR "Failed to install Rust toolchain on Windows")
        endif()
        set(ENV{PATH} "$ENV{USERPROFILE}/.cargo/bin;$ENV{PATH}")
        find_program(CARGO_EXECUTABLE cargo PATHS "$ENV{USERPROFILE}/.cargo/bin" NO_DEFAULT_PATH)
    else()
        execute_process(
            COMMAND sh -c "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable"
            RESULT_VARIABLE RUSTUP_RESULT
        )
        if(NOT RUSTUP_RESULT EQUAL 0)
            message(FATAL_ERROR "Failed to install Rust toolchain")
        endif()
        set(ENV{PATH} "$ENV{HOME}/.cargo/bin:$ENV{PATH}")
        find_program(CARGO_EXECUTABLE cargo PATHS "$ENV{HOME}/.cargo/bin" NO_DEFAULT_PATH)
    endif()
    if(NOT CARGO_EXECUTABLE)
        message(FATAL_ERROR "cargo still not found after rustup install")
    endif()
    message(STATUS "Rust installed: ${CARGO_EXECUTABLE}")
endif()

# ---------------------------------------------------------------------------
# Build Rust static library via cargo
# ---------------------------------------------------------------------------
set(RUST_DIR ${CMAKE_CURRENT_SOURCE_DIR})

if(CMAKE_BUILD_TYPE STREQUAL "Release" OR CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
    set(CARGO_PROFILE release)
    set(CARGO_FLAG --release)
else()
    set(CARGO_PROFILE debug)
    set(CARGO_FLAG "")
endif()

# ---------------------------------------------------------------------------
# Detect cross-compilation target for Rust
# ---------------------------------------------------------------------------
if(DEFINED ENV{CARGO_TARGET})
    set(RUST_TARGET "$ENV{CARGO_TARGET}")
elseif(DEFINED OSX_BUILD_ARCH AND NOT OSX_BUILD_ARCH STREQUAL "")
    if(OSX_BUILD_ARCH STREQUAL "x86_64")
        set(RUST_TARGET "x86_64-apple-darwin")
    elseif(OSX_BUILD_ARCH STREQUAL "arm64")
        set(RUST_TARGET "aarch64-apple-darwin")
    endif()
elseif(DEFINED DUCKDB_EXPLICIT_PLATFORM AND NOT DUCKDB_EXPLICIT_PLATFORM STREQUAL "")
    if(DUCKDB_EXPLICIT_PLATFORM STREQUAL "osx_amd64")
        set(RUST_TARGET "x86_64-apple-darwin")
    elseif(DUCKDB_EXPLICIT_PLATFORM STREQUAL "osx_arm64")
        set(RUST_TARGET "aarch64-apple-darwin")
    elseif(DUCKDB_EXPLICIT_PLATFORM STREQUAL "linux_amd64")
        set(RUST_TARGET "x86_64-unknown-linux-gnu")
    elseif(DUCKDB_EXPLICIT_PLATFORM STREQUAL "linux_arm64")
        set(RUST_TARGET "aarch64-unknown-linux-gnu")
    elseif(DUCKDB_EXPLICIT_PLATFORM STREQUAL "linux_amd64_gcc4")
        set(RUST_TARGET "x86_64-unknown-linux-gnu")
    elseif(DUCKDB_EXPLICIT_PLATFORM STREQUAL "windows_amd64")
        set(RUST_TARGET "x86_64-pc-windows-msvc")
    elseif(DUCKDB_EXPLICIT_PLATFORM STREQUAL "windows_amd64_mingw")
        set(RUST_TARGET "x86_64-pc-windows-gnu")
    endif()
endif()

if(DEFINED RUST_TARGET)
    execute_process(
        COMMAND ${CMAKE_COMMAND} -E env "PATH=$ENV{HOME}/.cargo/bin:$ENV{PATH}" rustup target add ${RUST_TARGET}
        WORKING_DIRECTORY ${RUST_DIR}
    )
    set(CARGO_TARGET_DIR "target/${RUST_TARGET}/${CARGO_PROFILE}")
    set(CARGO_TARGET_FLAG "--target" "${RUST_TARGET}")
    message(STATUS "Rust cross-compile target: ${RUST_TARGET}")
else()
    set(CARGO_TARGET_DIR "target/${CARGO_PROFILE}")
    set(CARGO_TARGET_FLAG "")
    message(STATUS "Rust target: host default")
endif()

if(MSVC)
    set(RUST_LIB_PREFIX "")
    set(RUST_LIB_SUFFIX ".lib")
else()
    set(RUST_LIB_PREFIX "lib")
    set(RUST_LIB_SUFFIX ".a")
endif()

set(RUST_CRATE_NAMES core)
set(RUST_STATIC_LIBS "")
foreach(CRATE ${RUST_CRATE_NAMES})
    list(APPEND RUST_STATIC_LIBS "${RUST_DIR}/${CARGO_TARGET_DIR}/${RUST_LIB_PREFIX}duckorch_${CRATE}${RUST_LIB_SUFFIX}")
endforeach()

if(WIN32)
    add_custom_command(
        OUTPUT ${RUST_STATIC_LIBS}
        COMMAND ${CARGO_EXECUTABLE} build ${CARGO_FLAG} ${CARGO_TARGET_FLAG} -p duckorch_core
        WORKING_DIRECTORY ${RUST_DIR}
        COMMENT "Building Rust workspace (duckorch crates)"
        VERBATIM
    )
else()
    add_custom_command(
        OUTPUT ${RUST_STATIC_LIBS}
        COMMAND ${CMAKE_COMMAND} -E env "PATH=$ENV{HOME}/.cargo/bin:$ENV{PATH}" ${CARGO_EXECUTABLE} build ${CARGO_FLAG} ${CARGO_TARGET_FLAG} -p duckorch_core
        WORKING_DIRECTORY ${RUST_DIR}
        COMMENT "Building Rust workspace (duckorch crates)"
        VERBATIM
    )
endif()

add_custom_target(rust_lib DEPENDS ${RUST_STATIC_LIBS})

# ---------------------------------------------------------------------------
# C++ extension sources
# ---------------------------------------------------------------------------
set(EXTENSION_SOURCES
    src/duckorch_extension.cpp
)

include_directories(src/include)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

add_dependencies(${EXTENSION_NAME} rust_lib)
add_dependencies(${LOADABLE_EXTENSION_NAME} rust_lib)

foreach(RUST_LIB ${RUST_STATIC_LIBS})
    target_link_libraries(${EXTENSION_NAME} ${RUST_LIB})
    target_link_libraries(${LOADABLE_EXTENSION_NAME} ${RUST_LIB})
endforeach()

if(APPLE)
    target_link_libraries(${EXTENSION_NAME} "-framework Security" "-framework CoreFoundation")
    target_link_libraries(${LOADABLE_EXTENSION_NAME} "-framework Security" "-framework CoreFoundation")
elseif(WIN32)
    target_link_libraries(${EXTENSION_NAME} ws2_32 userenv bcrypt ntdll)
    target_link_libraries(${LOADABLE_EXTENSION_NAME} ws2_32 userenv bcrypt ntdll)
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
