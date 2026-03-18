cmake_minimum_required(VERSION 3.5.0)

project(qore-amqp-module)

set(VERSION_MAJOR 1)
set(VERSION_MINOR 0)
set(VERSION_PATCH 0)

set(PROJECT_VERSION "${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}")

if (${CMAKE_VERSION} VERSION_GREATER_EQUAL "3.12.0")
    cmake_policy(SET CMP0074 NEW)

    if (${CMAKE_VERSION} VERSION_GREATER_EQUAL "3.18.0")
        cmake_policy(SET CMP0053 NEW)
    endif()
endif()

cmake_policy(SET CMP0042 NEW)

if (UNIX AND NOT APPLE)
    set(LINUX TRUE)
endif()

find_package(Qore 2.0 REQUIRED)

# Find Qpid Proton C++ library
# Try pkg-config first, then fall back to find_library
find_package(PkgConfig QUIET)
if(PKG_CONFIG_FOUND)
    pkg_check_modules(PROTON_CPP QUIET libqpid-proton-cpp)
endif()

if(NOT PROTON_CPP_FOUND)
    # Try CMake config mode (Proton installs ProtonCppConfig.cmake)
    find_package(ProtonCpp QUIET CONFIG)
    if(ProtonCpp_FOUND)
        set(PROTON_CPP_FOUND TRUE)
        if(TARGET Proton::cpp)
            set(PROTON_CPP_LIBRARIES Proton::cpp)
            get_target_property(PROTON_CPP_INCLUDE_DIRS Proton::cpp INTERFACE_INCLUDE_DIRECTORIES)
        endif()
    endif()
endif()

if(NOT PROTON_CPP_FOUND)
    # Manual fallback
    # On macOS, check MacPorts (/opt/local) and Homebrew (/usr/local, /opt/homebrew) paths
    set(_proton_search_paths /usr /usr/local)
    if(APPLE)
        list(APPEND _proton_search_paths /opt/local /opt/homebrew)
    endif()
    find_path(PROTON_CPP_INCLUDE_DIRS proton/container.hpp
        HINTS ${_proton_search_paths}
        PATH_SUFFIXES include)
    find_library(PROTON_CPP_LIBRARIES NAMES qpid-proton-cpp
        HINTS ${_proton_search_paths}
        PATH_SUFFIXES lib lib64)
    if(PROTON_CPP_INCLUDE_DIRS AND PROTON_CPP_LIBRARIES)
        set(PROTON_CPP_FOUND TRUE)
    endif()
endif()

if(NOT PROTON_CPP_FOUND)
    message(FATAL_ERROR "Could not find Qpid Proton C++ library. "
        "Install libqpid-proton-cpp-dev (Ubuntu) or qpid-proton-cpp-dev (Alpine).")
endif()

message(STATUS "Found Qpid Proton C++: ${PROTON_CPP_LIBRARIES}")
message(STATUS "Proton C++ include dirs: ${PROTON_CPP_INCLUDE_DIRS}")

# Ensure library directories from pkg-config are on the linker search path
if(PROTON_CPP_LIBRARY_DIRS)
    link_directories(${PROTON_CPP_LIBRARY_DIRS})
    message(STATUS "Proton C++ library dirs: ${PROTON_CPP_LIBRARY_DIRS}")
endif()

# Check for C++17
include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-std=c++17" COMPILER_SUPPORTS_CXX17)
if(COMPILER_SUPPORTS_CXX17)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17")
else()
    message(FATAL_ERROR "The compiler ${CMAKE_CXX_COMPILER} has no C++17 support. "
        "Please use a different C++ compiler.")
endif()

set(QPP_SRC
    src/QC_AmqpConnection.qpp
    src/QC_AmqpMessage.qpp
)

set(CPP_SRC
    src/amqp-module.cpp
    src/QoreAmqpConnection.cpp
    src/QoreAmqpMessage.cpp
    src/QoreAmqpHelper.cpp
)

qore_wrap_qpp_value(QPP_SOURCES METALIST QPP_META_SRC ${QPP_SRC})

foreach (it ${QPP_SOURCES})
    GET_FILENAME_COMPONENT(_outfile ${it} NAME_WE)
    set(QPP_DOX ${QPP_DOX} ${CMAKE_CURRENT_BINARY_DIR}/${_outfile}.dox.h)
endforeach()

set(module_name "amqp")

set(QMOD
    qlib/AmqpUtil
    qlib/AmqpDataProvider
)

set(QORE_DOX_TMPL_SRC
    docs/mainpage.dox.tmpl
)

add_library(${module_name} MODULE ${CPP_SRC} ${QPP_SOURCES})

include_directories(${CMAKE_SOURCE_DIR}/src)
include_directories(${CMAKE_CURRENT_BINARY_DIR})
if(PROTON_CPP_INCLUDE_DIRS)
    include_directories(${PROTON_CPP_INCLUDE_DIRS})
endif()
target_include_directories(${module_name} PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)

add_custom_target(QORE_INC_FILES DEPENDS ${QORE_INC_SRC})
add_dependencies(${module_name} QORE_INC_FILES)

target_link_libraries(${module_name} ${PROTON_CPP_LIBRARIES} ${QORE_LIBRARY})

set(MODULE_DOX_INPUT ${CMAKE_CURRENT_BINARY_DIR}/mainpage.dox ${QPP_DOX})
string(REPLACE ";" " " MODULE_DOX_INPUT "${MODULE_DOX_INPUT}")

if (DEFINED ENV{DOXYGEN_EXECUTABLE})
    set(DOXYGEN_EXECUTABLE $ENV{DOXYGEN_EXECUTABLE})
endif()

set(MODULE_DOX_INPUT ${CMAKE_BINARY_DIR})
qore_external_binary_module(${module_name} ${PROJECT_VERSION})

qore_dist(${PROJECT_VERSION})

qore_user_modules("${QMOD}")
qore_external_user_module("qlib/AmqpUtil" "")
qore_external_user_module("qlib/AmqpDataProvider" "")

# install qpp metadata files
if (COMMAND qore_install_qpp_metadata)
    qore_install_qpp_metadata(${module_name} ${QPP_META_SRC})
else()
    install(FILES ${QPP_META_SRC}
            DESTINATION ${CMAKE_INSTALL_DATADIR}/qore/metadata)
endif()

qore_config_info()

if (DOXYGEN_FOUND)
    qore_wrap_dox(QORE_DOX_SRC ${QORE_DOX_TMPL_SRC})
    add_custom_target(QORE_MOD_DOX_FILES DEPENDS ${QORE_DOX_SRC})
    add_dependencies(docs-module QORE_MOD_DOX_FILES)

    # Two-phase doc build: the initial pass (docs-module) generates amqp.tag with empty
    # TAGFILES. User module docs (docs-AmqpUtil, docs-AmqpDataProvider) are then built using
    # amqp.tag, generating their own tag files. The final pass (docs-module-final) rebuilds
    # the binary module docs with user module tag files in TAGFILES, enabling @ref
    # cross-references from the mainpage into user module symbols.

    # Suppress unresolved cross-reference warnings in the initial pass
    file(APPEND ${CMAKE_BINARY_DIR}/Doxyfile
        "\n# Suppress warnings for initial pass (no user module TAGFILES available)\nWARN_IF_DOC_ERROR = NO\n")

    # Create final-pass Doxyfile from the binary module's Doxyfile with user module tag files
    set(_tagfiles "${CMAKE_BINARY_DIR}/AmqpUtil.tag=../AmqpUtil/html "
        "${CMAKE_BINARY_DIR}/AmqpDataProvider.tag=../AmqpDataProvider/html")
    set(_doc_deps docs-AmqpUtil docs-AmqpDataProvider)

    file(COPY_FILE ${CMAKE_BINARY_DIR}/Doxyfile ${CMAKE_BINARY_DIR}/Doxyfile.final)
    file(APPEND ${CMAKE_BINARY_DIR}/Doxyfile.final
        "\n# Final pass: enable user module cross-references and re-enable doc error warnings\n"
        "TAGFILES = ${_tagfiles}\n"
        "WARN_IF_DOC_ERROR = YES\n")

    add_custom_target(docs-module-final
        COMMAND ${DOXYGEN_EXECUTABLE} ${CMAKE_BINARY_DIR}/Doxyfile.final
        COMMAND ${QORE_DOCS_ENV} ${QORE_QDX_COMMAND} --post
            ${CMAKE_BINARY_DIR}/docs/${module_name}/html
            ${CMAKE_BINARY_DIR}/docs/${module_name}/html/search
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMENT "Generating API documentation with Doxygen (final pass with user module cross-references)"
        VERBATIM
    )
    add_dependencies(docs-module-final ${_doc_deps})
    add_dependencies(docs docs-module-final)
endif()
