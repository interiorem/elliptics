include_directories(
    ${PROJECT_BINARY_DIR}
    ${PROJECT_SOURCE_DIR}/foreign/flatbuffers/include
)

set(ELLIPTICS_SRCS
    dnet.c
    notify.c
    server.c
    route.cpp
    backend.cpp
    ../example/config.hpp
    ../example/config.cpp
    ../example/backends.c
    ../example/eblob_backend.cpp
    ../example/eblob_backend.c
    ../grpc/elliptics_generated_wrapper.cpp
    ../grpc/grpc.cpp
    ../grpc/read_job.cpp
    ../grpc/serialization.cpp
    ../grpc/server.cpp
    ../grpc/thread_pool.cpp
    ../grpc/write_job.cpp
    )

add_library(elliptics_ids STATIC ids.cpp)
if(UNIX OR MINGW)
    set_target_properties(elliptics_ids PROPERTIES COMPILE_FLAGS "-fPIC")
endif()

add_library(elliptics SHARED ${ELLIPTICS_SRCS})
set_target_properties(elliptics PROPERTIES
    VERSION ${ELLIPTICS_VERSION}
    SOVERSION ${ELLIPTICS_VERSION_ABI}
    LINKER_LANGUAGE CXX
    )
target_link_libraries(elliptics
    ${ELLIPTICS_LIBRARIES}
    ${EBLOB_LIBRARIES}
    elliptics_common
    elliptics_cache
    elliptics_ids
    elliptics_monitor
    elliptics_client
    kora-util
    gpr
    grpc
    grpc++
    )
add_dependencies(elliptics
    generate-from-schema
    )

install(TARGETS elliptics
    LIBRARY DESTINATION lib${LIB_SUFFIX}
    ARCHIVE DESTINATION lib${LIB_SUFFIX}
    BUNDLE DESTINATION library
    )
