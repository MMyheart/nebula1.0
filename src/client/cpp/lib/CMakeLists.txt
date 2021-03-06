nebula_add_library(
    client_cpp_obj OBJECT
    NebulaClientImpl.cpp
    ExecutionResponse.cpp
    ConnectionPool.cpp
    ConnectionThread.cpp
)

if (DISABLE_CXX11_ABI)
    add_definitions(-DDISABLE_CXX11_ABI=${DISABLE_CXX11_ABI})
endif()

set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -L ${NEBULA_THIRDPARTY_ROOT}/lib")
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -L ${NEBULA_THIRDPARTY_ROOT}/lib64")
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/symbols.map")

nebula_add_library(
    nebula_client SHARED
    $<TARGET_OBJECTS:base_obj>
    $<TARGET_OBJECTS:time_obj>
    $<TARGET_OBJECTS:common_thrift_obj>
    $<TARGET_OBJECTS:thrift_obj>
    $<TARGET_OBJECTS:graph_thrift_obj>
    ExecutionResponse.cpp
    NebulaClientImpl.cpp
    NebulaClient.cpp
    ConnectionPool.cpp
    ConnectionThread.cpp
)

target_link_libraries(
    nebula_client
    ${OPENSSL_SSL_LIBRARY}
    ${OPENSSL_CRYPTO_LIBRARY}
    ${THRIFT_LIBRARIES}
    wangle
    folly
    boost_context
    boost_system
    glog
    gflags
    snappy
    unwind
    z
    zstd
    lz4
    lzma
    bz2
    event
    double-conversion
    pthread
    dl
    ssl
    crypto
    bz2
    -static-libgcc
    -static-libstdc++
    -Wl,--no-undefined
    ${GETTIME_LIB}
)

install(
    TARGETS nebula_client
    PERMISSIONS
        OWNER_WRITE OWNER_READ
        GROUP_READ
        WORLD_READ
    DESTINATION
        /usr/local/lib
)