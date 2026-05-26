cmake_minimum_required(VERSION 3.16)

project(distributed_model_serving LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

find_package(Protobuf REQUIRED)

find_program(GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin)

if(NOT GRPC_CPP_PLUGIN_EXECUTABLE)
  message(FATAL_ERROR "grpc_cpp_plugin executable not found in PATH")
endif()

find_library(GRPCPP_LIB grpc++)
find_library(GRPC_LIB grpc)

if(NOT GRPCPP_LIB OR NOT GRPC_LIB)
  message(FATAL_ERROR "Could not find gRPC libraries")
endif()

set(PROTO_FILE ${CMAKE_CURRENT_SOURCE_DIR}/protos/serving.proto)

set(PROTO_GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)

file(MAKE_DIRECTORY ${PROTO_GEN_DIR})

set(PROTO_SRC
    ${PROTO_GEN_DIR}/serving.pb.cc
)

set(PROTO_HDR
    ${PROTO_GEN_DIR}/serving.pb.h
)

set(GRPC_SRC
    ${PROTO_GEN_DIR}/serving.grpc.pb.cc
)

set(GRPC_HDR
    ${PROTO_GEN_DIR}/serving.grpc.pb.h
)

add_custom_command(
  OUTPUT
    ${PROTO_SRC}
    ${PROTO_HDR}

  COMMAND ${Protobuf_PROTOC_EXECUTABLE}

  ARGS
    --proto_path=${CMAKE_CURRENT_SOURCE_DIR}/protos
    --cpp_out=${PROTO_GEN_DIR}
    ${PROTO_FILE}

  DEPENDS ${PROTO_FILE}

  COMMENT "Generating Protobuf source files"
)

add_custom_command(
  OUTPUT
    ${GRPC_SRC}
    ${GRPC_HDR}

  COMMAND ${Protobuf_PROTOC_EXECUTABLE}

  ARGS
    --proto_path=${CMAKE_CURRENT_SOURCE_DIR}/protos
    --grpc_out=${PROTO_GEN_DIR}
    --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN_EXECUTABLE}
    ${PROTO_FILE}

  DEPENDS ${PROTO_FILE}

  COMMENT "Generating gRPC source files"
)

add_library(proto_lib STATIC
    ${PROTO_SRC}
    ${GRPC_SRC}
)

target_include_directories(proto_lib PUBLIC
    ${PROTO_GEN_DIR}
)

target_link_libraries(proto_lib PUBLIC
    protobuf::libprotobuf
    ${GRPCPP_LIB}
    ${GRPC_LIB}
)

add_executable(orchestrator src/orchestrator.cpp)

target_include_directories(orchestrator PRIVATE
    ${PROTO_GEN_DIR}
)

target_link_libraries(orchestrator PRIVATE
    proto_lib
)

add_executable(worker src/worker.cpp)

target_include_directories(worker PRIVATE
    ${PROTO_GEN_DIR}
)

target_link_libraries(worker PRIVATE
    proto_lib
)

add_executable(client src/client.cpp)

target_include_directories(client PRIVATE
    ${PROTO_GEN_DIR}
)

target_link_libraries(client PRIVATE
    proto_lib
)

install(TARGETS
    orchestrator
    worker
    client
    DESTINATION bin
)