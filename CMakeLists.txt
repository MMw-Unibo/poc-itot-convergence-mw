cmake_minimum_required(VERSION 3.0.0)
project(SensorIndustryPOC VERSION 0.1.0)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/bin")

add_subdirectory(deps)

find_package(RdKafka CONFIG REQUIRED)
find_package(json-c CONFIG REQUIRED)

add_executable(subscriber-machine subscriber_machine.c)
target_link_libraries(subscriber-machine
        PRIVATE
        open62541::open62541
        json-c::json-c
        )

add_executable(kafka-consumer kafka_consumer.c)
target_link_libraries(kafka-consumer
        PRIVATE
        open62541::open62541
        RdKafka::rdkafka RdKafka::rdkafka++
        json-c::json-c
        )

add_executable(gateway gateway.c)
target_link_libraries(gateway
        PRIVATE
        open62541::open62541
        RdKafka::rdkafka RdKafka::rdkafka++
        json-c::json-c
        )

add_executable(simulator simulator.c)
target_link_libraries(simulator
        PRIVATE
        open62541::open62541
        json-c::json-c
        )