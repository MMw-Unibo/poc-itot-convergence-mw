FetchContent_GetProperties(open62541)
if (NOT open62541_POPULATED)
    FetchContent_Populate(open62541)

    set(UA_ENABLE_PUBSUB "true")
    add_subdirectory(${open62541_SOURCE_DIR} ${open62541_BINARY_DIR})
endif()