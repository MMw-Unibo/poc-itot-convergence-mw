include(FetchContent)

FetchContent_Declare(open62541
        GIT_REPOSITORY https://github.com/open62541/open62541.git
        GIT_TAG v1.2.2
        )

add_subdirectory(open62541)