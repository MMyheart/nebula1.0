if(DISABLE_NEBULA_CLIENT)
    message(STATUS "disable nebula client")
    nebula_add_library(client_cpp_obj OBJECT GraphClient.cpp)
    return()
endif()

nebula_add_subdirectory(lib)