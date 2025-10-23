cmake_minimum_required(VERSION 3.28)
project(uDataPacketImport VERSION 0.0.1 LANGUAGES CXX)
enable_testing()

option(BUILD_SHARED_LIBS "Build using shared libraries" ON) 
option(BUILD_FOR_DEBIAN "Build debian container for deployment" OFF)
option(BUILD_TESTS "Compile regression tests" ON)
include(GenerateExportHeader)
include(FetchContent)

FetchContent_Declare(
  readerwriterqueue
  GIT_REPOSITORY    https://github.com/cameron314/readerwriterqueue
  GIT_TAG           8b2176698e9bdaba653cdc20c32b54737a934b47
  EXCLUDE_FROM_ALL
)
FetchContent_MakeAvailable(readerwriterqueue)
FetchContent_Declare(
  concurrentqueue
  GIT_REPOSITORY    https://github.com/cameron314/concurrentqueue
  GIT_TAG           c68072129c8a5b4025122ca5a0c82ab14b30cb03
  EXCLUDE_FROM_ALL
)
FetchContent_MakeAvailable(concurrentqueue)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)
set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
set(Boost_USE_STATIC_LIBS ON)
if (NOT ${BUILD_SHARED_LIBS})
   set(ZLIB_USE_STATIC_LIBS ON)
endif()
find_package(absl CONFIG REQUIRED)
find_package(utf8_range CONFIG REQUIRED)
find_package(protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(spdlog REQUIRED)
find_package(Boost REQUIRED program_options REQUIRED)
find_package(SEEDLink REQUIRED)
find_package(MiniSEED REQUIRED)
find_package(ZLIB REQUIRED)
find_package(Threads REQUIRED)
if (BUILD_TESTS)
   find_package(Catch2 3 REQUIRED)
endif()
configure_file(${CMAKE_SOURCE_DIR}/src/version.hpp.in
               ${CMAKE_SOURCE_DIR}/include/uDataPacketImport/version.hpp)
set(LIBRARY_SRC proto/dataPacketBroadcast.proto
    src/version.cpp
    src/streamIdentifier.cpp
    src/packet.cpp
    src/client.cpp 
    src/sanitizer/expiredPacketDetector.cpp
    src/sanitizer/futurePacketDetector.cpp
    src/seedLink/clientOptions.cpp
    src/seedLink/streamSelector.cpp
    src/seedLink/client.cpp
    src/seedLink/subscriber.cpp
    src/seedLink/subscriberOptions.cpp
    #src/client.cpp
    #src/station.cpp
    #src/database.cpp)
)
string(COMPARE GREATER_EQUAL ${MiniSEED_VERSION} "3.1.5" USE_MS_VERSION_315)
if (BUILD_SHARED_LIBS)
   add_library(uDataPacketImport SHARED ${LIBRARY_SRC})
else()
   add_library(uDataPacketImport STATIC ${LIBRARY_SRC})
endif()
if (USE_MS_VERSION_315)
   message("Using ms_sid2nslc_n")
   target_compile_definitions(uDataPacketImport PRIVATE USE_MS_VERSION_315)
endif()
target_sources(uDataPacketImport
               PUBLIC
               FILE_SET HEADERS
               BASE_DIRS
                  ${CMAKE_CURRENT_SOURCE_DIR}
               FILES 
                  include/uDataPacketImport/version.hpp
                  include/uDataPacketImport/client.hpp
                  include/uDataPacketImport/packet.hpp
                  include/uDataPacketImport/streamIdentifier.hpp
                  include/uDataPacketImport/grpc/client.hpp
                  include/uDataPacketImport/seedLink/clientOptions.hpp
                  include/uDataPacketImport/seedLink/streamSelector.hpp
                  include/uDataPacketImport/seedLink/subscriber.hpp
                  include/uDataPacketImport/seedLink/subscriberOptions.hpp
               )
set_target_properties(uDataPacketImport PROPERTIES
                      CXX_STANDARD 20
                      CXX_STANDARD_REQUIRED YES
                      CXX_EXTENSIONS NO)
target_link_libraries(uDataPacketImport PUBLIC gRPC::grpc gRPC::grpc++
                      PRIVATE protobuf::libprotobuf spdlog::spdlog_header_only
                              readerwriterqueue
                              MiniSEED::MiniSEED SEEDLink::SEEDLink)
target_include_directories(uDataPacketImport
                           PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
                           PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
                           PUBLIC  $<INSTALL_INTERFACE:include>)
protobuf_generate(TARGET uDataPacketImport
                  LANGUAGE cpp)
protobuf_generate(TARGET uDataPacketImport
                  LANGUAGE grpc
                  PLUGIN "protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>"
                  PLUGIN_OPTIONS generate_mock_code=true
                  GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc)

add_executable(uSEEDLinkBroadcast
               src/seedLink/importService.cpp)
set_target_properties(uSEEDLinkBroadcast PROPERTIES
                      CXX_STANDARD 20
                      CXX_STANDARD_REQUIRED YES
                      CXX_EXTENSIONS NO)
target_include_directories(uSEEDLinkBroadcast
                           PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
                           PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/src>
                           PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>)
target_link_libraries(uSEEDLinkBroadcast
                      PRIVATE gRPC::grpc
                              gRPC::grpc++
                              gRPC::grpc++_reflection
                              gRPC::grpcpp_otel_plugin
                              uDataPacketImport spdlog::spdlog_header_only
                              readerwriterqueue
                              Boost::boost Boost::program_options Threads::Threads)

add_executable(uPacketSanitizer
               src/sanitizer/sanitizer.cpp)
set_target_properties(uPacketSanitizer PROPERTIES
                      CXX_STANDARD 20
                      CXX_STANDARD_REQUIRED YES
                      CXX_EXTENSIONS NO)
target_include_directories(uPacketSanitizer
                           PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
                           PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/src>
                           PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>)
target_link_libraries(uPacketSanitizer
                      PRIVATE gRPC::grpc
                              gRPC::grpc++
                              gRPC::grpc++_reflection
                              gRPC::grpcpp_otel_plugin
                              uDataPacketImport spdlog::spdlog_header_only
                              #readerwriterqueue
                              Boost::boost Boost::program_options Threads::Threads)


#add_executable(uDataPacketGRPCProxy src/proxy.cpp)
#set_target_properties(uDataPacketGRPCProxy PROPERTIES
#                      CXX_STANDARD 20
#                      CXX_STANDARD_REQUIRED YES
#                      CXX_EXTENSIONS NO)
#target_include_directories(uDataPacketGRPCProxy
#                           PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
#                           PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>)
#target_link_libraries(uDataPacketGRPCProxy
#                      PRIVATE gRPC::grpc
#                              gRPC::grpc++
#                              gRPC::grpc++_reflection
#                              gRPC::grpcpp_otel_plugin
#                              uDataPacketImport spdlog::spdlog_header_only
#                              concurrentqueue readerwriterqueue
#                              Boost::boost Boost::program_options Threads::Threads)


##########################################################################################
#                                         Tests                                          #
##########################################################################################

add_executable(unitTests 
               testing/packet.cpp
               testing/seedLink.cpp)
set_target_properties(unitTests PROPERTIES
                      CXX_STANDARD 20
                      CXX_STANDARD_REQUIRED YES 
                      CXX_EXTENSIONS NO) 
target_link_libraries(unitTests uDataPacketImport
                      Threads::Threads
                      Catch2::Catch2 Catch2::Catch2WithMain)
target_include_directories(unitTests
                           PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
                           PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/data>
                           PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
                           PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/src>)
add_test(NAME unitTests 
         COMMAND unitTests)


##########################################################################################
#                                      Installation                                      #
##########################################################################################
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
configure_package_config_file(
    cmake/${PROJECT_NAME}Config.cmake.in "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}Config.cmake"
    INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}")
write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}ConfigVersion.cmake"
    VERSION "${version}"
    COMPATIBILITY AnyNewerVersion
)

install(TARGETS uDataPacketImport
        EXPORT ${PROJECT_NAME}-targets
        FILE_SET HEADERS
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        COMPONENT libraries)
install(TARGETS uSEEDLinkBroadcast uPacketSanitizer
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
        COMPONENT applications)
#export(EXPORT ${PROJECT_NAME}-targets
#       FILE "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}Targets.cmake")
install(EXPORT ${PROJECT_NAME}-targets
        NAMESPACE ${PROJECT_NAME}::
        FILE ${PROJECT_NAME}Targets.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME})
install(FILES
        ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}Config.cmake
        ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}ConfigVersion.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME})

##########################################################################################
#                                        Containers                                      #
##########################################################################################
if (${BUILD_FOR_DEBIAN})
   include(CPackComponent)
   configure_file(${CMAKE_CURRENT_SOURCE_DIR}/scripts/Dockerfile.deb.in
                  ${CMAKE_CURRENT_BINARY_DIR}/Dockerfile.deb @ONLY)
   add_custom_target(${PROJECT_NAME}-deb
                     COMMENT "Creating deb file for ${PROJECT_NAME}"
                     COMMAND ${CMAKE_CPACK_COMMAND} -G DEB 
                     WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR} VERBATIM)
   add_dependencies(${PROJECT_NAME}-deb uSEEDLinkBroadcast uPacketSanitizer)
   find_program(buildah_EXECUTABLE buildah)
   if (buildah_EXECUTABLE)
      add_custom_target(buildah
                        COMMENT "Preparing the container with buildah"
                        COMMAND ${buildah_EXECUTABLE} bud -t umetadata:${PROJECT_VERSION} -t umetadata:latest -f ./Dockerfile.deb
                        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR} VERBATIM)
      add_dependencies(buildah ${PROJECT_NAME}-deb)
   endif()
endif()

##########################################################################################
#                                     CPACK Packaging                                    #
##########################################################################################
include(CPackComponent)
set(CPACK_PACKAGE_NAME ${PROJECT_NAME})
set(CPACK_PACKAGE_VENDOR "UUSS")
set(CPACK_PACKAGE_CONTACT "ben.baker@utah.edu")
set(CPACK_PACKAGE_LICENSE "MIT")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "UUSS packetized data import microservice for the UUSS Kubernetes environment.")
set(CPACK_PACKAGE_INSTALL_DIRECTORY ${CPACK_PACKAGE_NAME})
set(CPACK_VERBATIM_VARIABLES YES)
set(CPACK_PACKAGE_VERSION ${PROJECT_VERSION})
set(CPACK_PACKAGE_VERSION_MAJOR ${PROJECT_VERSION_MAJOR})
set(CPACK_PACKAGE_VERSION_MINOR ${PROJECT_VERSION_MINOR})
set(CPACK_PACKAGE_VERSION_PATCH ${PROJECT_VERSION_PATCH})
set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_SOURCE_DIR}/LICENSE")
if (${BUILD_SHARED_LIBS})
   cpack_add_component(libraries
                       DISPLAY_NAME "uDataPacketImport common libraries"
                       DEPENDS ${GRPC_LIBRARY} ${GRPC_GRPC++_LIBRARY} ${SEEDLink_LIBRARIES})
else()
   cpack_add_component(libraries
                       DISPLAY_NAME "uDataPacketImport common libraries")
endif()
cpack_add_component(headers
                    DISPLAY_NAME "uDataPacketImport library header files"
                    DEPENDS libraries)
cpack_add_component(applications
                    DISPLAY_NAME "uDataPacketImport applications"
                    DEPENDS libraries)
set(CPACK_GENERATOR ZIP TGZ)
if (WIN32)
   list(APPEND CPACK_GENERATOR WIX)
elseif (APPLE)
   list(APPEND CPACK_GENERATOR productbuild)
elseif(CMAKE_SYSTEM_NAME STREQUAL "Linux")
   find_program(RPMBUILD_PROGRAM rpmbuild)
   if (RPMBUILD_PROGRAM)
      list(APPEND CPACK_GENERATOR RPM)
   endif()
   if (${BUILD_FOR_DEBIAN})
      find_program(DPKG_PROGRAM dpkg REQUIRED)
   else()
      find_program(DPKG_PROGRAM dpkg)
   endif()
   if (DPKG_PROGRAM)
      list(APPEND CPACK_GENERATOR DEB)
      set(CPACK_DEBIAN_PACKAGE_DEPENDS "${CPACK_DEBIAN_PACKAGE_DEPENDS}")
      set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS ON) 
   endif()
else()
   message("Unknown operating system")
endif()
set(CPACK_SOURCE_IGNORE_FILES
  /\\.git/
  \\.swp
  \\.orig
  /CMakeLists\\.txt\\.user
  /private/
)
include(CPack) # Put this last!
