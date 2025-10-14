#.rst:
# FindMiniSEED
# ------------
# 
# Finds the MiniSEED library and include.
#
# Imported targets
# ^^^^^^^^^^^^^^^^
#
# ``MiniSEED_FOUND``
#   True indicates MiniSEED was found.
# ``MiniSEED_VERSION_STRING``
#   The version found.
# ``MiniSEED::MiniSEED``
#   The MiniSEED library, if found.

# Already in cache, be silent
if (MiniSEED_INCLUDE_DIR AND MiniSEED_LIBRARY)
    set(MiniSEED_FIND_QUIETLY TRUE)
endif()

# Find the include directory
find_path(MiniSEED_INCLUDE_DIR
          NAMES libmseed.h
          PATHS /usr/local/include
                /usr/include
                "$ENV{MiniSEED_ROOT}/include"
                "$ENV{MiniSEED_ROOT}/")
# Find the library components
if (BUILD_SHARED_LIBS)
   message("Looking for libmseed shared")
   find_library(MiniSEED_LIBRARY
                NAME libmseed.so
                PATHS $ENV{MiniSEED}/lib/
                      $ENV{MiniSEED}/
                      /usr/local/lib64
                      /usr/local/lib
               )
else()
   message("Looking for libmseed static")
   find_library(MiniSEED_LIBRARY
                NAME libmseed.a
                PATHS $ENV{MiniSEED}/lib/
                      $ENV{MiniSEED}/
                      /usr/local/lib64
                      /usr/local/lib
               )
endif()
file(STRINGS ${MiniSEED_INCLUDE_DIR}/libmseed.h MSEED_HEADER_DATA)
set(MiniSEED_VERSION "")
while (MSEED_HEADER_DATA)
  list(POP_FRONT MSEED_HEADER_DATA LINE)
  if (LINE MATCHES "#define LIBMSEED_VERSION")
     message("Found mseed version line: " ${LINE})
     string(REPLACE "#define LIBMSEED_VERSION " "" LINE ${LINE})
     string(REPLACE "\"" "" LINE ${LINE})
     string(REPLACE "//!< Library version" "" LINE ${LINE})
     string(STRIP ${LINE} LINE)
     set(LINE_LIST ${LINE})
     list(GET LINE_LIST 0 MiniSEED_VERSION)
     #string(COMPARE GREATER ${VERSION} "3.1.5" RESULT) 
     #if (RESULT)
     #   set(MiniSEED_VERSION ${VERSION})
     #endif()
     message("Extracted mseed version ${MiniSEED_VERSION}")
     break()
  endif()
endwhile()
#message("value " ${MSEED_HEADER_DATA})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(MiniSEED
                                  FOUND_VAR MiniSEED_FOUND
                                  REQUIRED_VARS MiniSEED_LIBRARY MiniSEED_INCLUDE_DIR
                                  VERSION_VAR ${MiniSEED_VERSION})
if (MiniSEED_FOUND AND NOT TARGET MiniSEED::MiniSEED)
   add_library(MiniSEED::MiniSEED UNKNOWN IMPORTED)
   set_target_properties(MiniSEED::MiniSEED PROPERTIES
                         IMPORTED_LOCATION "${MiniSEED_LIBRARY}"
                         INTERFACE_INCLUDE_DIRECTORIES "${MiniSEED_INCLUDE_DIR}")
endif()
mark_as_advanced(MiniSEED_INCLUDE_DIR MiniSEED_LIBRARY)

#include(FindPackageHandleStandardArgs)
#find_package_handle_standard_args(MiniSEED
#                                  DEFAULT_MSG MINISEED_INCLUDE_DIR MINISEED_LIBRARY MINISEED_VERSION)
#mark_as_advanced(MINISEED_INCLUDE_DIR MINISEED_LIBRARY MINISEED_VERSION)
