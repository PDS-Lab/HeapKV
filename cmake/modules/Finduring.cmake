# - Find liburing
#
# uring_INCLUDE_DIRS - Where to find liburing.h
# uring_LINK_LIBRARIES - List of libraries when using uring.
# uring_FOUND - True if uring found.

# find_path(uring_INCLUDE_DIR
# NAMES liburing.h)
# find_library(uring_LIBRARIES
# NAMES liburing.a liburing)

# include(FindPackageHandleStandardArgs)
# find_package_handle_standard_args(uring
# DEFAULT_MSG uring_LIBRARIES uring_INCLUDE_DIR)

# mark_as_advanced(
# uring_INCLUDE_DIR
# uring_LIBRARIES)

find_package(PkgConfig REQUIRED)
pkg_check_modules(uring REQUIRED IMPORTED_TARGET liburing)

if(uring_FOUND AND NOT TARGET uring::uring)
  add_library(uring::uring UNKNOWN IMPORTED)
  set_target_properties(uring::uring PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${uring_INCLUDE_DIRS}"
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    IMPORTED_LOCATION "${uring_LINK_LIBRARIES}")
endif()
