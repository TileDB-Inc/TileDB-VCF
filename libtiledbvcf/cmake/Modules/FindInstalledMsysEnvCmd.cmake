
#
# input variables
# - DRIVE_LETTERS_TO_SEARCH - can be empty, if empty attempt made to find active drive letters
# - DRIVE_LETTERS_TO_IGNORE - can be populated with any drive letters to -not- search
# output variables
# - MSYS2_ENV_CMD - if env.exe found, path to last one found returned in this variable
#

if(NOT DRIVE_LETTERS_TO_SEARCH)
include (${CMAKE_SOURCE_DIR}/cmake/Modules/FindWinDrvLet.cmake)
set(DRIVE_LETTERS_TO_SEARCH "${WINDOWS_DRIVE_LETTERS}")
endif()

set(LAST_FOUND_MSYS2_ENV_CMD)
set(DRIVE_LETTERS_TO_IGNORE "C")
#foreach(drvlet IN LISTS WINDOWS_DRIVE_LETTERS)
foreach(drvlet IN LISTS DRIVE_LETTERS_TO_SEARCH)
  string(FIND DRIVE_LETTERS_TO_IGNORE ${drvlet} DRIVE_LETTER_TO_IGNORE_POS)
  message("find ${drvlet} yields ${DRIVE_LETTER_TO_IGNORE_POS}")
  #if(NOT DRIVE_LETTER_TO_IGNORE_POS EQUAL -1)
  #if(NOT (DRIVE_LETTER_TO_IGNORE_POS EQUAL -1))
  #if(DRIVE_LETTER_TO_IGNORE_POS NOT EQUAL -1)
  if(DRIVE_LETTER_TO_IGNORE_POS LESS_EQUAL -1)
  #if(DRIVE_LETTER_TO_IGNORE_POS EQUAL -1)
  #else()
    continue()
  endif()
  set(MSYS2_ENV_CMD)
  find_program(MSYS2_ENV_CMD
    NAMES env.exe
    PATHS "${drvlet}:/msys64/usr/bin"
    NO_DEFAULT_PATH
    # TBD: removeme, NO_CACHE, for dev purposes...
    NO_CACHE #so it will search any subsequent letters when found earlier...
  )
  if(MSYS2_ENV_CMD)
    message("Found ${MSYS2_ENV_CMD} on drive ${drvlet}:")
    set(LAST_FOUND_MSYS2_ENV_CMD ${MSYS2_ENV_CMD})
  else()
    message("Did not find env.exe on ${drvlet}")
  endif()
endforeach()
set(MSYS2_ENV_CMD ${LAST_FOUND_MSYS2_ENV_CMD})
