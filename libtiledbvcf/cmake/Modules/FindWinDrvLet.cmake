set(WINDRVLET_FOUND FALSE)

if(NOT WIN32)
  return()
endif()

find_program(FSUTIL_CMD
  NAMES fsutil.exe
)

if (NOT FSUTIL_CMD)
  message(WARNING "Did not find fsutil!")
else()
  if(1)
  execute_process(COMMAND ${FSUTIL_CMD} fsinfo drives OUTPUT_VARIABLE FSUTIL_DRIVES_RESULT)
  # example output in FSUTIL_DRIVES_RESULT... single line preceded by eol followed by eol,
  # trailing space after last drive specifier
  #"
  #Drives: C:\ D:\ E:\ F:\ T:\
  #"
  message(STATUS "FSUTIL_DRIVES_RESULT is \"${FSUTIL_DRIVES_RESULT}\"")
  string(REPLACE "\\" "/" DRIVE_SPECIFIERS_FWD_SLASH ${FSUTIL_DRIVES_RESULT})
  message(STATUS "DRIVE_SPECIFIERS_FWD_SLASH is \"${DRIVE_SPECIFIERS_FWD_SLASH}\"")
  string(REGEX REPLACE "[\r\n]" "" FSUTIL_DRIVES_STRIPPED ${DRIVE_SPECIFIERS_FWD_SLASH})
  message(STATUS "FSUTIL_DRIVES_STRIPPED is \"${FSUTIL_DRIVES_STRIPPED}\"")
  # remove trailing space after last item
  string(STRIP ${FSUTIL_DRIVES_STRIPPED} FSUTIL_DRIVES_STRIPPED)
  message(STATUS "FSUTIL_DRIVES_STRIPPED is \"${FSUTIL_DRIVES_STRIPPED}\"")
  # remove the "Drives: " prefix
  string(REPLACE "Drives: " "" FSUTIL_DRIVES_STRIPPED ${FSUTIL_DRIVES_STRIPPED})
  # TBD: Will spaces work as well as semi's for list iteration? seems not for 'IN LISTS'...
  # contrary to what I thought docs indicated, spaces don't work, sub semicolons
  string(REPLACE " " ";" DRIVE_SPECIFIERS_FWD_SLASH_LIST ${FSUTIL_DRIVES_STRIPPED})
  message(STATUS "DRIVE_SPECIFIERS_FWD_SLASH_LIST is \"${DRIVE_SPECIFIERS_FWD_SLASH_LIST}\"")
  string(REPLACE ":/" "" WINDOWS_DRIVE_LETTERS ${FSUTIL_DRIVES_STRIPPED})
  message(STATUS "WINDOWS_DRIVE_LETTERS is ${WINDOWS_DRIVE_LETTERS}")
  #foreach(drvspec IN LISTS DRIVE_SPECIFIERS_FWD_SLASH)
  foreach(drvspec IN LISTS DRIVE_SPECIFIERS_FWD_SLASH_LIST)
    message(STATUS "drvspec is \"${drvspec}\"")
  endforeach()
  #string(REPLACE ":/" "" WINDOWS_DRIVE_LETTERS ${DRIVE_SPECIFIERS_FWD_SLASH_LIST})
  string(REPLACE ":/" "" WINDOWS_DRIVE_LETTERS "${DRIVE_SPECIFIERS_FWD_SLASH_LIST}")
  message(STATUS "WINDOWS_DRIVE_LETTERS is ${WINDOWS_DRIVE_LETTERS}")
  foreach(drvlet IN LISTS WINDOWS_DRIVE_LETTERS )
    message(STATUS "drvlet is \"${drvlet}\"")
  endforeach()
  else()
  execute_process(COMMAND ${FSUTIL_CMD} fsinfo drives OUTPUT_VARIABLE FSUTIL_DRIVE_RESULT)
  message(STATUS "FSUTIL_DRIVE_RESULT is ${FSUTIL_DRIVE_RESULT}")
  string(REPLACE "Drives: " "" FSUTIL_DRIVE_SPECIFIERS ${FSUTIL_DRIVE_RESULT})
  message(STATUS "FSUTIL_DRIVE_SPECIFIERS is ${FSUTIL_DRIVE_SPECIFIERS}")
  string(REPLACE " " ";" FSUTIL_DRIVE_SPECIFIER_LIST ${FSUTIL_DRIVE_SPECIFIERS})
  message(STATUS "FSUTIL_DRIVE_SPECIFIER_LIST is ${FSUTIL_DRIVE_SPECIFIER_LIST}")
  string(REPLACE "\\" "/" DRIVE_SPECIFIERS_FWD_SLASH ${FSUTIL_DRIVE_RESULT})
  message(STATUS "DRIVE_SPECIFIERS_FWD_SLASH is ${DRIVE_SPECIFIERS_FWD_SLASH}")
  message(STATUS "DRIVE_SPECIFIERS_FWD_SLASH is |${DRIVE_SPECIFIERS_FWD_SLASH}|")
  #foreach(drvspec ${FSUTIL_DRIVE_SPECIFIER_LIST})
  #foreach(drvspec IN LISTS ${FSUTIL_DRIVE_SPECIFIER_LIST})
  string(REPLACE "\\" "/" FSUTIL_DRIVE_SPECIFIER_LIST_FS ${FSUTIL_DRIVE_SPECIFIER_LIST})
  #string(REPLACE "[\r\n]" "" FSUTIL_DRIVE_SPECIFIER_LIST_FS2 ${FSUTIL_DRIVE_SPECIFIER_LIST_FS})
  message(STATUS "FSUTIL_DRIVE_SPECIFIER_LIST_FS2 is |${FSUTIL_DRIVE_SPECIFIER_LIST_FS2}|")
  #foreach(drvspec IN LISTS FSUTIL_DRIVE_SPECIFIER_LIST_FS)
  foreach(drvspec IN LISTS FSUTIL_DRIVE_SPECIFIER_LIST_FS2)
    message(STATUS "drvspec is ${drvspec}")
  endforeach()
  foreach(drvspec IN LISTS DRIVE_SPECIFIERS_FWD_SLASH)
    message(STATUS "drvspec is ${drvspec}")
    message(STATUS "huh")
  endforeach()
  endif()
endif()
