# DO NOT CHANGE THE SYMBOLIC VARIABLE `SOURCES`.
# You can, however, change the list of files that comprise this variable.

include_directories(include include_tests)
set(SOURCES 
    src/bst_test.c
    src/pflx.c
    src/threads_entry.c
    src/main.c
    src/parser.c
    src/udp.c
    src/bst_set.c
    src/queue.c
    src/logger.c
    src/utils.c
    src/tests.c
    src/ba.c
    src/node.c
)
# DO NOT EDIT THE FOLLOWING LINES
find_package(Threads)
add_executable(da_proc ${SOURCES})
target_link_libraries(da_proc ${CMAKE_THREAD_LIBS_INIT})
