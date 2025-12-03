#ifndef THREADS_ENTRY_H
#define THREADS_ENTRY_H

#include<pthread.h>


// struct implemented with LLMs
// Internal thread registry (no header changes)
typedef struct _threads_entry {
    void* inst;
    pthread_t send_tid;
    pthread_t recv_tid;
    int send_started;
    int recv_started;
    struct _threads_entry* next;
} _threads_entry;

extern void* generic_thread_wrapper(void* args_ptr);

// private routine adding thread entry
extern struct _threads_entry* _thr_get(void* inst, int create);

// private routine removing threads entry
extern void _thr_remove(void* inst);



typedef struct {
    void* (*routine)(void*);
    void* arg;
    void* inst;
} thread_wrapper_args;

thread_wrapper_args* thread_wrapper_args_init(void* (*routine)(void*), void* arg, void* inst);
#endif