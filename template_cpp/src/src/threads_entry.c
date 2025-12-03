#include"threads_entry.h"
#include<stdlib.h>
#include<pthread.h>

static _threads_entry* _thr_head = NULL;
static pthread_mutex_t _thr_mu = PTHREAD_MUTEX_INITIALIZER;

// Thread entry points

extern thread_wrapper_args* thread_wrapper_args_init(void* (*routine)(void*), void* arg, void* inst){
    thread_wrapper_args* thread_wrapper = malloc(sizeof(void*) * 3);
    thread_wrapper->arg = arg;
    thread_wrapper->inst = inst;
    thread_wrapper->routine = routine;
    return thread_wrapper;
}

extern void* generic_thread_wrapper(void* args_ptr) {
    thread_wrapper_args* targs = (thread_wrapper_args*)args_ptr;
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);
    
    void* result = targs->routine(targs->arg);
    void* inst = targs->inst;
    free(targs);
    
    return result;
}

extern _threads_entry* _thr_get(void* inst, int create) {
    pthread_mutex_lock(&_thr_mu);
    _threads_entry* cur = _thr_head;
    while (cur) {
        if (cur->inst == inst) { pthread_mutex_unlock(&_thr_mu); return cur; }
        cur = cur->next;
    }
    if (!create) { pthread_mutex_unlock(&_thr_mu); return NULL; }
    _threads_entry* e = calloc(1, sizeof(*e));
    if (!e) { pthread_mutex_unlock(&_thr_mu); return NULL; }
    e->inst = inst;
    e->next = _thr_head;
    _thr_head = e;
    pthread_mutex_unlock(&_thr_mu);
    return e;
}

extern void _thr_remove(void* inst) {
    pthread_mutex_lock(&_thr_mu);
    _threads_entry** link = &_thr_head;
    while (*link) {
        if ((*link)->inst == inst) {
            _threads_entry* del = *link;
            *link = del->next;
            free(del);
            break;
        }
        link = &(*link)->next;
    }
    pthread_mutex_unlock(&_thr_mu);
}