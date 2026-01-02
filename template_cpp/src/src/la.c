// external headers
#include<stdatomic.h>
#include<stdio.h>

// internal
#include"pflx.h"
#include"queue.h"
#include"bst_set.h"
#include"ba.h"
#include"dict.h"
#include"la.h"
#include"threads_entry.h"



static const int CONGESTION_CONTROL = 0;
static const int BUFFERSIZE = 256;
static const long TIMEOUT_QUEUE_POP = 1000; // in ms
static const long MAX_DOWNQUEUE_SIZE = 100;
// Sentinel used to wake and stop the sender loop
#define LA_SHUTDOWN_SENTINEL ((void*)-1)

static void* _send_wrapper(void* arg) {
    la* f = (la*)arg;
    int result = la_send_routine(f);
    return (void*)(intptr_t)result;
}

static void* _recv_wrapper(void* arg) {
    la* f = (la*)arg;
    int result = la_recv_routine(f);
    return (void*)(intptr_t)result;
}

int la_start(la* la);
int la_stop(la* la);

int la_send(la* la, void* message, size_t originID);

int la_recv(la* la, void* message, size_t* messageSize);
int la_send_routine(la* la);
int la_recv_routine(la* la);
int la_deliver(la* la, la_proposal* msg);
la* la_init(pflx* pflx, size_t pid);
int la_destroy(la*);

int beb_with_pflx(pflx*, void*);
int la_bootstrap_from_config(la*, char*);

la_proposal* la_proposal_copy(la_proposal*);
void* la_to_frame(la_proposal);
la_proposal* frame_to_la(void*);
la_proposal* la_proposal_init(ba*, void*);
int la_proposal_destroy(la_proposal* la_prop);