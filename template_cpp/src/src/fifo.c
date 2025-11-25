#include"fifo.h"
#include"pflx.h"
#include"queue.h"
#include"ba.h"
#include"threads_entry.h"
#include<stdlib.h>
#include<stdatomic.h>
#include <errno.h>

const int CONGESTION_CONTROL = 0;
const int BUFFERSIZE = 256;
const long TIMEOUT_QUEUE_POP = 1000; // in ms
const long MAX_DOWNQUEUE_SIZE = 100;
// Sentinel used to wake and stop the sender loop
#define FIFO_SHUTDOWN_SENTINEL ((void*)-1)

static void* _send_wrapper(void* arg) {
    fifo* f = (fifo*)arg;
    int result = _fifo_send_routine(f);
    return (void*)(intptr_t)result;
}

static void* _recv_wrapper(void* arg) {
    fifo* f = (fifo*)arg;
    int result = _fifo_recv_routine(f);
    return (void*)(intptr_t)result;
}

int fifo_send_routine(fifo* fifo){

}

int fifo_recv_routine(fifo* fifo){
    
}


// maybe better if pflx makes frame recursively
static void* fifo_make_frame(fifo_message* m){
    size_t frameSize = 0;
    void* frame = malloc(m->messageSize + sizeof(fifo_message));
    // Build wire frame: [originID, messageID, targetID, payloadSize][payload...]
    const size_t hdr_words = 3;
    const size_t hdr_size = hdr_words * sizeof(size_t);

    size_t* hdr = (size_t*)frame;
    hdr[0] = m->originID;
    hdr[1] = m->messageID;
    hdr[2] = m->messageSize;
    memcpy(frame + hdr_size, m->message, m->messageSize);
    
    size_t frame_size = hdr_size + m->messageSize;

    return frame_size;
}

int fifo_broadcast_missing(fifo* fifo, fifo_message* msgToBroadcast){

    ba* acksForMsg; size_t* missingProcesses; size_t sizeMissing; 
    bst_set_lookup(fifo->id_tbd, msgToBroadcast->originID, &acksForMsg, NULL);

    ba_where_0(acksForMsg, &missingProcesses, &sizeMissing);
    if (sizeMissing <= 0){
        return 0;
    }
    void* fifoFrame = fifo_make_frame(msgToBroadcast);

    size_t msgSize = msgToBroadcast->messageSize + sizeof(fifo_message);
    for (int i = 0; i < sizeMissing; i++){
        // MEMOPT: right now send memcopies, could save in one location and 
        // keep track of instances floating around
        pflx_send(fifo->pflx_layer, 
            fifoFrame, msgSize,
            msgToBroadcast->originID, missingProcesses[i]+1); // +1 for index to pid convertion
    }
    fre(fifoFrame);
    return sizeMissing;
}

int fifo_send(fifo* fifo, void* message, size_t messageSize, size_t originID){
    int msgID;
    atomic_fetch_add(&msgID, 1);msgID++;
    
    fifo_message* msg = fifo_message_init(message, messageSize, msgID, originID);
    if (msg == NULL) {
        return 1;
    }

    int res = queue_push(fifo->downQueue, msg, sizeof(fifo_message*)); // push pointer value
    if (res != 0){
        return res;
    }

    return 0;
}

int fifo_recv(fifo* fifo, void* message, size_t* messageSize){
    fifo_message* msg = NULL;
    size_t msgPtrSize = sizeof(fifo_message*);

    int res = queue_pop_timed(fifo->upQueue, (void **)&msg, &msgPtrSize, TIMEOUT_QUEUE_POP);
    if(res != 0){
        if (res == ETIMEDOUT){
            return res;
        }
        return res;
    }

    memcpy(message, msg->message, msg->messageSize);
    *messageSize = msg->messageSize;
    fifo_message_destroy(msg);
    return 0;
}

int fifo_start(fifo* fifo){
    _threads_entry* e = _thr_get(fifo, 1);
    if (!e) return -1;

    // Start receiver first
    thread_wrapper_args* recvArgs = thread_wrapper_args_init(_recv_wrapper, fifo, fifo);
    if (!recvArgs || pthread_create(&e->recv_tid, NULL, generic_thread_wrapper, recvArgs) != 0) {
        free(recvArgs);
        return -1;
    }
    e->recv_started = 1;

    // Start sender
    thread_wrapper_args* sendArgs = thread_wrapper_args_init(_send_wrapper, fifo, fifo);
    if (!sendArgs || pthread_create(&e->send_tid, NULL, generic_thread_wrapper, sendArgs) != 0) {
        free(sendArgs);
        // rollback receiver
        pthread_cancel(e->recv_tid);
        pthread_join(e->recv_tid, NULL);
        e->recv_started = 0;
        return -1;
    }
    e->send_started = 1;

    return 0;
}

int fifo_stop(fifo* fifo){
    _threads_entry* e = _thr_get(fifo, 0);
    if (!e) return -1;

    // reset graceful stop flag
    atomic_flag_test_and_set(fifo->shouldStop);

    // Wake sender loop (if waiting on downQueue)
    if (e->send_started) {
        (void)queue_push(fifo->downQueue, FIFO_SHUTDOWN_SENTINEL, sizeof(fifo_message*));
    }

    // Join threads BEFORE removing entry
    if (e->send_started) { 
        pthread_join(e->send_tid, NULL); 
        e->send_started = 0; 
    }
    if (e->recv_started) { 
        pthread_join(e->recv_tid, NULL); 
        e->recv_started = 0; 
    }

    _thr_remove(fifo);
    return 0;
}

fifo* fifo_init(pflx* pflx){
    fifo* fifo_socket = malloc(sizeof(fifo));
    fifo_socket->upQueue = queue_init();
    fifo_socket->downQueue = queue_init();
    
    fifo_socket->ownMessageID = 0;
    fifo_socket->next_id_tbd = malloc(sizeof(size_t) * pflx->phonebook_size);
    fifo_socket->id_tbd = malloc(sizeof(void *) * pflx->phonebook_size);
    for(int i = 0; i < pflx->phonebook_size; i++){
        fifo_socket->id_tbd[i] = bst_set_init();
    }
    atomic_flag_clear(&fifo_socket->shouldStop);
    fifo_socket->pflx_layer = pflx;

    return fifo_socket;
}

int fifo_destroy(fifo* fifo_socket){
    int res = 0;
    res = queue_destroy(fifo_socket->downQueue);
    res = res + queue_destroy(fifo_socket->upQueue);
    res = res + pflx_destroy(fifo_socket->pflx_layer);

    free(fifo_socket->next_id_tbd);
    bst_set_destroy(fifo_socket->id_tbd);

    free(fifo_socket);
    return res;

}

fifo_message* fifo_message_init(void* message, size_t messageSize, size_t messageID, size_t originID){
    fifo_message* fifoMsg = malloc(sizeof(fifo_message));
    fifoMsg->message = message;
    fifoMsg->messageSize = messageSize;
    fifoMsg->messageID = messageID;
    fifoMsg->originID = originID;

    return fifoMsg;
}

int fifo_message_destroy(fifo_message* fifoMsg){
    free(fifoMsg->message);
    free(fifoMsg);

    return 0;
}