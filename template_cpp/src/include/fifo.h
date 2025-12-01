#ifndef FIFO_H
#define FIFO_H

#include"pflx.h"
#include"queue.h"
#include"bst_set.h"
#include"ba.h"
#include<stdatomic.h>


typedef struct{
    int pid;

    queue_t* upQueue;
    queue_t* downQueue;
    int network_busy;

    atomic_uint ownMessageID;
    size_t* next_id_tbd;
    bst_set** id_tbd;
    pthread_mutex_t* tbd_mutexes;

    atomic_int shouldStop;

    pflx* pflx_layer;
}fifo;

typedef struct{
    void* message;
    size_t messageSize;
    ba* acks;

    size_t messageID;
    size_t originID;
}fifo_message;

int fifo_start(fifo* fifo);

int fifo_stop(fifo* fifo);

int fifo_send(fifo* fifo, void* message, size_t messageSize, size_t originID);

int fifo_recv(fifo* fifo, void* message, size_t* messageSize);

int fifo_send_routine(fifo* fifo);

int fifo_recv_routine(fifo* fifo);

int fifo_deliver(fifo* fifo, fifo_message* msg);

// returns number of missing processes
size_t fifo_broadcast_missing(fifo* fifo, fifo_message* msgToBroadcast);

fifo* fifo_init(pflx* pflx);

int fifo_destroy(fifo* fifo);

fifo_message* fifo_message_init(void* message, size_t messageSize, size_t messageID, size_t originID, size_t numAcks);

int fifo_message_destroy(fifo_message* fifoMsg);





#endif