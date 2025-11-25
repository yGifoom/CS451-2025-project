#ifndef FIFO_H
#define FIFO_H

#include"pflx.h"
#include"queue.h"
#include"bst_set.h"
#include<stdatomic.h>


typedef struct{
    queue_t* upQueue;
    queue_t* downQueue;

    atomic_int ownMessageID;
    size_t* next_id_tbd;
    bst_set** id_tbd;

    atomic_flag* shouldStop;

    pflx* pflx_layer;
}fifo;

int fifo_start(fifo* fifo);

int fifo_stop(fifo* fifo);

int fifo_send(fifo* fifo, void* message, size_t messageSize, size_t originID);

int fifo_recv(fifo* fifo, void* message, size_t* messageSize);

int fifo_send_routine(fifo* fifo);

int fifo_recv_routine(fifo* fifo);

// returns number of missing processes
int fifo_broadcast_missing(fifo* fifo, fifo_message* msgToBroadcast);

fifo* fifo_init(pflx* pflx);

int fifo_destroy(fifo* fifo);

typedef struct{
    void* message;
    size_t messageSize;

    size_t messageID;
    size_t originID;
}fifo_message; // PFLX PUTTING INTO QUEUE AND POPPING SHOULD HANDLE THE CAPSULE.

fifo_message* fifo_message_init(void* message, size_t messageSize, size_t messageID, size_t originID);

int fifo_message_destroy(fifo_message* fifoMsg);



#endif