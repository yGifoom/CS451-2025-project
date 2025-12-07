#ifndef PFLX_H
#define PFLX_H

#include"udp.h"
#include"queue.h"
#include"bst_set.h"
#include"parser.h"
#include <pthread.h>

// New thread-safe state for next_id_tbd
typedef struct {
    size_t value;
    pthread_mutex_t mutex;
} delivered_state;

typedef struct pflx{
    UDP* udpSocket;
    queue_t* upQueue;
    queue_t* downQueue;
    

    const Host* phonebook;
    unsigned int* ownMessageID;
    pthread_mutex_t ownMessageID_mutex;
    delivered_state* next_id_tbd; // was size_t*
    bst_set** id_delivered; // array of maps of size phonebook_size
    
    size_t phonebook_size;

    int network_busy;
    pthread_mutex_t network_busy_mutex;

    // Graceful stop flag (protected by mutex)
    int should_stop;
    pthread_mutex_t should_stop_mutex;
} pflx;

// start up the network routines
int pflx_start(pflx* pflx);

// gently stops all routines in network
int pflx_stop(pflx* pflx);

// send with perfect link properties
// will push in downQueue
// downcalled from nodeloop
// processId is 1-based (matches Host IDs in parser)
int pflx_send(pflx* pflx, void* message, size_t messageSize, size_t originID, size_t targetID);

// recieve a message following the perfect link properties
// pops the buffer the pop of upQueue, puts result in buffer
// upcalled by udp
int pflx_recv(pflx* pflx, void* buffer, size_t* buffSize); 

// continuously check in downQueue
// if there should be messages to be sent
// sends them and goes to the next one
// messages are not pushed back into the queue 
// only when their ack is found in the state of pflx
int _pflx_send_routine(pflx* pflx);

// continuously check upd socket
// puts back on the downQueue acks
// pushes in upQueue if the message is to be delivered
int _pflx_recv_routine(pflx* pflx);

// returns 1 (net busy) or 0 (network not busy)
// returns -1 if err
int pflx_network_status(pflx* pflx);

// changes status of net to 0 atomically
// returns -1 if err
int pflx_network_change_status(pflx* pflx);

// construct perfect link
pflx* pflx_init(short unsigned int port, const Host* phonebook, size_t phonebook_size);

// destroy perfect link
int pflx_destroy(pflx* udp);

typedef struct{
    void* message;
    size_t messageSize;

    size_t originID;
    size_t messageID;

    size_t targetID;
} pflx_message;

// initializes message in the pflx format
pflx_message* pflx_message_init(void* message, size_t messageSize, size_t originID, size_t messageID, size_t targetID);

// destroys message in the pflx format
int pflx_message_destroy(pflx_message* message);

// Thread-safe accessors for delivered_state
size_t ack_read(delivered_state* s);

void ack_write(delivered_state* s, size_t v);

// returns 1 if s-> value > v, -1 if s-> value < v, else 0
int ack_compare(delivered_state* s, size_t v);

// Thread-safe helpers for graceful stop
void pflx_request_stop(pflx* pflx);
int pflx_should_stop(pflx* pflx);

#endif // PFLX_H