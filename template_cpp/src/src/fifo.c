#include"fifo.h"
#include"pflx.h"
#include"queue.h"
#include"ba.h"
#include"threads_entry.h"
#include<stdlib.h>
#include<stdatomic.h>
#include<errno.h>
#include<string.h>
#include<stdio.h>

static const int CONGESTION_CONTROL = 0;
static const int BUFFERSIZE = 256;
static const long TIMEOUT_QUEUE_POP = 1000; // in ms
static const long MAX_DOWNQUEUE_SIZE = 100;
// Sentinel used to wake and stop the sender loop
#define FIFO_SHUTDOWN_SENTINEL ((void*)-1)

static void* _send_wrapper(void* arg) {
    fifo* f = (fifo*)arg;
    int result = fifo_send_routine(f);
    return (void*)(intptr_t)result;
}

static void* _recv_wrapper(void* arg) {
    fifo* f = (fifo*)arg;
    int result = fifo_recv_routine(f);
    return (void*)(intptr_t)result;
}

static fifo_message* fifo_unframe(void* frame, size_t frameSize){
    if (!frame || frameSize < 5 * sizeof(size_t)) {
        if (!frame){
            printf("FIFO FRAME: fifo frame provided is null\n"); fflush(stdout);
            return NULL;
        } else{
            printf("FIFO FRAME: frameSize provided has wrong value (%zu instead of 5 size_t)\n", frameSize); fflush(stdout);
            return NULL;
        }
    }

    const size_t hdr_words = 5;
    const size_t hdr_size = hdr_words * sizeof(size_t);
    
    size_t* hdr = (size_t*)frame;
    size_t originID = hdr[0];
    // hdr[1] is targetID (not needed for reconstruction)
    size_t messageID = hdr[2];
    size_t messageSize = hdr[3];
    size_t acksLength = hdr[4];
    
    // Calculate bit array size
    size_t num_blocks = (acksLength + 63) / 64; // Round up to blocks
    size_t baSize = num_blocks * 8;
    
    // Validate frame size
    if (frameSize < hdr_size + baSize + messageSize) {
        printf("FIFO FRAME: frame reconstructed has wrong size\n"); fflush(stdout);
        return NULL;
    }
    
    // Reconstruct bit array
    void* bitArrayData = (unsigned char*)frame + hdr_size;
    ba* acks = ba_construct(bitArrayData, acksLength);
    if (!acks) {
        printf("FIFO FRAME: could not make ba from frame\n"); fflush(stdout);
        return NULL;
    }
    
    // Copy message payload
    void* messageData = malloc(messageSize);
    if (!messageData) {
        ba_destroy(acks);
        return NULL;
    }
    memcpy(messageData, (unsigned char*)frame + hdr_size + baSize, messageSize);
    
    // Create fifo_message
    fifo_message* msg = malloc(sizeof(fifo_message));
    if (!msg) {
        free(messageData);
        ba_destroy(acks);
        printf("FIFO FRAME: failed to construct fifo_message struct\n"); fflush(stdout);
        return NULL;
    }
    
    msg->message = messageData;
    msg->messageSize = messageSize;
    msg->messageID = messageID;
    msg->originID = originID;
    msg->acks = acks;
    
    return msg;
}

int fifo_send_routine(fifo* fifo){
    unsigned char* frame = malloc(BUFFERSIZE);
    printf("FIFO SEND ROUTINE: started\n"); fflush(stdout);

    while(1){
        if (atomic_load_explicit(&fifo->shouldStop, memory_order_relaxed)){
            printf("FIFO SEND ROUTINE: stop requested, exiting\n"); fflush(stdout);
            break;
        }

        void* popped = NULL; size_t msgSize;
        int res = queue_pop_timed(fifo->downQueue, &popped, &msgSize, TIMEOUT_QUEUE_POP);
        if (res != 0){
            if (res == ETIMEDOUT){
                continue;
            }
            printf("FIFO SEND ROUTINE: queue_pop_timed failed with error %d\n", res); fflush(stdout);
            free(frame);
            return res;
        }
        // Check for shutdown sentinel
        if (popped == FIFO_SHUTDOWN_SENTINEL) {
            printf("FIFO SEND ROUTINE: shutdown sentinel received, exiting\n"); fflush(stdout);
            break;
        }

        fifo_message* msgToSend = (fifo_message*)popped;
        printf("FIFO SEND ROUTINE: processing message ID %zu from origin %zu\n", 
               msgToSend->messageID, msgToSend->originID); fflush(stdout);

        size_t remainingAcks = fifo_broadcast_missing(fifo, msgToSend);
        printf("FIFO SEND ROUTINE: broadcast returned %zu missing processes\n", remainingAcks); fflush(stdout);
        
        if (remainingAcks < fifo->pflx_layer->phonebook_size/2 - 1){
            printf("FIFO SEND ROUTINE: delivering message ID %zu (enough acks received)\n", 
                   msgToSend->messageID); fflush(stdout);
            fifo_deliver(fifo, msgToSend);
        }
        queue_push(fifo->downQueue, msgToSend, msgSize);
    }
    printf("FIFO SEND ROUTINE: exiting\n"); fflush(stdout);
    return 0;
}

int fifo_recv_routine(fifo* fifo){
    // init
    unsigned char* buffer = malloc(BUFFERSIZE);
    printf("FIFO RECV ROUTINE: started\n"); fflush(stdout);

    while(1){
        if (atomic_load_explicit(&fifo->shouldStop, memory_order_relaxed)){
            printf("FIFO RECV ROUTINE: stop requested, exiting\n"); fflush(stdout);
            break;
        }

        void* popped = NULL; size_t msgSize;
        int res = queue_pop_timed(fifo->pflx_layer->upQueue, &popped, &msgSize, TIMEOUT_QUEUE_POP);
        if (res != 0){
            if (res == ETIMEDOUT){
                continue;
            }
            printf("FIFO RECV ROUTINE: queue_pop_timed failed with error %d\n", res); fflush(stdout);
            free(buffer);
            return res;
        }
        // Check for shutdown sentinel
        if (popped == FIFO_SHUTDOWN_SENTINEL) {
            printf("FIFO RECV ROUTINE: shutdown sentinel received, exiting\n"); fflush(stdout);
            break;
        }
        
        size_t* hdr = (size_t*)popped;
        printf("FIFO RECV ROUTINE: received frame with originID=%zu, targetID=%zu, messageID=%zu\n",
               hdr[0], hdr[1], hdr[2]); fflush(stdout);
        if(hdr[0] > fifo->pflx_layer->phonebook_size || hdr[1] > fifo->pflx_layer->phonebook_size){
            printf("FIFO RECV ROUTINE: got message from invalid originid %zu\n", hdr[0]); fflush(stdout);
            free(popped);
            continue;
        } 

        fifo_message* msgRecvd = NULL;
        // handle recv from pflx layer

        // this routine is the only place where bst might be modified
        // no need to sync if only 1 recv thread
        fifo_message* oldMsg = NULL; size_t acks_size;
        msgRecvd = fifo_unframe(popped, msgSize);
        
        if (!msgRecvd) {
            printf("FIFO RECV ROUTINE: failed to unframe message\n"); fflush(stdout);
            free(popped);
            continue;
        }
        printf("FIFO RECV ROUTINE: unframed message ID %zu from origin %zu\n",
               msgRecvd->messageID, msgRecvd->originID); fflush(stdout);

        // Add sender's ack (targetID from header converted to index)
        ba_add(msgRecvd->acks, hdr[1]-1); // convertion pid -> index
        
        // Convert originID to array index
        size_t originIdx = hdr[0] - 1;
        
        if(bst_set_lookup(fifo->id_tbd[originIdx], hdr[2], (void**)&oldMsg, (void*)&acks_size) == 1){
            printf("FIFO RECV ROUTINE: message already exists in BST, merging acks\n"); fflush(stdout);
            ba_merge(oldMsg->acks, msgRecvd->acks);
            fifo_message_destroy(msgRecvd);
            msgRecvd = oldMsg;
        } else{
            printf("FIFO RECV ROUTINE: new message, adding to BST\n"); fflush(stdout);
            bst_set_add(fifo->id_tbd[originIdx], hdr[2], 
            (void*)msgRecvd, sizeof(fifo_message));
        }

        if (fifo->next_id_tbd[originIdx] == hdr[2]){
            printf("FIFO RECV ROUTINE: message matches next expected ID, checking for delivery\n"); fflush(stdout);
            
            while (ba_where_1(msgRecvd->acks, NULL, NULL) > fifo->pflx_layer->phonebook_size/2){
                printf("FIFO RECV ROUTINE: delivering message ID %zu from origin %zu\n",
                       msgRecvd->messageID, msgRecvd->originID); fflush(stdout);
                
                fifo_deliver(fifo, msgRecvd);
                fifo->next_id_tbd[originIdx]++; // not atomic
                printf("FIFO RECV ROUTINE: next expected ID for origin %zu is now %zu\n",
                       hdr[0], fifo->next_id_tbd[originIdx]); fflush(stdout);

                if(bst_set_lookup(fifo->id_tbd[originIdx], fifo->next_id_tbd[originIdx], 
                (void**)&msgRecvd, (void*)&msgSize) != 1){
                    printf("FIFO RECV ROUTINE: no more consecutive messages to deliver\n"); fflush(stdout);
                    break;
                }
            }
        } else {
            printf("FIFO RECV ROUTINE: message ID %zu does not match next expected %zu, buffering\n",
                   hdr[2], fifo->next_id_tbd[originIdx]); fflush(stdout);
        }
        
        free(popped);
    }
    printf("FIFO RECV ROUTINE: exiting\n"); fflush(stdout);
    return 0;
}

int fifo_deliver(fifo* fifo, fifo_message* msg){
    //OPT: send a message to everyone that they should deliver?
    printf("FIFO DELIVER: delivering message ID %zu from origin %zu to upQueue\n",
           msg->messageID, msg->originID); fflush(stdout);
    int res = queue_push(fifo->upQueue, msg->message, msg->messageSize);
    if (res != 0) {
        printf("FIFO DELIVER: failed to push to upQueue, error %d\n", res); fflush(stdout);
    }
    return res;
}


//OPT: maybe better if pflx makes frame recursively
static unsigned char* fifo_make_frame(fifo_message* m, size_t* frameSize){
    // Build wire frame: [originID, messageID, targetID, payloadSize][payload...]
    const size_t hdr_words = 5;
    const size_t hdr_size = hdr_words * sizeof(size_t);

    void* bitArray = ba_unsafe_copy(m->acks);
    size_t baSize = m->acks->num_blocks * 8;

    size_t frameSizeLocal = hdr_size + baSize + m->messageSize;
    unsigned char* frame = malloc(frameSizeLocal);
    if (!frame) {
        free(bitArray);
        return NULL;
    }

    size_t* hdr = (size_t*)frame;
    hdr[0] = m->originID;
    hdr[1] = 0; // fill in later
    hdr[2] = m->messageID;
    hdr[3] = m->messageSize;
    hdr[4] = m->acks->length; // we know block size to be 64 bits
    
    // copying bit array
    memcpy(frame + hdr_size, bitArray, baSize);

    // copying message payload
    memcpy(frame + hdr_size + baSize, m->message, m->messageSize);

    *frameSize = frameSizeLocal;
    free(bitArray);
    return frame;
}

size_t fifo_broadcast_missing(fifo* fifo, fifo_message* msgToBroadcast){
    printf("FIFO BROADCAST: broadcasting message ID %zu from origin %zu\n",
           msgToBroadcast->messageID, msgToBroadcast->originID); fflush(stdout);

    fifo_message* Msg; size_t* missingProcesses; size_t sizeMissing; 
    if(bst_set_lookup(fifo->id_tbd[msgToBroadcast->originID], msgToBroadcast->messageID, 
        (void**)&Msg, NULL) != 1){
            Msg = msgToBroadcast;
    } else{
        ba_merge(Msg->acks, msgToBroadcast->acks);
    }

    ba_where_0(Msg->acks, &missingProcesses, &sizeMissing);
    printf("FIFO BROADCAST: found %zu missing processes\n", sizeMissing); fflush(stdout);
    
    if (sizeMissing <= 0){
        return 0;
    }
    
    size_t frameSize;
    unsigned char* fifoFrame = fifo_make_frame(msgToBroadcast, &frameSize);
    size_t* hdr = (size_t*)fifoFrame;

    if (fifoFrame == NULL){
        printf("FIFO BROADCAST: make frame run into an error  %zu\n", hdr[1]); fflush(stdout);
        return sizeMissing;
    }
    printf("FIFO BROADCAST: made frame with size %zu: originID:%zu, messageID:%zu, message size: %zu, ack size: %zu\n", 
        frameSize, hdr[0], hdr[2], hdr[3], hdr[4]); fflush(stdout);

    for (size_t i = 0; i < sizeMissing; i++){
        // MEMOPT: right now send memcopies, could save in one location and 
        // keep track of instances floating around

        // change targetID position: hdr[1]
        hdr[1] = missingProcesses[i]+1;
        printf("FIFO BROADCAST: sending to process %zu, frameSize=%zu\n", hdr[1], frameSize); fflush(stdout);
        
        printf("FIFO BROADCAST: calling pflx_send with frame pointer=%p, size=%zu\n", 
               (void*)fifoFrame, frameSize); fflush(stdout);
        
        pflx_send(fifo->pflx_layer, 
            fifoFrame, frameSize,
            msgToBroadcast->originID, missingProcesses[i]+1); 
    }
    free(fifoFrame);
    return sizeMissing;
}

int fifo_send(fifo* fifo, void* message, size_t messageSize, size_t originID){
    atomic_uint msgID;
    while(1){
        msgID = atomic_load_explicit(&fifo->ownMessageID, memory_order_relaxed);
        if(atomic_compare_exchange_weak(&fifo->ownMessageID, &msgID, msgID + 1)){
            break;
        }
    }
    
    printf("FIFO SEND: creating message ID %u from origin %zu\n", msgID, originID); fflush(stdout);
    
    fifo_message* msg = fifo_message_init(message, messageSize, msgID, originID, fifo->pflx_layer->phonebook_size);
    if (msg == NULL) {
        printf("FIFO SEND: failed to create fifo_message\n"); fflush(stdout);
        return 1;
    }

    int res = queue_push(fifo->downQueue, msg, sizeof(fifo_message*)); // push pointer value
    if (res != 0){
        printf("FIFO SEND: failed to push to downQueue, error %d\n", res); fflush(stdout);
        return res;
    }

    printf("FIFO SEND: message queued successfully\n"); fflush(stdout);
    return 0;
}

int fifo_recv(fifo* fifo, void* message, size_t* messageSize){
    fifo_message* msg = NULL;
    size_t msgPtrSize = sizeof(fifo_message*);
    
    printf("FIFO RECV: started\n"); fflush(stdout);

    int res = queue_pop_timed(fifo->upQueue, (void **)&msg, &msgPtrSize, TIMEOUT_QUEUE_POP);
    if(res != 0){
        if (res == ETIMEDOUT){
            printf("FIFO RECV: timed out\n"); fflush(stdout);
            return res;
        }
        printf("FIFO RECV: failed with error %d\n", res); fflush(stdout);
        return res;
    }

    memcpy(message, msg->message, msg->messageSize);
    *messageSize = msg->messageSize;
    printf("FIFO RECV: received message of size %zu\n", *messageSize); fflush(stdout);
    fifo_message_destroy(msg);
    return 0;
}

int fifo_start(fifo* fifo){
    printf("FIFO START: starting fifo threads\n"); fflush(stdout);
    
    _threads_entry* e = _thr_get(fifo, 1);
    if (!e) {
        printf("FIFO START: failed to get threads entry\n"); fflush(stdout);
        return -1;
    }

    // Start receiver first
    thread_wrapper_args* recvArgs = thread_wrapper_args_init(_recv_wrapper, fifo, fifo);
    if (!recvArgs || pthread_create(&e->recv_tid, NULL, generic_thread_wrapper, recvArgs) != 0) {
        printf("FIFO START: failed to create receiver thread\n"); fflush(stdout);
        free(recvArgs);
        return -1;
    }
    e->recv_started = 1;
    printf("FIFO START: receiver thread started\n"); fflush(stdout);

    // Start sender
    thread_wrapper_args* sendArgs = thread_wrapper_args_init(_send_wrapper, fifo, fifo);
    if (!sendArgs || pthread_create(&e->send_tid, NULL, generic_thread_wrapper, sendArgs) != 0) {
        printf("FIFO START: failed to create sender thread\n"); fflush(stdout);
        free(sendArgs);
        // rollback receiver
        pthread_cancel(e->recv_tid);
        pthread_join(e->recv_tid, NULL);
        e->recv_started = 0;
        return -1;
    }
    e->send_started = 1;
    printf("FIFO START: sender thread started\n"); fflush(stdout);

    return 0;
}

int fifo_stop(fifo* fifo){
    printf("FIFO STOP: stopping fifo\n"); fflush(stdout);
    
    _threads_entry* e = _thr_get(fifo, 0);
    if (!e) {
        printf("FIFO STOP: failed to get threads entry\n"); fflush(stdout);
        return -1;
    }

    // reset graceful stop flag
    atomic_store_explicit(&fifo->shouldStop, 1, memory_order_relaxed);
    printf("FIFO STOP: stop flag set\n"); fflush(stdout);

    // Wake sender loop (if waiting on downQueue)
    if (e->send_started) {
        (void)queue_push(fifo->downQueue, FIFO_SHUTDOWN_SENTINEL, sizeof(fifo_message*));
        printf("FIFO STOP: shutdown sentinel sent to sender\n"); fflush(stdout);
    }

    // Join threads BEFORE removing entry
    if (e->send_started) { 
        printf("FIFO STOP: joining sender thread\n"); fflush(stdout);
        pthread_join(e->send_tid, NULL); 
        e->send_started = 0; 
    }
    if (e->recv_started) { 
        printf("FIFO STOP: joining receiver thread\n"); fflush(stdout);
        pthread_join(e->recv_tid, NULL); 
        e->recv_started = 0; 
    }

    _thr_remove(fifo);
    printf("FIFO STOP: fifo stopped successfully\n"); fflush(stdout);
    return 0;
}

fifo* fifo_init(pflx* pflx){
    fifo* fifo_socket = malloc(sizeof(fifo));
    fifo_socket->upQueue = queue_init();
    fifo_socket->downQueue = queue_init();
    
    fifo_socket->ownMessageID = 1;
    fifo_socket->next_id_tbd = malloc(sizeof(size_t) * pflx->phonebook_size);
    for(size_t i = 0; i < pflx->phonebook_size; i++){
        fifo_socket->next_id_tbd[i] = 1;
    }
    fifo_socket->id_tbd = malloc(sizeof(void *) * pflx->phonebook_size);
    for(size_t i = 0; i < pflx->phonebook_size; i++){
        fifo_socket->id_tbd[i] = bst_set_init();
    }
    atomic_init(&fifo_socket->shouldStop, 0);
    fifo_socket->pflx_layer = pflx;

    return fifo_socket;
}

int fifo_destroy(fifo* fifo_socket){
    int res = 0;
    res = queue_destroy(fifo_socket->downQueue);
    res = res + queue_destroy(fifo_socket->upQueue);
    res = res + pflx_destroy(fifo_socket->pflx_layer);

    free(fifo_socket->next_id_tbd);
    for(size_t i = 0; i< fifo_socket->pflx_layer->phonebook_size; i++){
        bst_set_destroy(fifo_socket->id_tbd[i]);
    }

    free(fifo_socket);
    return res;

}

fifo_message* fifo_message_init(void* message, size_t messageSize, size_t messageID, size_t originID, size_t numAcks){
    fifo_message* fifoMsg = malloc(sizeof(fifo_message));
    fifoMsg->message = message;
    fifoMsg->messageSize = messageSize;
    fifoMsg->acks = ba_init(numAcks);
    ba_add(fifoMsg->acks, originID-1); // from id to index
    fifoMsg->messageID = messageID;
    fifoMsg->originID = originID;

    return fifoMsg;
}

int fifo_message_destroy(fifo_message* fifoMsg){
    free(fifoMsg->message);
    ba_destroy(fifoMsg->acks);
    free(fifoMsg);

    return 0;
}