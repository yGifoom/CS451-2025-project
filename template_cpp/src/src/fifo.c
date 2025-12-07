#define _POSIX_C_SOURCE      199309L
#include"fifo.h"
#include"pflx.h"
#include"queue.h"
#include"ba.h"
#include"bst_set.h"
#include"threads_entry.h"
#include<stdlib.h>
#include<stdatomic.h>
#include<errno.h>
#include<string.h>
#include<stdio.h>
#include<time.h>


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

static fifo_message* fifo_unframe(fifo* fifo, void* frame, size_t frameSize){
    if (!frame || frameSize < 4 * sizeof(size_t)) {
        if (!frame){
            return NULL;
        } else{
            return NULL;
        }
    }

    const size_t hdr_words = 5;
    const size_t hdr_size = hdr_words * sizeof(size_t);
    
    // hdr[0]: originID hdr[1]: targetID hdr[2]: msgID hdr[3]: msg size hdr[4]: relayID
    size_t* hdr = (size_t*)frame;
    size_t originID = hdr[0];
    // hdr[1] is targetID (not needed for reconstruction)
    size_t messageID = hdr[2];
    size_t messageSize = hdr[3];
    size_t relayID = hdr[4];
    
    size_t num_blocks = (fifo->pflx_layer->phonebook_size + 63) / 64; // round up to 64-bit blocks
    size_t baSize = num_blocks * sizeof(uint64_t);

    if (frameSize < hdr_size + baSize + messageSize) {
        return NULL;
    }
    
    void* bitArrayData = (unsigned char*)frame + hdr_size;
    ba* acks = ba_construct(bitArrayData, fifo->pflx_layer->phonebook_size);
    if (!acks) {
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
        return NULL;
    }
    
    msg->message = messageData;
    msg->messageSize = messageSize;
    msg->messageID = messageID;
    msg->originID = originID;
    msg->acks = acks;
    msg->relayID = relayID;
    
    return msg;
}

int fifo_send_routine(fifo* fifo){
    unsigned char* frame = malloc(BUFFERSIZE);
    printf("%zu-FIFO SEND ROUTINE: started\n", fifo->pid); fflush(stdout);
    
    // Validate that our own BST set pointer is valid
    size_t own_idx = fifo->pid - 1;
    if (own_idx >= fifo->pflx_layer->phonebook_size) {
        printf("%zu-FIFO SEND ROUTINE: ERROR - own_idx %zu >= phonebook_size %zu\n", 
               fifo->pid, own_idx, fifo->pflx_layer->phonebook_size); fflush(stdout);
        free(frame);
        return -1;
    }
    
    bst_set* own_bst = fifo->id_tbd[own_idx];
    
    if (!own_bst) {
        printf("%zu-FIFO SEND ROUTINE: ERROR - own bst_set is NULL!\n", fifo->pid); fflush(stdout);
        free(frame);
        return -1;
    }

    while(1){
        
        if (atomic_load_explicit(&fifo->shouldStop, memory_order_acquire) == 1){
            printf("%zu-FIFO SEND ROUTINE: stop requested, exiting\n", fifo->pid); fflush(stdout);
            break;
        }
        if(pflx_network_status(fifo->pflx_layer) == 0){
            printf("%zu-FIFO SEND ROUTINE: pflx network not busy\n", fifo->pid); fflush(stdout);
            fifo->network_busy = 0;
        }

        struct timespec ts = { .tv_sec = 0, .tv_nsec = CONGESTION_CONTROL }; // 10ms = 10000000
        nanosleep(&ts, NULL);

        void* popped = NULL; size_t msgSize;
        int res = queue_pop_timed(fifo->downQueue, &popped, &msgSize, TIMEOUT_QUEUE_POP);
        if (res != 0){
            if (res == ETIMEDOUT){
                continue;
            }
            printf("%zu-FIFO SEND ROUTINE: queue_pop_timed failed with error %d\n", fifo->pid, res); fflush(stdout);
            free(frame);
            return res;
        }
        // Check for shutdown sentinel
        if (popped == FIFO_SHUTDOWN_SENTINEL) {
            printf("%zu-FIFO SEND ROUTINE: shutdown sentinel received, exiting\n", fifo->pid); fflush(stdout);
            break;
        }

        // NOW MANAGING MESSAGE 
        fifo_message* msgToSend = (fifo_message*)popped;
        size_t originIdx = msgToSend->originID - 1;
        printf("%zu-FIFO SEND ROUTINE: processing message ID %zu from origin %zu\n", fifo->pid,
               msgToSend->messageID, msgToSend->originID); fflush(stdout);

        // remove already delivered msgs
        pthread_mutex_lock(&fifo->tbd_mutexes[originIdx]);
        if(msgToSend->messageID < fifo->next_id_tbd[originIdx]){
            pthread_mutex_unlock(&fifo->tbd_mutexes[originIdx]);
            fifo_message_destroy(msgToSend);
            continue; 
        }
        pthread_mutex_unlock(&fifo->tbd_mutexes[originIdx]);

        // ANY UPDATES SINCE MESSAGE WAS IN QUEUE? MERGE IT WITH RECORD
        fifo_message* Msg = NULL; size_t remainingAcks = -1u;
        pthread_mutex_lock(&fifo->tbd_mutexes[originIdx]);
        int bst_lookup = bst_set_lookup(fifo->id_tbd[originIdx], msgToSend->messageID, (void**)&Msg, NULL);
        if (bst_lookup != 1) {
            // first time this message is known: add a copy as canonical
            fifo_message* canonical = fifo_message_copy(msgToSend);
            if (canonical == NULL){
                printf("%zu-FIFO SEND ROUTINE: Error in copying into canonical, putting back in queue\n", fifo->pid); fflush(stdout);
                queue_push(fifo->downQueue, msgToSend, msgSize);
                continue;
            }
            bst_set_add(fifo->id_tbd[originIdx], msgToSend->messageID, canonical, sizeof(fifo_message));
            pthread_mutex_unlock(&fifo->tbd_mutexes[originIdx]);
        } else {
            pthread_mutex_unlock(&fifo->tbd_mutexes[originIdx]);
            queue_push(fifo->downQueue, msgToSend, msgSize);
            continue;
            /* USELESS AS PFLX NETWORK IS LOSSLESS
            // broadcast only when pflx has responded with something
            // broadcasting when no new acks have been recieved is congests the network
            size_t nOfAcks = ba_sum(msgToSend->acks);
            if (ba_sum(Msg->acks) == nOfAcks){

                printf("%zu-FIFO BROADCAST: suspended broadcasting message ID %zu from origin %zu, no new acks\n", fifo->pid,
                msgToSend->messageID, msgToSend->originID); fflush(stdout);

                queue_push(fifo->downQueue, msgToSend, msgSize);
                continue;
            } else{
                // merge to msgToSend to keep congestion control mechanism updated
                ba_merge(Msg->acks, msgToSend->acks);
                ba_merge(msgToSend->acks, Msg->acks);
                }
            */
        }
        
        // BROADCAST
        printf("%zu-FIFO SEND ROUTINE: going to broadcast\n", fifo->pid); fflush(stdout);
        remainingAcks = fifo_broadcast_missing(fifo, msgToSend);
        printf("%zu-FIFO SEND ROUTINE: broadcast for msID %zu origin %zu returned %zu missing processes\n", fifo->pid, 
            msgToSend->messageID, msgToSend->originID, remainingAcks); fflush(stdout);

        queue_push(fifo->downQueue, msgToSend, msgSize);
        
    }
    printf("%zu-FIFO SEND ROUTINE: exiting\n", fifo->pid); fflush(stdout);
    return 0;
}

int fifo_recv_routine(fifo* fifo){
    // init
    void* popped = malloc(BUFFERSIZE);
    size_t msgSize;
    printf("%zu-FIFO RECV ROUTINE: started\n", fifo->pid); fflush(stdout);

    while(1){
        if (atomic_load_explicit(&fifo->shouldStop, memory_order_acquire) == 1){
            printf("%zu-FIFO RECV ROUTINE: stop requested, exiting\n", fifo->pid); fflush(stdout);
            break;
        }
        if(pflx_network_status(fifo->pflx_layer) == 0){
            printf("%zu-FIFO RECV ROUTINE: pflx network not busy\n", fifo->pid); fflush(stdout);
            fifo->network_busy = 0;
        }

        int res = pflx_recv(fifo->pflx_layer, popped, &msgSize);
        if (res != 0){
            if (res == ETIMEDOUT){
                printf("%zu-FIFO RECV ROUTINE: pflx_recv timed out\n", fifo->pid); fflush(stdout);
                for(size_t i = 0; i<fifo->pflx_layer->phonebook_size; i++){
                    printf("%zu-FIFO RECV ROUTINE: for process %zu next tbd %zu\n", fifo->pid, i+1, fifo->next_id_tbd[i]); fflush(stdout);
                }
                continue;
            }
            printf("%zu-FIFO RECV ROUTINE: pflx_recv failed with error %d\n", fifo->pid, res); fflush(stdout);
            free(popped);
            return res;
        }

        // RECONSTRUCT THE FRAME
        // hdr[0]: originID hdr[1]: targetID hdr[2]: msgID hdr[3]: msg size hdr[4]: relayID
        size_t* hdr = (size_t*)popped;
        printf("%zu-FIFO RECV ROUTINE: received frame of size %zu with originID=%zu, targetID=%zu, messageID=%zu, relayID=%zu\n", fifo->pid,
               hdr[3], hdr[0], hdr[1], hdr[2], hdr[4]); fflush(stdout);

        if(hdr[0] > fifo->pflx_layer->phonebook_size || hdr[1] > fifo->pflx_layer->phonebook_size){
            printf("%zu-FIFO RECV ROUTINE: got message from invalid originid %zu\n", fifo->pid, hdr[0]); fflush(stdout);
            continue;
        } 

        fifo_message *msgRecvd;
        msgRecvd = fifo_unframe(fifo, popped, msgSize);
        if (!msgRecvd) {
            printf("%zu-FIFO RECV ROUTINE: failed to unframe message\n", fifo->pid); fflush(stdout);
            continue;
        }
        printf("%zu-FIFO RECV ROUTINE: unframed message ID %zu from origin %zu\n", fifo->pid,
               msgRecvd->messageID, msgRecvd->originID); fflush(stdout);
        
        // Convert originID to array index
        size_t originIdx = hdr[0] - 1; 
        size_t targetIdx = hdr[1] - 1;
        size_t relayIdx = hdr[4] - 1;
        
        int stored_as_canonical = 0;
        fifo_message* canonical = NULL;

        // UPDATE ACKS
        pthread_mutex_lock(&fifo->tbd_mutexes[originIdx]);
        if(bst_set_lookup(fifo->id_tbd[originIdx], hdr[2], (void**)&canonical, NULL) == 1){
            // NOT first time I recieve this message
            printf("%zu-FIFO RECV ROUTINE: not first time recvieving msgID %zu of OriginID %zu\n", fifo->pid, 
            msgRecvd->messageID, msgRecvd->originID); fflush(stdout);
            
            // I am adding all that I can reasonably add so that no cornercases are left
            ba_add(msgRecvd->acks, targetIdx);
            ba_add(msgRecvd->acks, originIdx);
            ba_add(msgRecvd->acks, relayIdx);
            ba_add(msgRecvd->acks, fifo->pid - 1); // convertion pid -> index

            // Merge into canonical;
            if (canonical == NULL){
                // canonical is corrupted
                printf("%zu-FIFO RECV ROUTINE: old message for msgID %zu of OriginID %zu is corrupted, or no present, saving new\n", fifo->pid,
                    msgRecvd->messageID, msgRecvd->originID); fflush(stdout);

                ba_add(msgRecvd->acks, fifo->pid - 1); // convertion pid -> index
                bst_set_add(fifo->id_tbd[originIdx], hdr[2], (void*)msgRecvd, sizeof(fifo_message));
                stored_as_canonical = 1;
            } else{
                ba_merge(canonical->acks, msgRecvd->acks);
            }
        } else{
            // first time I recieve this message 
            // i.e. fifo->pid != msgRecvd->origin as before broadcasting I save into canonical
            // I am adding all that I can reasonably add so that no cornercases are left
            ba_add(msgRecvd->acks, fifo->pid - 1);
            ba_add(msgRecvd->acks, originIdx);
            ba_add(msgRecvd->acks, relayIdx);

            printf("%zu-FIFO RECV ROUTINE: first time receiving msgID %zu of OriginID %zu\n", fifo->pid, 
                msgRecvd->messageID, msgRecvd->originID); fflush(stdout);
            
            int result_add = bst_set_add(fifo->id_tbd[originIdx], hdr[2], (void*)msgRecvd, sizeof(fifo_message));
            if(result_add == 1){
                printf("%zu-FIFO RECV ROUTINE: key already exists for msgID %zu of OriginID %zu in id_tbd! PANIC\n", fifo->pid, 
                msgRecvd->messageID, msgRecvd->originID); fflush(stdout); 
                free(popped);
                return 0;
            } else if (result_add == -1){
                printf("%zu-FIFO RECV ROUTINE: error adding msgID %zu of OriginID %zu to id_tbd! PANIC\n", fifo->pid, 
                msgRecvd->messageID, msgRecvd->originID); fflush(stdout); 
                free(popped);
                return 0;
            }

            stored_as_canonical = 1;
            canonical = msgRecvd;

            // naive retransmit
            fifo_broadcast_missing(fifo, canonical);
        }
        pthread_mutex_unlock(&fifo->tbd_mutexes[originIdx]);

        // try to deliver message
        int fifoDeliverRes = fifo_deliver(fifo, canonical);
        if( fifoDeliverRes == 0){
            // was not delivered 
        } else if(fifoDeliverRes == -1){
            printf("%zu-FIFO RECV ROUTINE: deliver panicked for msgID %zu of OriginID %zu\n", fifo->pid, 
                msgRecvd->messageID, msgRecvd->originID); fflush(stdout);
            free(popped);
            return 1;
        }

        // free only the transient copy if not stored
        if (!stored_as_canonical) {
            fifo_message_destroy(msgRecvd);
        }
    }
    printf("%zu-FIFO RECV ROUTINE: exiting\n", fifo->pid); fflush(stdout);
    free(popped);
    return 0;
}

int fifo_deliver(fifo* fifo, fifo_message* msg){
    if(msg == NULL){
        printf("%zu-FIFO DELIVER: msg is null pointer: %p \n", fifo->pid, (void*)msg); fflush(stdout); 
        return -1;
    }

    size_t originIdx = msg->originID - 1;
    int succ = 1; int nOfDelivered = 0;
    size_t firstOriginID = msg->originID; 
    size_t firstMsgID = msg->messageID;

    pthread_mutex_lock(&fifo->tbd_mutexes[originIdx]);

    while (ba_sum(msg->acks) > fifo->pflx_layer->phonebook_size/2){
        if(fifo->next_id_tbd[originIdx] == msg->messageID){
            succ = 0;
        }else{
            break;
        }
        printf("%zu-FIFO DELIVER: delivering message ID %zu from origin %zu\n", fifo->pid,
               msg->messageID, msg->originID); fflush(stdout);

        // Push a heap copy of the payload to avoid aliasing BST-owned fifo_message
        // message is never going to be a pointer
        void* deliver_buf = malloc(msg->messageSize);
        if (!deliver_buf) {
            printf("%zu-FIFO DELIVER: failed to alloc deliver buffer\n", fifo->pid); fflush(stdout);
            pthread_mutex_unlock(&fifo->tbd_mutexes[originIdx]);
            return -1;
        }
        memcpy(deliver_buf, msg->message, msg->messageSize);
        int res = queue_push(fifo->upQueue, deliver_buf, msg->messageSize);
        if (res != 0) {
            printf("%zu-FIFO DELIVER: failed to push to upQueue, error %d\n", fifo->pid, res); fflush(stdout);
            free(deliver_buf);
            pthread_mutex_unlock(&fifo->tbd_mutexes[originIdx]);
            return -1;
        }
        // update next expected msgID
        fifo->next_id_tbd[originIdx]++;
        nOfDelivered++;

        // Lookup next
        if (bst_set_lookup(fifo->id_tbd[originIdx], fifo->next_id_tbd[originIdx], (void**)&msg, NULL) != 1) {
            break;
        }
    }
    printf("%zu-FIFO DELIVER: succ is %d for msgID: %zu originID: %zu next_tbd: %zu, #delivered: %d as MessageID: %zu was has %zu acks\n", fifo->pid, succ,
        firstMsgID, firstOriginID, fifo->next_id_tbd[originIdx], nOfDelivered, msg->messageID, ba_sum(msg->acks)); fflush(stdout);
    pthread_mutex_unlock(&fifo->tbd_mutexes[originIdx]);
    return succ;
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
    
    // hdr[0]: originID hdr[1]: targetID hdr[2]: msgID hdr[3]: msg size hdr[4]: relayID
    size_t* hdr = (size_t*)frame;
    hdr[0] = m->originID;
    hdr[1] = 0; // fill in later
    hdr[2] = m->messageID;
    hdr[3] = m->messageSize;
    hdr[4] = m->relayID;
    
    // copying bit array
    memcpy(frame + hdr_size, bitArray, baSize);

    // copying message payload
    memcpy(frame + hdr_size + baSize, m->message, m->messageSize);

    *frameSize = frameSizeLocal;
    free(bitArray);
    return frame;
}

size_t fifo_broadcast_missing(fifo* fifo, fifo_message* msgToBroadcast){
    printf("%zu-FIFO BROADCAST: broadcasting message ID %zu from origin %zu\n", fifo->pid,
           msgToBroadcast->messageID, msgToBroadcast->originID); fflush(stdout);

    size_t* missingProcesses; size_t sizeMissing;
    
    ba_where_0(msgToBroadcast->acks, &missingProcesses, &sizeMissing);
    
    if (sizeMissing <= 0){
        return 0;
    }
    
    size_t frameSize;
    unsigned char* fifoFrame = fifo_make_frame(msgToBroadcast, &frameSize);

    // hdr[0]: originID hdr[1]: targetID hdr[2]: msgID hdr[3]: msg size hdr[4]: relayID
    size_t* hdr = (size_t*)fifoFrame;

    if (fifoFrame == NULL){
        free(missingProcesses);
        return sizeMissing;
    }

    for (size_t i = 0; i < sizeMissing; i++){
        // MEMOPT: right now send memcopies, could save in one location and 
        // keep track of instances floating around

        // change targetID position: hdr[1]
        printf("%zu-FIFO BROADCAST: broadcasting msgID:%zu of originID:%zu relayID:%zu to targetID: %zu\n", fifo->pid, 
            hdr[2], hdr[0], fifo->pid, missingProcesses[i]+1); fflush(stdout);
        hdr[1] = missingProcesses[i]+1;
        hdr[4] = fifo->pid;
        
        pflx_send(fifo->pflx_layer, 
            fifoFrame, frameSize,
            fifo->pid, missingProcesses[i]+1); 
    }
    free(missingProcesses);
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
    
    printf("%zu-FIFO SEND: creating message ID %u from origin %zu, relayID: %zu\n", fifo->pid, msgID, originID, fifo->pid); fflush(stdout);
    
    // relay for send is always the origin
    fifo_message* msg = fifo_message_init(message,
        messageSize, msgID, originID, fifo->pflx_layer->phonebook_size, fifo->pid);
    if (msg == NULL) {
        return 1;
    }

    int res = queue_push(fifo->downQueue, msg, sizeof(fifo_message*)); // push pointer value
    if (res != 0){
        printf("%zu-FIFO SEND: failed to push to downQueue, error %d\n", fifo->pid, res); fflush(stdout);
        return res;
    }

    printf("%zu-FIFO SEND: message queued successfully\n", fifo->pid); fflush(stdout);
    return 0;
}

int fifo_recv(fifo* fifo, void* message, size_t* messageSize){
    printf("%zu-FIFO RECV: started\n", fifo->pid); fflush(stdout);

    void* payload = NULL; size_t payloadSize = 0;
    int res = queue_pop_timed(fifo->upQueue, &payload, &payloadSize, TIMEOUT_QUEUE_POP);
    if(res != 0){
        if (res == ETIMEDOUT){
            printf("%zu-FIFO RECV: timed out\n", fifo->pid); fflush(stdout);
            return res;
        }
        printf("%zu-FIFO RECV: failed with error %d\n", fifo->pid, res); fflush(stdout);
        return res;
    }

    memcpy(message, payload, payloadSize);
    *messageSize = payloadSize;
    free(payload);

    printf("%zu-FIFO RECV: received message of size %zu\n", fifo->pid, *messageSize); fflush(stdout);
    return 0;
}

int fifo_start(fifo* fifo){
    printf("%zu-FIFO START: starting fifo threads\n", fifo->pid); fflush(stdout);
    int res = pflx_start(fifo->pflx_layer);
    if (res != 0){
        printf("%zu-FIFO START: failed to start pflx\n", fifo->pid); fflush(stdout);
        return -1;
    } 
    _threads_entry* e = _thr_get(fifo, 1);
    if (!e) {
        printf("%zu-FIFO START: failed to get threads entry\n", fifo->pid); fflush(stdout);
        return -1;
    }

    // Start receiver first
    thread_wrapper_args* recvArgs = thread_wrapper_args_init(_recv_wrapper, fifo, fifo);
    if (!recvArgs || pthread_create(&e->recv_tid, NULL, generic_thread_wrapper, recvArgs) != 0) {
        printf("%zu-FIFO START: failed to create receiver thread\n", fifo->pid); fflush(stdout);
        free(recvArgs);
        return -1;
    }
    e->recv_started = 1;
    printf("%zu-FIFO START: receiver thread started\n", fifo->pid); fflush(stdout);

    // Start sender
    thread_wrapper_args* sendArgs = thread_wrapper_args_init(_send_wrapper, fifo, fifo);
    if (!sendArgs || pthread_create(&e->send_tid, NULL, generic_thread_wrapper, sendArgs) != 0) {
        printf("%zu-FIFO START: failed to create sender thread\n", fifo->pid); fflush(stdout);
        free(sendArgs);
        // rollback receiver
        pthread_cancel(e->recv_tid);
        pthread_join(e->recv_tid, NULL);
        e->recv_started = 0;
        return -1;
    }
    e->send_started = 1;
    printf("%zu-FIFO START: sender thread started\n", fifo->pid); fflush(stdout);

    return 0;
}

int fifo_stop(fifo* fifo){
    printf("%zu-FIFO STOP: stopping fifo\n", fifo->pid); fflush(stdout);
    
    // Set stop flag FIRST before stopping pflx
    atomic_store_explicit(&fifo->shouldStop, 1, memory_order_release);
    printf("%zu-FIFO STOP: stop flag set\n", fifo->pid); fflush(stdout);
    
    // Stop pflx which will wake recv_routine
    int res = pflx_stop(fifo->pflx_layer);
    if (res != 0) {
        printf("%zu-FIFO STOP: failed to stop pflx\n", fifo->pid); fflush(stdout);
        return -1;
    }

    _threads_entry* e = _thr_get(fifo, 0);
    if (!e) {
        printf("%zu-FIFO STOP: no threads entry found\n", fifo->pid); fflush(stdout);
        return 0; // Already stopped
    }

    // Wake sender loop (if waiting on downQueue)
    if (e->send_started) {
        (void)queue_push(fifo->downQueue, FIFO_SHUTDOWN_SENTINEL, sizeof(fifo_message*));
        printf("%zu-FIFO STOP: shutdown sentinel sent to sender\n", fifo->pid); fflush(stdout);
    }

    // Join threads BEFORE removing entry
    if (e->send_started) { 
        printf("%zu-FIFO STOP: joining sender thread\n", fifo->pid); fflush(stdout);
        pthread_join(e->send_tid, NULL); 
        e->send_started = 0; 
        printf("%zu-FIFO STOP: sender thread joined\n", fifo->pid); fflush(stdout);
    }
    if (e->recv_started) { 
        printf("%zu-FIFO STOP: joining receiver thread\n", fifo->pid); fflush(stdout);
        pthread_join(e->recv_tid, NULL); 
        e->recv_started = 0;
        printf("%zu-FIFO STOP: receiver thread joined\n", fifo->pid); fflush(stdout);
    }

    _thr_remove(fifo);
    printf("%zu-FIFO STOP: fifo stopped successfully\n", fifo->pid); fflush(stdout);
    return 0;
}

fifo* fifo_init(pflx* pflx, size_t pid){
    fifo* fifo_socket = malloc(sizeof(fifo));

    fifo_socket->pid = pid;
    fifo_socket->upQueue = queue_init();
    fifo_socket->downQueue = queue_init();
    fifo_socket->network_busy = 1;
    fifo_socket->ownMessageID = 1;
    fifo_socket->next_id_tbd = malloc(sizeof(size_t) * pflx->phonebook_size);
    fifo_socket->id_tbd = malloc(sizeof(bst_set*) * pflx->phonebook_size);
    fifo_socket->tbd_mutexes = malloc(sizeof(pthread_mutex_t) * pflx->phonebook_size);
    
    for(size_t i = 0; i < pflx->phonebook_size; i++){
        pthread_mutex_init(&fifo_socket->tbd_mutexes[i], NULL);
        fifo_socket->id_tbd[i] = bst_set_init();
        fifo_socket->next_id_tbd[i] = 1u;
    }
    
    // Set atomic and pflx_layer AFTER initializing id_tbd
    atomic_init(&fifo_socket->shouldStop, 0);
    fifo_socket->pflx_layer = pflx;

    return fifo_socket;
}

int fifo_destroy(fifo* fifo_socket){
    int res = 0;
    res = queue_destroy(fifo_socket->upQueue);
    res = res + queue_destroy(fifo_socket->downQueue);
    free(fifo_socket->next_id_tbd);

    for(size_t i = 0; i< fifo_socket->pflx_layer->phonebook_size; i++){
        bst_set_destroy(fifo_socket->id_tbd[i]);
        pthread_mutex_destroy(&fifo_socket->tbd_mutexes[i]);
    }
    free(fifo_socket->id_tbd);
    free(fifo_socket->tbd_mutexes);
    res = res + pflx_stop(fifo_socket->pflx_layer);
    res = res + pflx_destroy(fifo_socket->pflx_layer);
    free(fifo_socket);
    return res;

}

fifo_message* fifo_message_copy(fifo_message* msg){
    fifo_message* cpy_msg = malloc(sizeof(fifo_message));
    cpy_msg->message = malloc(msg->messageSize);
    if (cpy_msg->message == NULL){
        free(cpy_msg);
        return NULL;
    }
    memcpy(cpy_msg->message, msg->message, msg->messageSize);
    void* ba_buffer = ba_unsafe_copy(msg->acks);
    if(ba_buffer == NULL){
        free(cpy_msg);
        return NULL;
    }
    cpy_msg->acks = ba_construct(ba_buffer, msg->acks->length);
    if (cpy_msg->acks == NULL){
        free(cpy_msg);
        free(ba_buffer);
        return NULL;
    }

    cpy_msg->messageID = msg->messageID;
    cpy_msg->messageSize = msg->messageSize;
    cpy_msg->originID = msg->originID;
    cpy_msg->relayID = msg->relayID;

    free(ba_buffer);
    return cpy_msg;
}

fifo_message* fifo_message_init(void* message, 
    size_t messageSize, size_t messageID, size_t originID, size_t numAcks, size_t relayID){
    fifo_message* fifoMsg = malloc(sizeof(fifo_message));
    if (!fifoMsg) return NULL;

    // Deep-copy payload to avoid aliasing stack/temporary buffers
    void* buf = malloc(messageSize);
    if (!buf) {
        free(fifoMsg);
        return NULL;
    }
    memcpy(buf, message, messageSize);

    fifoMsg->message = buf;
    fifoMsg->messageSize = messageSize;
    fifoMsg->acks = ba_init(numAcks);
    ba_add(fifoMsg->acks, relayID-1); // from id to index
    if(originID != relayID){
        ba_add(fifoMsg->acks, originID-1);
    }
    fifoMsg->messageID = messageID;
    fifoMsg->originID = originID;
    fifoMsg->relayID = relayID;

    return fifoMsg;
}

int fifo_message_destroy(fifo_message* fifoMsg){
    free(fifoMsg->message);
    ba_destroy(fifoMsg->acks);
    free(fifoMsg);

    return 0;
}