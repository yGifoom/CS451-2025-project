#define _POSIX_C_SOURCE      199309L
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
#include<time.h>


static const int CONGESTION_CONTROL = 1000000;
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

    const size_t hdr_words = 4;
    const size_t hdr_size = hdr_words * sizeof(size_t);
    
    size_t* hdr = (size_t*)frame;
    size_t originID = hdr[0];
    // hdr[1] is targetID (not needed for reconstruction)
    size_t messageID = hdr[2];
    size_t messageSize = hdr[3];
    
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
    
    return msg;
}

int fifo_send_routine(fifo* fifo){
    unsigned char* frame = malloc(BUFFERSIZE);
    printf("%d-FIFO SEND ROUTINE: started\n", fifo->pid); fflush(stdout);

    while(1){
        if (atomic_load_explicit(&fifo->shouldStop, memory_order_acquire) == 1){
            printf("%d-FIFO SEND ROUTINE: stop requested, exiting\n", fifo->pid); fflush(stdout);
            break;
        }
        if(pflx_network_status(fifo->pflx_layer) == 0){
            printf("%d-FIFO SEND ROUTINE: pflx network not busy\n", fifo->pid); fflush(stdout);
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
            printf("%d-FIFO SEND ROUTINE: queue_pop_timed failed with error %d\n", fifo->pid, res); fflush(stdout);
            free(frame);
            return res;
        }
        // Check for shutdown sentinel
        if (popped == FIFO_SHUTDOWN_SENTINEL) {
            printf("%d-FIFO SEND ROUTINE: shutdown sentinel received, exiting\n", fifo->pid); fflush(stdout);
            break;
        }

        fifo_message* msgToSend = (fifo_message*)popped;
        size_t originIdx = msgToSend->originID - 1;
        printf("%d-FIFO SEND ROUTINE: processing message ID %zu from origin %zu\n", fifo->pid,
               msgToSend->messageID, msgToSend->originID); fflush(stdout);

        // remove already delivered msgs
        pthread_mutex_lock(&fifo->tbd_mutexes[originIdx]);
        if(msgToSend->messageID < fifo->next_id_tbd[originIdx]){
            pthread_mutex_unlock(&fifo->tbd_mutexes[originIdx]);
            continue;
        }
        pthread_mutex_unlock(&fifo->tbd_mutexes[originIdx]);

        size_t remainingAcks = fifo_broadcast_missing(fifo, msgToSend);
        printf("%d-FIFO SEND ROUTINE: broadcast returned %zu missing processes\n", fifo->pid, remainingAcks); fflush(stdout);

        // use the canonical message from BST (with merged ACKs) before delivering
        fifo_message* canonical = NULL;
        if (bst_set_lookup(fifo->id_tbd[originIdx], msgToSend->messageID, (void**)&canonical, NULL) == 1 && canonical) {
            msgToSend = canonical;
        }

        int succ = fifo_deliver(fifo, msgToSend);
        if (succ == -1){
            // panic
            return 1;
        } else if(succ == 1){
            queue_push(fifo->downQueue, msgToSend, msgSize);
        } 
    }
    printf("%d-FIFO SEND ROUTINE: exiting\n", fifo->pid); fflush(stdout);
    return 0;
}

int fifo_recv_routine(fifo* fifo){
    // init
    void* popped = malloc(BUFFERSIZE);
    size_t msgSize;
    printf("%d-FIFO RECV ROUTINE: started\n", fifo->pid); fflush(stdout);

    while(1){
        if (atomic_load_explicit(&fifo->shouldStop, memory_order_acquire) == 1){
            printf("%d-FIFO RECV ROUTINE: stop requested, exiting\n", fifo->pid); fflush(stdout);
            break;
        }
        if(pflx_network_status(fifo->pflx_layer) == 0){
            printf("%d-FIFO RECV ROUTINE: pflx network not busy\n", fifo->pid); fflush(stdout);
            fifo->network_busy = 0;
        }

        int res = pflx_recv(fifo->pflx_layer, popped, &msgSize);
        if (res != 0){
            if (res == ETIMEDOUT){
                printf("%d-FIFO RECV ROUTINE: pflx_recv timed out\n", fifo->pid); fflush(stdout);
                continue;
            }
            printf("%d-FIFO RECV ROUTINE: pflx_recv failed with error %d\n", fifo->pid, res); fflush(stdout);
            free(popped);
            return res;
        }

        size_t* hdr = (size_t*)popped;
        printf("%d-FIFO RECV ROUTINE: received frame of size %zu with originID=%zu, targetID=%zu, messageID=%zu\n", fifo->pid,
               msgSize, hdr[0], hdr[1], hdr[2]); fflush(stdout);
        if(hdr[0] > fifo->pflx_layer->phonebook_size || hdr[1] > fifo->pflx_layer->phonebook_size){
            printf("%d-FIFO RECV ROUTINE: got message from invalid originid %zu\n", fifo->pid, hdr[0]); fflush(stdout);
            continue;
        } 

        fifo_message* msgRecvd = NULL;
        // handle recv from pflx layer

        // this routine is the only place where bst might be modified
        // no need to sync if only 1 recv thread
        fifo_message* oldMsg = NULL;
        msgRecvd = fifo_unframe(fifo, popped, msgSize);
        if (!msgRecvd) {
            printf("%d-FIFO RECV ROUTINE: failed to unframe message\n", fifo->pid); fflush(stdout);
            continue;
        }
        printf("%d-FIFO RECV ROUTINE: unframed message ID %zu from origin %zu\n", fifo->pid,
               msgRecvd->messageID, msgRecvd->originID); fflush(stdout);

        // Add sender's ack (targetID from header converted to index)
        ba_add(msgRecvd->acks, hdr[1]-1); // convertion pid -> index
        
        // Convert originID to array index
        size_t originIdx = hdr[0] - 1;
        
        if(bst_set_lookup(fifo->id_tbd[originIdx], hdr[2], (void**)&oldMsg, NULL) == 1){
            // Merge into canonical; discard transient copy
            ba_merge(oldMsg->acks, msgRecvd->acks);
            fifo_message_destroy(msgRecvd);
            msgRecvd = oldMsg;
        } else{
            bst_set_add(fifo->id_tbd[originIdx], hdr[2], (void*)msgRecvd, sizeof(fifo_message));
        }
        // try to deliver message,
        if(fifo_deliver(fifo, msgRecvd)== -1){
            //panic
            return 1;
        }

        
    }
    printf("%d-FIFO RECV ROUTINE: exiting\n", fifo->pid); fflush(stdout);
    free(popped);
    return 0;
}

int fifo_deliver(fifo* fifo, fifo_message* msg){
    size_t originIdx = msg->originID - 1;
    int succ = 1;
    size_t firstOriginID = msg->originID; 
    size_t firstMsgID = msg->messageID;
    pthread_mutex_lock(&fifo->tbd_mutexes[originIdx]);
    while (ba_where_1(msg->acks, NULL, NULL) > fifo->pflx_layer->phonebook_size/2){
        if(fifo->next_id_tbd[originIdx] == msg->messageID){
            fifo->next_id_tbd[originIdx]++;
            succ = 0;
        }else{
            break;
        }
        printf("%d-FIFO DELIVER: delivering message ID %zu from origin %zu\n", fifo->pid,
               msg->messageID, msg->originID); fflush(stdout);

        // Push a heap copy of the payload to avoid aliasing BST-owned fifo_message
        void* deliver_buf = malloc(msg->messageSize);
        if (!deliver_buf) {
            printf("%d-FIFO DELIVER: failed to alloc deliver buffer\n", fifo->pid); fflush(stdout);
            pthread_mutex_unlock(&fifo->tbd_mutexes[originIdx]);
            return -1;
        }
        memcpy(deliver_buf, msg->message, msg->messageSize);
        int res = queue_push(fifo->upQueue, deliver_buf, msg->messageSize);
        if (res != 0) {
            printf("%d-FIFO DELIVER: failed to push to upQueue, error %d\n", fifo->pid, res); fflush(stdout);
            free(deliver_buf);
            pthread_mutex_unlock(&fifo->tbd_mutexes[originIdx]);
            return -1;
        }

        // Lookup next; do NOT destroy current until after loop if you remove from BST.
        if (bst_set_lookup(fifo->id_tbd[originIdx], fifo->next_id_tbd[originIdx], (void**)&msg, NULL) != 1) {
            break;
        }
    }
    printf("%d-FIFO DELIVER: succ is %d for msgID: %zu originID: %zu next_tbd: %zu\n", fifo->pid, succ,
        firstOriginID, firstMsgID, fifo->next_id_tbd[originIdx]); fflush(stdout);
    pthread_mutex_unlock(&fifo->tbd_mutexes[originIdx]);
    return succ;
}


//OPT: maybe better if pflx makes frame recursively
static unsigned char* fifo_make_frame(fifo_message* m, size_t* frameSize){
    // Build wire frame: [originID, messageID, targetID, payloadSize][payload...]
    const size_t hdr_words = 4;
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
    
    // copying bit array
    memcpy(frame + hdr_size, bitArray, baSize);

    // copying message payload
    memcpy(frame + hdr_size + baSize, m->message, m->messageSize);

    *frameSize = frameSizeLocal;
    free(bitArray);
    return frame;
}

size_t fifo_broadcast_missing(fifo* fifo, fifo_message* msgToBroadcast){
    printf("%d-FIFO BROADCAST: broadcasting message ID %zu from origin %zu\n", fifo->pid,
           msgToBroadcast->messageID, msgToBroadcast->originID); fflush(stdout);
    size_t originIdx = msgToBroadcast->originID - 1;
    fifo_message* Msg; size_t* missingProcesses; size_t sizeMissing;
    if (bst_set_lookup(fifo->id_tbd[originIdx], msgToBroadcast->messageID, (void**)&Msg, NULL) != 1) {
        // Not in BST yet: add as canonical
        bst_set_add(fifo->id_tbd[originIdx], msgToBroadcast->messageID, msgToBroadcast, sizeof(fifo_message));
        Msg = msgToBroadcast;
    } else {
        // Merge incoming acks into canonical bitmap only (one direction sufficient)
        ba_merge(Msg->acks, msgToBroadcast->acks);
        // Use canonical for all subsequent computations
        msgToBroadcast = Msg;
    }
    // Compute missing based on canonical bitmap
    ba_where_0(msgToBroadcast->acks, &missingProcesses, &sizeMissing);
    
    if (sizeMissing <= 0){
        return 0;
    }
    
    size_t frameSize;
    unsigned char* fifoFrame = fifo_make_frame(msgToBroadcast, &frameSize);
    size_t* hdr = (size_t*)fifoFrame;

    if (fifoFrame == NULL){
        return sizeMissing;
    }
    printf("%d-FIFO BROADCAST: made frame with size %zu: originID:%zu, messageID:%zu, message size: %zu\n", fifo->pid, 
        frameSize, hdr[0], hdr[2], hdr[3]); fflush(stdout);

    for (size_t i = 0; i < sizeMissing; i++){
        // MEMOPT: right now send memcopies, could save in one location and 
        // keep track of instances floating around

        // change targetID position: hdr[1]
        hdr[1] = missingProcesses[i]+1;
        
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
    
    printf("%d-FIFO SEND: creating message ID %u from origin %zu\n", fifo->pid, msgID, originID); fflush(stdout);
    
    fifo_message* msg = fifo_message_init(message, messageSize, msgID, originID, fifo->pflx_layer->phonebook_size);
    if (msg == NULL) {
        return 1;
    }

    int res = queue_push(fifo->downQueue, msg, sizeof(fifo_message*)); // push pointer value
    if (res != 0){
        printf("%d-FIFO SEND: failed to push to downQueue, error %d\n", fifo->pid, res); fflush(stdout);
        return res;
    }

    printf("%d-FIFO SEND: message queued successfully\n", fifo->pid); fflush(stdout);
    return 0;
}

int fifo_recv(fifo* fifo, void* message, size_t* messageSize){
    printf("%d-FIFO RECV: started\n", fifo->pid); fflush(stdout);

    void* payload = NULL; size_t payloadSize = 0;
    int res = queue_pop_timed(fifo->upQueue, &payload, &payloadSize, TIMEOUT_QUEUE_POP);
    if(res != 0){
        if (res == ETIMEDOUT){
            printf("%d-FIFO RECV: timed out\n", fifo->pid); fflush(stdout);
            return res;
        }
        printf("%d-FIFO RECV: failed with error %d\n", fifo->pid, res); fflush(stdout);
        return res;
    }

    memcpy(message, payload, payloadSize);
    *messageSize = payloadSize;
    free(payload);

    printf("%d-FIFO RECV: received message '%s' of size %zu\n", fifo->pid, (char*)message, *messageSize); fflush(stdout);
    return 0;
}

int fifo_start(fifo* fifo){
    printf("%d-FIFO START: starting fifo threads\n", fifo->pid); fflush(stdout);
    int res = pflx_start(fifo->pflx_layer);
    if (res != 0){
        printf("%d-FIFO START: failed to start pflx\n", fifo->pid); fflush(stdout);
        return -1;
    } 
    _threads_entry* e = _thr_get(fifo, 1);
    if (!e) {
        printf("%d-FIFO START: failed to get threads entry\n", fifo->pid); fflush(stdout);
        return -1;
    }

    // Start receiver first
    thread_wrapper_args* recvArgs = thread_wrapper_args_init(_recv_wrapper, fifo, fifo);
    if (!recvArgs || pthread_create(&e->recv_tid, NULL, generic_thread_wrapper, recvArgs) != 0) {
        printf("%d-FIFO START: failed to create receiver thread\n", fifo->pid); fflush(stdout);
        free(recvArgs);
        return -1;
    }
    e->recv_started = 1;
    printf("%d-FIFO START: receiver thread started\n", fifo->pid); fflush(stdout);

    // Start sender
    thread_wrapper_args* sendArgs = thread_wrapper_args_init(_send_wrapper, fifo, fifo);
    if (!sendArgs || pthread_create(&e->send_tid, NULL, generic_thread_wrapper, sendArgs) != 0) {
        printf("%d-FIFO START: failed to create sender thread\n", fifo->pid); fflush(stdout);
        free(sendArgs);
        // rollback receiver
        pthread_cancel(e->recv_tid);
        pthread_join(e->recv_tid, NULL);
        e->recv_started = 0;
        return -1;
    }
    e->send_started = 1;
    printf("%d-FIFO START: sender thread started\n", fifo->pid); fflush(stdout);

    return 0;
}

int fifo_stop(fifo* fifo){
    printf("%d-FIFO STOP: stopping fifo\n", fifo->pid); fflush(stdout);
    int res = pflx_stop(fifo->pflx_layer);
    if (res != 0) {
        printf("%d-FIFO STOP: failed to stop pflx\n", fifo->pid); fflush(stdout);
        return -1;
    }

    _threads_entry* e = _thr_get(fifo, 0);
    if (!e) {
        printf("%d-FIFO STOP: failed to get threads entry\n", fifo->pid); fflush(stdout);
        return -1;
    }

    // reset graceful stop flag
    atomic_store_explicit(&fifo->shouldStop, 1, memory_order_release);
    printf("%d-FIFO STOP: stop flag set\n", fifo->pid); fflush(stdout);

    // Wake sender loop (if waiting on downQueue)
    if (e->send_started) {
        (void)queue_push(fifo->downQueue, FIFO_SHUTDOWN_SENTINEL, sizeof(fifo_message*));
        printf("%d-FIFO STOP: shutdown sentinel sent to sender\n", fifo->pid); fflush(stdout);
    }

    // Join threads BEFORE removing entry
    if (e->send_started) { 
        printf("%d-FIFO STOP: joining sender thread\n", fifo->pid); fflush(stdout);
        pthread_join(e->send_tid, NULL); 
        e->send_started = 0; 
    }
    if (e->recv_started) { 
        printf("%d-FIFO STOP: joining receiver thread\n", fifo->pid); fflush(stdout);
        pthread_join(e->recv_tid, NULL); 
        e->recv_started = 0; 
    }

    _thr_remove(fifo);
    printf("%d-FIFO STOP: fifo stopped successfully\n", fifo->pid); fflush(stdout);
    return 0;
}

fifo* fifo_init(pflx* pflx){
    fifo* fifo_socket = malloc(sizeof(fifo));

    fifo_socket->pid = pflx->udpSocket->sockfd;
    fifo_socket->upQueue = queue_init();
    fifo_socket->downQueue = queue_init();
    fifo_socket->network_busy = 1;
    fifo_socket->ownMessageID = 1;
    fifo_socket->next_id_tbd = malloc(sizeof(unsigned int) * pflx->phonebook_size);
    fifo_socket->id_tbd = malloc(sizeof(void *) * pflx->phonebook_size);
    fifo_socket->tbd_mutexes = malloc(sizeof(pthread_mutex_t) * pflx->phonebook_size);

    for(size_t i = 0; i < pflx->phonebook_size; i++){
        pthread_mutex_init(&fifo_socket->tbd_mutexes[i], NULL);
        fifo_socket->id_tbd[i] = bst_set_init();
        fifo_socket->next_id_tbd[i] = 1;
    }
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

fifo_message* fifo_message_init(void* message, size_t messageSize, size_t messageID, size_t originID, size_t numAcks){
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