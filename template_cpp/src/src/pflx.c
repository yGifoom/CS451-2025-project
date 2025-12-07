#define _POSIX_C_SOURCE      199309L
#include"pflx.h"
#include"threads_entry.h"
#include"udp.h"
#include"logger.h"
#include<string.h>
#include<stdlib.h>
#include<stdio.h>
#include <errno.h>
#include<time.h>

const int CONGESTION_CONTROL = 0;
const int BUFFERSIZE = 256;
const long TIMEOUT_QUEUE_POP = 1000; // in ms
const long MAX_DOWNQUEUE_SIZE = 10000000;
// Sentinel used to wake and stop the sender loop
#define PFLX_SHUTDOWN_SENTINEL ((void*)-1)

static void* _send_wrapper(void* arg) {
    pflx* p = (pflx*)arg;
    int result = _pflx_send_routine(p);
    return (void*)(intptr_t)result;
}

static void* _recv_wrapper(void* arg) {
    pflx* p = (pflx*)arg;
    int result = _pflx_recv_routine(p);
    return (void*)(intptr_t)result;
}

int pflx_start(pflx* pflx){
    _threads_entry* e = _thr_get(pflx, 1);
    if (!e) return -1;

    // reset graceful stop flag
    pthread_mutex_lock(&pflx->should_stop_mutex);
    pflx->should_stop = 0;
    pthread_mutex_unlock(&pflx->should_stop_mutex);

    //reset network busy
    pthread_mutex_lock(&pflx->network_busy_mutex);
    pflx->network_busy = 1;
    pthread_mutex_unlock(&pflx->network_busy_mutex);

    // Start receiver first
    thread_wrapper_args* recvArgs = thread_wrapper_args_init(_recv_wrapper, pflx, pflx);
    if (!recvArgs || pthread_create(&e->recv_tid, NULL, generic_thread_wrapper, recvArgs) != 0) {
        free(recvArgs);
        return -1;
    }
    e->recv_started = 1;

    // Start sender
    thread_wrapper_args* sendArgs = thread_wrapper_args_init(_send_wrapper, pflx, pflx);
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

int pflx_stop(pflx* pflx){
    _threads_entry* e = _thr_get(pflx, 0);
    if (!e) {
        printf("%d-PFLX STOP: no entry found (already stopped or never started)\n", 
               pflx->udpSocket->sockfd); 
        fflush(stdout);
        return 0; // Not an error - already stopped
    }

    // Request graceful stop FIRST
    pflx_request_stop(pflx);

    // Wake sender loop (if waiting on downQueue) BEFORE joining
    if (e->send_started) {
        (void)queue_push(pflx->downQueue, PFLX_SHUTDOWN_SENTINEL, sizeof(pflx_message*));
    }

    // Join threads BEFORE removing entry
    if (e->send_started) { 
        printf("%d-PFLX STOP: joining sender thread\n", pflx->udpSocket->sockfd); fflush(stdout);
        pthread_join(e->send_tid, NULL); 
        e->send_started = 0;
        printf("%d-PFLX STOP: sender joined\n", pflx->udpSocket->sockfd); fflush(stdout);
    }
    if (e->recv_started) { 
        printf("%d-PFLX STOP: joining receiver thread\n", pflx->udpSocket->sockfd); fflush(stdout);
        pthread_join(e->recv_tid, NULL); 
        e->recv_started = 0;
        printf("%d-PFLX STOP: receiver joined\n", pflx->udpSocket->sockfd); fflush(stdout);
    }

    _thr_remove(pflx);
    printf("%d-PFLX STOP: entry removed\n", pflx->udpSocket->sockfd); fflush(stdout);
    return 0;
}

int pflx_send(pflx* pflx, void* message, size_t messageSize, size_t originID, size_t targetID){
    pthread_mutex_lock(&pflx->ownMessageID_mutex);
    // the same message could be copied and sent N-1 times, we want 
    size_t msgID = (size_t)pflx->ownMessageID[targetID - 1];
    size_t ack_msg_id = 0;
    // acks should follow: msgID of ack msg == msgID msg acks for
    if (sscanf((char*)message, "ack %zu", &ack_msg_id) == 1) {
        msgID = ack_msg_id;
    }
    
    pflx_message* msg = pflx_message_init(message, messageSize, originID, msgID, targetID);
    if (msg == NULL) {
        pthread_mutex_unlock(&pflx->ownMessageID_mutex);
        return 1;
    }

    int res = queue_push(pflx->downQueue, msg, sizeof(pflx_message*)); // push pointer value
    if (res != 0){
        pthread_mutex_unlock(&pflx->ownMessageID_mutex);
        return res;
    }

    // msg sent was not an ack
    if(! (ack_msg_id > 0)){
        pflx->ownMessageID[targetID - 1]++;
    }
    pthread_mutex_unlock(&pflx->ownMessageID_mutex);

    // Print the real payload size, not sizeof(pointer)
    printf("%d-PFLX SEND: I am sending message of id '%zu' to process: %zu\n", pflx->udpSocket->sockfd, msg->messageID, msg->targetID); fflush(stdout);
    return 0;
}

// main ruotine for sending 
int _pflx_send_routine(pflx* pflx){
    // init
    unsigned char* frame = malloc(BUFFERSIZE);
    size_t dataSize;

    while (1){
        // Check stop flag at loop start
        if (pflx_should_stop(pflx)) {
            printf("%d-PFLX SEND ROUTINE: stop requested, exiting\n", pflx->udpSocket->sockfd); fflush(stdout);
            break;
        }

        //printf("%d-PFLX SEND ROUTINE: sending new message\n", pflx->udpSocket->sockfd); fflush(stdout);
        // pop a pflx_message* (not reusing 'frame' as holder)
        void* popped = NULL;
        int res = queue_pop_timed(pflx->downQueue, &popped, &dataSize, TIMEOUT_QUEUE_POP);
        if (res != 0){
            if (res == ETIMEDOUT){
                continue;
            }
            free(frame);
            return res;
        }

        // Check for shutdown sentinel
        if (popped == PFLX_SHUTDOWN_SENTINEL) {
            printf("%d-PFLX SEND ROUTINE: shutdown sentinel received, exiting\n", pflx->udpSocket->sockfd); fflush(stdout);
            break;
        }
        /////////////////////////////////////////////////////////////////// main logic
        // CHECK IF MESSAGE IS ACK OR MSG
        pflx_message* msg_to_send = (pflx_message*)popped;

        size_t ack_msg_id;
        int lenMessageSent;
        size_t originIdx = msg_to_send->originID - 1;
        size_t targetIdx = msg_to_send->targetID - 1;

        // Build wire frame: [originID, messageID, targetID, payloadSize][payload...]
        const size_t hdr_words = 4;
        const size_t hdr_size = hdr_words * sizeof(size_t);
        if (hdr_size + msg_to_send->messageSize > (size_t)BUFFERSIZE) {
            // Too large to fit, drop or truncate; here we drop quietly.
            continue;
        }
        size_t* hdr = (size_t*)frame;
        hdr[0] = msg_to_send->originID;
        hdr[1] = msg_to_send->messageID;
        hdr[2] = msg_to_send->targetID;
        hdr[3] = msg_to_send->messageSize;
        memcpy(frame + hdr_size, msg_to_send->message, msg_to_send->messageSize);
        
        size_t frame_size = hdr_size + msg_to_send->messageSize;

        // check if message was ack
        if (msg_to_send->message && 
            sscanf((char*)msg_to_send->message, "ack %zu", &ack_msg_id) == 1) {
            // strictly reciever behaviour (sending an ACK back)
            lenMessageSent = udp_send(pflx->udpSocket, 
                                    pflx->phonebook[targetIdx].ip_readable, 
                                    ntohs(pflx->phonebook[targetIdx].port), 
                                    frame, frame_size);
            if (lenMessageSent < 0){
                // send failed, we might want to handle it
            }
        } else {
            // strictly sender behaviour
            // sleep for a little bit as to not overwhelm the network
            struct timespec ts = { .tv_sec = 0, .tv_nsec = CONGESTION_CONTROL }; // 10ms = 10000000
            nanosleep(&ts, NULL);

            // (msg_to_send->messageID * targetID) + targetIdx
            // the msgs are indexed per destination:
            // ack recv uses originID, msg send uses targetID
            size_t sentMsgBstKey = 
            (msg_to_send->messageID-1) * (pflx->phonebook_size - 1) + msg_to_send->targetID 
            - (msg_to_send->targetID > msg_to_send->originID); // never recieve acks from self
            printf("%d-PFLX SEND ROUTINE: got sentMsgBstKey: %zu from msg_to_sendID: %zu, targetID: %zu\n",
                   pflx->udpSocket->sockfd, sentMsgBstKey, msg_to_send->messageID, msg_to_send->targetID); fflush(stdout);
            

            int ok = bst_set_lookup(pflx->id_delivered[originIdx], sentMsgBstKey, NULL, NULL);
            if(ok || ack_read(&pflx->next_id_tbd[originIdx]) > sentMsgBstKey){
                // already acked -> deliver
                pflx_message* msgToUp = (pflx_message*)popped;
                printf("%d-PFLX SEND ROUTINE: pushing into upQueue \n", pflx->udpSocket->sockfd); fflush(stdout);
                queue_push(pflx->upQueue, popped, dataSize);
            } else if (frame != NULL){
                lenMessageSent = udp_send(pflx->udpSocket, 
                                    pflx->phonebook[targetIdx].ip_readable, 
                                    ntohs(pflx->phonebook[targetIdx].port), 
                                    frame, frame_size);
                if (lenMessageSent == 0){
                    printf("%d-PFLX SEND ROUTINE: failed to sending msg of ID %zu to target %zu\n", pflx->udpSocket->sockfd, 
                    msg_to_send->messageID, msg_to_send->targetID); fflush(stdout);
                }else{
                    printf("%d-PFLX SEND ROUTINE: sending msg of ID %zu to target %zu\n", pflx->udpSocket->sockfd, 
                    msg_to_send->messageID, msg_to_send->targetID); fflush(stdout);
                }
                // put message back in, we will be waiting for ack
                res = queue_push(pflx->downQueue, msg_to_send, sizeof(pflx_message*));
                if (res != 0){
                    return res;
                }

                if (lenMessageSent < 0){
                    // send failed, or queue push failed
                }
            }else{
                printf("%d-PFLX SEND ROUTINE: could not send message in queue, continuing\n", pflx->udpSocket->sockfd); fflush(stdout);
            }
        }
    }

    free(frame);
    printf("%d-PFLX SEND ROUTINE: exited cleanly\n", pflx->udpSocket->sockfd); fflush(stdout);
    return 0;
}

// returns what is deliverable with perfect links properties
int pflx_recv(pflx* pflx, void* message, size_t* messageSize){
    pflx_message* msg = NULL;
    size_t msgPtrSize = sizeof(pflx_message*);
    printf("%d-PFLX RECV: started\n", pflx->udpSocket->sockfd); fflush(stdout);
    int res = queue_pop_timed(pflx->upQueue, (void **)&msg, &msgPtrSize, TIMEOUT_QUEUE_POP);
    if(res != 0){
        if (res == ETIMEDOUT){
            printf("PFLX RECV: timed out\n"); fflush(stdout);
            return res;
        }
        printf("PFLX RECV: failed gracefully\n"); fflush(stdout);
        return res;
    }

    memcpy(message, msg->message, msg->messageSize);
    *messageSize = msg->messageSize;
    printf("%d-I am recieving a message of len '%zu'\n", pflx->udpSocket->sockfd, *messageSize); fflush(stdout);
    pflx_message_destroy(msg);
    return 0;
}

int _pflx_recv_routine(pflx* pflx){
    // init
    unsigned char* buffer = malloc(BUFFERSIZE);
    int network_how_busy = 5;

    while(1){
        // Exit promptly if stop requested
        if (pflx_should_stop(pflx)) {
            printf("%d-PFLX RECV ROUTINE: stop requested, exiting\n", pflx->udpSocket->sockfd); fflush(stdout);
            break;
        }

        ssize_t len = udp_recv_timeout(pflx->udpSocket, buffer, BUFFERSIZE, 1000); // 1 second timeout
        printf("%d-PFLX RECV ROUTINE: just udp recvd wh result %ld\n", pflx->udpSocket->sockfd, len); fflush(stdout);
        
        if (len == 0) {
            // Timeout occurred
            network_how_busy--; 
            if (network_how_busy < 0){pflx_network_change_status(pflx);}
            printf("%d-PFLX RECV ROUTINE: NETWORK NOT BUSY: %d\n", pflx->udpSocket->sockfd, pflx_network_status(pflx)); fflush(stdout);
            continue;
        }
        network_how_busy = 5;
        
        if (len < 0) {
            // Error occurred
            free(buffer);
            return 1;
        }

        ////////////////////////////////////////////////////////// main logic
        // Parse wire frame: [originID, messageID, targetID, payloadSize][payload...]
        const size_t hdr_words = 4;
        const size_t hdr_size = hdr_words * sizeof(size_t);
        if ((size_t)len < hdr_size) {
            // malformed, ignore
            printf("%d-PFLX RECV ROUTINE: here's the malformed buffer\n", pflx->udpSocket->sockfd); fflush(stdout);

            continue;
        }

        size_t* hdr = (size_t*)buffer;
        size_t originID = hdr[0];
        size_t messageID = hdr[1];
        size_t targetID = hdr[2];
        size_t payloadSize = hdr[3];
        if (hdr_size + payloadSize != (size_t)len) {
            // malformed, ignore
            printf("%d-PFLX RECV ROUTINE: malformed message\n", pflx->udpSocket->sockfd); fflush(stdout);

            continue;
        }
        printf("%d-PFLX RECV ROUTINE: recreating message from frame\n", pflx->udpSocket->sockfd); fflush(stdout);

        // Reconstruct a local pflx_message with a copied payload (safe pointer)
        pflx_message* msg_recvd = pflx_message_init(buffer + hdr_size, payloadSize, originID, messageID, targetID);
        if(msg_recvd == NULL) {
            return 1;
        }
        
        size_t ack_msg_id = 0;
        size_t origin_index = msg_recvd->originID - 1;
        size_t target_index = msg_recvd->targetID - 1;
        size_t senderId, msgId;

        size_t msgOriginID =  msg_recvd->originID;
        size_t msgTargetID = msg_recvd->targetID;
        size_t msgMessageID = msg_recvd->messageID;

        printf("%d-PFLX RECV ROUTINE: entering main logic\n", pflx->udpSocket->sockfd); fflush(stdout);
        // Check if it's an ACK
        if (msg_recvd->message && sscanf((char*)msg_recvd->message, "ack %zu", &ack_msg_id) == 1) {
            // sender logic

            // BOUNDS CHECK (should be caught earlier, but defensive)
            if (target_index >= pflx->phonebook_size) {
                printf("%d-PFLX RECV ROUTINE: dropping out of bounds ack (target_index=%zu)\n",
                       pflx->udpSocket->sockfd, target_index); fflush(stdout);
                pflx_message_destroy(msg_recvd);
                continue;
            }
            
            // SAFETY: Verify BST exists
            if (!pflx->id_delivered[target_index]) {
                printf("%d-PFLX RECV ROUTINE: ERROR - BST at target_index %zu is NULL!\n",
                       pflx->udpSocket->sockfd, target_index);
                pflx_message_destroy(msg_recvd);
                continue;
            }

            if (target_index >= pflx->phonebook_size) {
                // out-of-range ACK; drop
                printf("%d-PFLX RECV ROUTINE: dropping out of bounds ack\n", pflx->udpSocket->sockfd); fflush(stdout);
                
                pflx_message_destroy(msg_recvd);
                continue;
            }
            printf("%d-PFLX RECV ROUTINE: recieved an ack from %zu for message ID %zu\n",
                   pflx->udpSocket->sockfd, msg_recvd->originID, ack_msg_id); fflush(stdout);
            
            // map the ack'd message in an interwoven manner to 
            // avoid aliasing between different broadcast messages 
            size_t ackBstKey = 
            (ack_msg_id-1) * (pflx->phonebook_size - 1) + msg_recvd->originID 
            - (msg_recvd->originID > msg_recvd->targetID);
            printf("%d-PFLX RECV ROUTINE: got ackBstKey: %zu from ackMsgId: %zu, originID: %zu\n",
                   pflx->udpSocket->sockfd, ackBstKey, ack_msg_id, msg_recvd->originID); fflush(stdout);
            // E.G.
            // originID 1 msgID 2 -> 2
            // originID 2 msgID 1 -> 3

            pthread_mutex_lock(&pflx->next_id_tbd[target_index].mutex);

            if (pflx->next_id_tbd[target_index].value == ackBstKey){
                printf("%d-PFLX RECV ROUTINE: ACK matches expected consequent, updating\n",
                       pflx->udpSocket->sockfd); fflush(stdout);

                size_t removed = bst_set_compact_consequent(
                    pflx->id_delivered[target_index], 
                    ackBstKey, NULL);

                pflx->next_id_tbd[target_index].value += removed + 1;
            } else if (pflx->next_id_tbd[target_index].value < ackBstKey) {
                printf("%d-PFLX RECV ROUTINE: ACK is non-consequent, adding to set\n",
                       pflx->udpSocket->sockfd); fflush(stdout);
                
                    // BOUNDS CHECK for origin_index
                if (target_index >= pflx->phonebook_size) {
                    printf("%d-PFLX RECV ROUTINE: target_index %zu out of bounds!\n",
                        pflx->udpSocket->sockfd, target_index);
                    pflx_message_destroy(msg_recvd);
                    continue;
                }
                
                // SAFETY: Verify BST exists
                if (!pflx->id_delivered[target_index]) {
                    printf("%d-PFLX RECV ROUTINE: ERROR - BST at target_index %zu is NULL!\n",
                        pflx->udpSocket->sockfd, target_index);
                    pflx_message_destroy(msg_recvd);
                    continue;
                }

                int res = bst_set_add(pflx->id_delivered[target_index], ackBstKey, NULL, 0);
                if(res == -1){
                    pflx_message_destroy(msg_recvd);
                    pthread_mutex_unlock(&pflx->next_id_tbd[target_index].mutex);
                    return res;
                }
            }
            pthread_mutex_unlock(&pflx->next_id_tbd[target_index].mutex);
            // ACKs are protocol-internal: never deliver to app
            pflx_message_destroy(msg_recvd);
            continue;

        // Any other message
        } else if (msg_recvd->message){
            printf("%d-PFLX RECV ROUTINE: recvd message from %zu\n", pflx->udpSocket->sockfd, msg_recvd->originID); fflush(stdout);
            // strictly reciever behaviour
            
            // drop if too many packets
            if (queue_size(pflx->downQueue) > MAX_DOWNQUEUE_SIZE){
                printf("%d-PFLX RECV ROUTINE: dropping packet, recieving too many\n", pflx->udpSocket->sockfd); fflush(stdout);
                pflx_message_destroy(msg_recvd);
                continue;
            }
            printf("%d-PFLX RECV ROUTINE: downqueuesize = %zu\n", pflx->udpSocket->sockfd,queue_size(pflx->downQueue)); fflush(stdout);
            printf("%d-PFLX RECV ROUTINE: upqueuesize = %zu\n", pflx->udpSocket->sockfd, queue_size(pflx->upQueue)); fflush(stdout);

            // TRY TO DELIVER
            int delivered = 0; // track if we hand the pointer up
            size_t expectedConsequentId;
            pthread_mutex_lock(&pflx->next_id_tbd[origin_index].mutex);

            expectedConsequentId = pflx->next_id_tbd[origin_index].value;
            if(expectedConsequentId > msgMessageID){
                // expectedConsequentId > msgMessageID - old message, already delivered
                printf("%d-PFLX RECV ROUTINE: recvd OLD msg (ID %zu < expected %zu), not delivering\n", 
                       pflx->udpSocket->sockfd, msgMessageID, expectedConsequentId); fflush(stdout);
            }else if(expectedConsequentId == msgMessageID){
                printf("%d-PFLX RECV ROUTINE: recvd an expected continuous msg\n", pflx->udpSocket->sockfd); fflush(stdout);
                
                size_t removed = bst_set_compact_consequent(
                    pflx->id_delivered[origin_index],
                    msgMessageID, NULL);

                pflx->next_id_tbd[origin_index].value += removed + 1;

                // deliver message (push capsule)
                queue_push(pflx->upQueue, msg_recvd, sizeof(pflx_message*));
                delivered = 1;
            }else if(expectedConsequentId < msgMessageID){
                // non consequent, let's save it
                int res = bst_set_add(pflx->id_delivered[origin_index], msgMessageID, NULL, 0);
                if (res == -1){
                    pthread_mutex_unlock(&pflx->next_id_tbd[origin_index].mutex);
                    return res;
                }
                // Only deliver if it's truly new (res == 0 means newly added)
                if (res == 0){
                    printf("%d-PFLX RECV ROUTINE: recvd an expected NON-continuous NEW msg\n", pflx->udpSocket->sockfd); fflush(stdout);
                    queue_push(pflx->upQueue, msg_recvd, sizeof(pflx_message*));
                    delivered = 1;
                } else {
                    printf("%d-PFLX RECV ROUTINE: recvd DUPLICATE non-continuous msg, not delivering\n", pflx->udpSocket->sockfd); fflush(stdout);
                }
            }

            pthread_mutex_unlock(&pflx->next_id_tbd[origin_index].mutex);

            // sending ack back regardless
            char ackbuf[64];
            int n = snprintf(ackbuf, sizeof(ackbuf), "ack %zu", msgMessageID);
            if (n > 0 && (size_t)n < sizeof(ackbuf)) {
                int sres = pflx_send(pflx, ackbuf, (size_t)n + 1, msgTargetID, msgOriginID);
                if (sres != 0){
                    return 1;
                }
            }

            // only free if we did not deliver to the app queue
            if (!delivered) {
                pflx_message_destroy(msg_recvd);
            }
            continue;
        }
    }
    free(buffer);
    printf("%d-PFLX RECV ROUTINE: exited cleanly\n", pflx->udpSocket->sockfd); fflush(stdout);
    return 0;
}

int pflx_network_status(pflx* pflx){
    int err = pthread_mutex_lock(&pflx->network_busy_mutex);
    if (err != 0){
        return -1;
    }
    int v = pflx->network_busy;
    err = pthread_mutex_unlock(&pflx->network_busy_mutex);
    if (err != 0){
        return -1;
    }
    return v;
}

int pflx_network_change_status(pflx* pflx){
    int err = pthread_mutex_lock(&pflx->network_busy_mutex);
    if (err != 0){
        return -1;
    }
    pflx->network_busy = 0;
    int v =  pflx->network_busy;
    err = pthread_mutex_unlock(&pflx->network_busy_mutex);
    if (err != 0){
        return -1;
    }
    return v;
}

pflx* pflx_init(short unsigned port, const Host* phonebook, size_t phonebook_size){
    pflx* socket = malloc(sizeof(pflx));
    if (!socket) return NULL;

    // allocate array of bst_set* of length phonebook_size
    socket->id_delivered = malloc(sizeof(bst_set*) * phonebook_size);
    for(size_t i = 0; i < phonebook_size; i++){
        socket->id_delivered[i] = bst_set_init();
        if (!socket->id_delivered[i]){
            free(socket);
            return NULL;
        }
    }
    
    UDP* udpSocket = udp_init(port);

    socket->upQueue = queue_init();
    socket->downQueue = queue_init();

    socket->udpSocket = udpSocket;
    socket->phonebook = phonebook;
    socket->ownMessageID = malloc(sizeof(unsigned int) * phonebook_size);
    for (size_t i = 0; i < phonebook_size; i++){
        socket->ownMessageID[i] = 1u; // start at 1 to match next_id_tbd default
    }

    pthread_mutex_init(&socket->ownMessageID_mutex, NULL);

    // Init next_id_tbd as delivered_state array
    socket->next_id_tbd = malloc(sizeof(delivered_state)*phonebook_size);
    for(size_t i = 0; i < phonebook_size; i++){
        pthread_mutex_init(&socket->next_id_tbd[i].mutex, NULL);
        socket->next_id_tbd[i].value = 1;
    }

    socket->phonebook_size = phonebook_size;

    // network being busy
    socket->network_busy = 0;
    pthread_mutex_init(&socket->network_busy_mutex, NULL);


    // Init graceful stop flag
    socket->should_stop = 0;
    pthread_mutex_init(&socket->should_stop_mutex, NULL);

    return socket;
}

int pflx_destroy(pflx* socket){
    if (!socket) return -1;

    // phonebook is not freed as it's a const 

    udp_destroy(socket->udpSocket);
    queue_destroy(socket->upQueue); queue_destroy(socket->downQueue);

    pthread_mutex_destroy(&socket->ownMessageID_mutex);

    // Destroy delivered_state array
    for(size_t i = 0; i < socket->phonebook_size; i++){
        pthread_mutex_destroy(&socket->next_id_tbd[i].mutex);
    }
    free(socket->next_id_tbd);

    for(size_t i = 0; i < socket->phonebook_size ; i++){
        bst_set_destroy(socket->id_delivered[i]);
    }
    free(socket->id_delivered);
    pthread_mutex_destroy(&socket->network_busy_mutex);

    free(socket->ownMessageID);
    // Destroy graceful stop mutex
    pthread_mutex_destroy(&socket->should_stop_mutex);

    free(socket);
    return 0;
}

pflx_message* pflx_message_init(void* message, size_t messageSize, size_t originID, size_t messageID, size_t targetID){
    pflx_message* msg = malloc(sizeof(pflx_message));

    if (msg == NULL){
        return NULL;
    }

    void* messageCopy = malloc(messageSize);
    if (messageCopy == NULL){
        free(msg);
        return NULL;
    }

    memcpy(messageCopy, message, messageSize);

    msg->message = messageCopy;
    msg->messageSize = messageSize;

    msg->messageID = messageID;
    msg->originID = originID;
    msg->targetID = targetID;

    return msg;
}

int pflx_message_destroy(pflx_message* message){
    free(message->message);
    free(message);

    return 0;
}

// Thread-safe accessors
size_t ack_read(delivered_state* s){
    if (!s) return 0;
    pthread_mutex_lock(&s->mutex);
    size_t v = s->value;
    pthread_mutex_unlock(&s->mutex);
    return v;
}

void ack_write(delivered_state* s, size_t v){
    if (!s) return;
    pthread_mutex_lock(&s->mutex);
    s->value = v;
    pthread_mutex_unlock(&s->mutex);
}

int ack_compare(delivered_state* s, size_t v){
    if (!s) return 0;
    int res;
    pthread_mutex_lock(&s->mutex);
    if (s->value > v){
        res = 1;
    } else if(s->value < v){
        res = -1;
    } else {
        res = 0;
    }
    pthread_mutex_unlock(&s->mutex);
    return res;
}

// Thread-safe helpers for graceful stop
void pflx_request_stop(pflx* pflx){
    if (!pflx) return;
    pthread_mutex_lock(&pflx->should_stop_mutex);
    pflx->should_stop = 1;
    pthread_mutex_unlock(&pflx->should_stop_mutex);
}
int pflx_should_stop(pflx* pflx){
    if (!pflx) return 1;
    pthread_mutex_lock(&pflx->should_stop_mutex);
    int v = pflx->should_stop;
    pthread_mutex_unlock(&pflx->should_stop_mutex);
    return v;
}