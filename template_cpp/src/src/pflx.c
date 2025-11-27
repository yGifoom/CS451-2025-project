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

static const int CONGESTION_CONTROL = 0;
static const int BUFFERSIZE = 256;
static const long TIMEOUT_QUEUE_POP = 1000; // in ms
static const long MAX_DOWNQUEUE_SIZE = 100;
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
    if (!e) return -1;

    // Request graceful stop
    pflx_request_stop(pflx);

    // Wake sender loop (if waiting on downQueue)
    if (e->send_started) {
        (void)queue_push(pflx->downQueue, PFLX_SHUTDOWN_SENTINEL, sizeof(pflx_message*));
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

    _thr_remove(pflx);
    return 0;
}

int pflx_send(pflx* pflx, void* message, size_t messageSize, size_t originID, size_t targetID){
    pthread_mutex_lock(&pflx->ownMessageID_mutex);
    size_t msgID = pflx->ownMessageID;
    
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

    pflx->ownMessageID++;
    pthread_mutex_unlock(&pflx->ownMessageID_mutex);

    // Print the real payload size, not sizeof(pointer)
    printf("%d-PFLX SEND: I am sending '%s', message of len '%zu' to process: %zu\n", pflx->udpSocket->sockfd, (char*)msg->message, msg->messageSize, msg->targetID); fflush(stdout);
    return 0;
}

// main ruotine for sending 
int _pflx_send_routine(pflx* pflx){
    // init
    unsigned char* frame = malloc(BUFFERSIZE);
    size_t dataSize;

    while (1){
        // If stop requested and we aren't blocked, exit
        if (pflx_should_stop(pflx)) {
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

        pflx_message* msg_to_send = (pflx_message*)popped;

        size_t ack_msg_id;
        int lenMessageSent;
        size_t index = msg_to_send->targetID - 1;

        // Build wire frame: [originID, messageID, targetID, payloadSize][payload...]
        const size_t hdr_words = 4;
        const size_t hdr_size = hdr_words * sizeof(size_t);
        if (hdr_size + msg_to_send->messageSize > (size_t)BUFFERSIZE) {
            // Too large to fit, drop or truncate; here we drop quietly.
            printf("%d-PFLX SEND ROUTINE: message too large (%zu bytes), dropping\n",
                   pflx->udpSocket->sockfd, hdr_size + msg_to_send->messageSize);
            fflush(stdout);
            continue;
        }
        size_t* hdr = (size_t*)frame;
        hdr[0] = msg_to_send->originID;
        hdr[1] = msg_to_send->messageID;
        hdr[2] = msg_to_send->targetID;
        hdr[3] = msg_to_send->messageSize;
        memcpy(frame + hdr_size, msg_to_send->message, msg_to_send->messageSize);
        
        size_t frame_size = hdr_size + msg_to_send->messageSize;

        printf("%d-PFLX SEND ROUTINE: frame_size=%zu, hdr_size=%zu, payload=%zu\n",
               pflx->udpSocket->sockfd, frame_size, hdr_size, msg_to_send->messageSize);
        fflush(stdout);

        // check if message was ack
        if (msg_to_send->message && 
            sscanf((char*)msg_to_send->message, "ack %zu", &ack_msg_id) == 1) {
            // strictly reciever behaviour (sending an ACK back)
            lenMessageSent = udp_send(pflx->udpSocket, 
                                    pflx->phonebook[index].ip_readable, 
                                    ntohs(pflx->phonebook[index].port), 
                                    frame, frame_size);
            printf("%d-PFLX SEND ROUTINE: ACK sent, udp_send returned %d (expected %zu)\n",
                   pflx->udpSocket->sockfd, lenMessageSent, frame_size);
            fflush(stdout);
        } else {
            // strictly sender behaviour
            // sleep for a little bit as to not overwhelm the network
            struct timespec ts = { .tv_sec = 0, .tv_nsec = CONGESTION_CONTROL }; // 10ms = 10000000
            nanosleep(&ts, NULL);

            int ok = bst_set_lookup(pflx->nonConsequentAcks[index], msg_to_send->messageID, NULL, NULL);
            if(ok || ack_read(&pflx->expectedConsequentAck[index]) > msg_to_send->messageID){
                // already acked -> deliver
                pflx_message* msgToUp = (pflx_message*)popped;
                printf("%d-PFLX SEND ROUTINE: pushing into upQueue '%s'\n", pflx->udpSocket->sockfd, 
                    (char*)msgToUp->message); fflush(stdout);
                queue_push(pflx->upQueue, popped, dataSize);
            } else{
                lenMessageSent = udp_send(pflx->udpSocket, 
                                    pflx->phonebook[index].ip_readable, 
                                    ntohs(pflx->phonebook[index].port), 
                                    frame, frame_size);

                printf("%d-PFLX SEND ROUTINE: DATA sent, udp_send returned %d (expected %zu)\n",
                       pflx->udpSocket->sockfd, lenMessageSent, frame_size);
                fflush(stdout);

                // put message back in, we will be waiting for ack
                res = queue_push(pflx->downQueue, msg_to_send, sizeof(pflx_message*));
                if (res != 0){
                    return res;
                }

                if (lenMessageSent < 0){
                    // send failed, or queue push failed
                }
            }
        }
    }

    free(frame);
    return 0;
}

// returns what is deliverable: ("%zu %zu", senderid msgId)
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
    printf("%d-I am recieving '%s', message of len '%zu'\n", pflx->udpSocket->sockfd, (char*)message, *messageSize); fflush(stdout);
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

        ssize_t len = udp_recv_timeout(pflx->udpSocket, buffer, BUFFERSIZE, 1000);
        printf("%d-PFLX RECV ROUTINE: udp_recv_timeout returned %ld bytes\n",
               pflx->udpSocket->sockfd, len);
        fflush(stdout);
        
        if (len == 0) {
            // Timeout occurred
            network_how_busy--; 
            if (network_how_busy < 0){pflx_network_change_status(pflx);}
            continue;
        }
        network_how_busy = 5;
        
        if (len < 0) {
            // Error occurred
            free(buffer);
            return 1;
        }


        // Parse wire frame: [originID, messageID, targetID, payloadSize][payload...]
        const size_t hdr_words = 4;
        const size_t hdr_size = hdr_words * sizeof(size_t);
        if ((size_t)len < hdr_size) {
            // malformed, ignore
            printf("%d-PFLX RECV ROUTINE: here's the malformed buffer: '%s'\n", pflx->udpSocket->sockfd, buffer); fflush(stdout);

            continue;
        }

        size_t* hdr = (size_t*)buffer;
        size_t originID = hdr[0];
        size_t messageID = hdr[1];
        size_t targetID = hdr[2];
        size_t payloadSize = hdr[3];
        
        printf("%d-PFLX RECV ROUTINE: parsed header - originID=%zu, msgID=%zu, targetID=%zu, payloadSize=%zu\n",
               pflx->udpSocket->sockfd, originID, messageID, targetID, payloadSize);
        printf("%d-PFLX RECV ROUTINE: expected total=%zu, actual len=%ld, difference=%ld\n",
               pflx->udpSocket->sockfd, hdr_size + payloadSize, len, (long)(hdr_size + payloadSize) - len);
        fflush(stdout);
        
        if (hdr_size + payloadSize != (size_t)len) {
            // malformed, ignore
            printf("%d-PFLX RECV ROUTINE: SIZE MISMATCH! hdr_size=%zu + payloadSize=%zu = %zu, but received %ld bytes\n",
                   pflx->udpSocket->sockfd, hdr_size, payloadSize, hdr_size + payloadSize, len);
            fflush(stdout);
            continue;
        }
        printf("%d-PFLX RECV ROUTINE: recreating message from frame\n", pflx->udpSocket->sockfd); fflush(stdout);

        // Reconstruct a local pflx_message with a copied payload (safe pointer)
        pflx_message* msg_recvd = pflx_message_init(buffer + hdr_size, payloadSize, originID, messageID, targetID);
        if(msg_recvd == NULL) {
            return 1;
        }
        
        size_t ack_msg_id;
        size_t origin_index = msg_recvd->originID - 1;
        size_t target_index = msg_recvd->targetID - 1;
        size_t senderId, msgId;

        printf("%d-PFLX RECV ROUTINE: entering main logic\n", pflx->udpSocket->sockfd); fflush(stdout);
        // Check if it's an ACK //TODO: this is a terrible implementation, but fixing it
        // requires changing pflx message
        if (msg_recvd->message && sscanf((char*)msg_recvd->message, "ack %zu", &ack_msg_id) == 1) {
            // Use the peer index (origin of ACK), not our own ID
            size_t peer_index = origin_index;

            // BOUNDS CHECK (should be caught earlier, but defensive)
            if (peer_index >= pflx->phonebook_size) {
                printf("%d-PFLX RECV ROUTINE: dropping out of bounds ack (peer_index=%zu)\n",
                       pflx->udpSocket->sockfd, peer_index); fflush(stdout);
                pflx_message_destroy(msg_recvd);
                continue;
            }
            
            // SAFETY: Verify BST exists
            if (!pflx->nonConsequentAcks[peer_index]) {
                printf("%d-PFLX RECV ROUTINE: ERROR - BST at peer_index %zu is NULL!\n",
                       pflx->udpSocket->sockfd, peer_index);
                pflx_message_destroy(msg_recvd);
                continue;
            }

            if (peer_index >= pflx->phonebook_size) {
                // out-of-range ACK; drop
                printf("%d-PFLX RECV ROUTINE: dropping out of bounds ack\n", pflx->udpSocket->sockfd); fflush(stdout);
                
                pflx_message_destroy(msg_recvd);
                continue;
            }
            printf("%d-PFLX RECV ROUTINE: recieved an ack from %zu for message ID %zu\n",
                   pflx->udpSocket->sockfd, msg_recvd->originID, ack_msg_id); fflush(stdout);
            
            pthread_mutex_lock(&pflx->expectedConsequentAck[peer_index].mutex);

            if (pflx->expectedConsequentAck[peer_index].value == ack_msg_id){
                printf("%d-PFLX RECV ROUTINE: ACK matches expected consequent, updating\n",
                       pflx->udpSocket->sockfd); fflush(stdout);

                size_t removed = bst_set_compact_consequent(
                    pflx->nonConsequentAcks[peer_index], 
                    pflx->expectedConsequentAck[peer_index].value,
                NULL);

                pflx->expectedConsequentAck[peer_index].value += removed + 1;
            } else if (ack_msg_id > pflx->expectedConsequentAck[peer_index].value) {
                printf("%d-PFLX RECV ROUTINE: ACK is non-consequent, adding to set\n",
                       pflx->udpSocket->sockfd); fflush(stdout);
                
                    // BOUNDS CHECK for origin_index
                if (origin_index >= pflx->phonebook_size) {
                    printf("%d-PFLX RECV ROUTINE: origin_index %zu out of bounds!\n",
                        pflx->udpSocket->sockfd, origin_index);
                    pflx_message_destroy(msg_recvd);
                    continue;
                }
                
                // SAFETY: Verify BST exists
                if (!pflx->nonConsequentAcks[origin_index]) {
                    printf("%d-PFLX RECV ROUTINE: ERROR - BST at origin_index %zu is NULL!\n",
                        pflx->udpSocket->sockfd, origin_index);
                    pflx_message_destroy(msg_recvd);
                    continue;
                }

                int res = bst_set_add(pflx->nonConsequentAcks[peer_index], ack_msg_id, NULL, 0);
                if(res == -1){
                    pflx_message_destroy(msg_recvd);
                    pthread_mutex_unlock(&pflx->expectedConsequentAck[peer_index].mutex);
                    return res;
                }
            }
            pthread_mutex_unlock(&pflx->expectedConsequentAck[peer_index].mutex);
            // ACKs are protocol-internal: never deliver to app
            pflx_message_destroy(msg_recvd);
            continue;

        // Try to parse as two integers separated by whitespace
        } else if (msg_recvd->message){
            printf("%d-PFLX RECV ROUTINE: recvd message '%s', from %zu\n", pflx->udpSocket->sockfd, (char*)msg_recvd->message, senderId); fflush(stdout);
            // strictly reciever behaviour
            // copy values we'll need
            size_t msgOriginID =  msg_recvd->originID;
            size_t msgTargetID = msg_recvd->targetID;
            size_t msgMessageID = msg_recvd->messageID;
            
            // drop if too many packets
            if (queue_size(pflx->downQueue) > MAX_DOWNQUEUE_SIZE){
                printf("%d-PFLX RECV ROUTINE: dropping packet, recieving too many\n", pflx->udpSocket->sockfd); fflush(stdout);
                pflx_message_destroy(msg_recvd);
                continue;
            }
            printf("%d-PFLX RECV ROUTINE: downqueuesize = %zu\n", pflx->udpSocket->sockfd,queue_size(pflx->downQueue)); fflush(stdout);
            printf("%d-PFLX RECV ROUTINE: upqueuesize = %zu\n", pflx->udpSocket->sockfd, queue_size(pflx->upQueue)); fflush(stdout);


            int delivered = 0; // track if we hand the pointer up
            size_t expectedConsequentId;
            pthread_mutex_lock(&pflx->expectedConsequentAck[origin_index].mutex);

            expectedConsequentId = pflx->expectedConsequentAck[origin_index].value;
            if(expectedConsequentId > msgMessageID){
                // expectedConsequentId > msgMessageID - old message, already delivered
                printf("%d-PFLX RECV ROUTINE: recvd OLD msg (ID %zu < expected %zu), not delivering\n", 
                       pflx->udpSocket->sockfd, msgMessageID, expectedConsequentId); fflush(stdout);
            }else if(expectedConsequentId == msgMessageID){
                printf("%d-PFLX RECV ROUTINE: recvd an expected continuous msg\n", pflx->udpSocket->sockfd); fflush(stdout);
                
                size_t removed = bst_set_compact_consequent(
                    pflx->nonConsequentAcks[origin_index],
                    expectedConsequentId, NULL);

                pflx->expectedConsequentAck[origin_index].value += removed + 1;

                // deliver message (push capsule)
                queue_push(pflx->upQueue, msg_recvd, sizeof(pflx_message*));
                delivered = 1;
            }else if(expectedConsequentId < msgMessageID){
                // non consequent, let's save it
                int res = bst_set_add(pflx->nonConsequentAcks[origin_index], msgMessageID, NULL, 0);
                if (res == -1){
                    pthread_mutex_unlock(&pflx->expectedConsequentAck[origin_index].mutex);
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

            pthread_mutex_unlock(&pflx->expectedConsequentAck[origin_index].mutex);

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
        } else {
            // If not ACK and not parsable payload, drop silently
            printf("%d-PFLX RECV ROUTINE: recieved null message\n",
                   pflx->udpSocket->sockfd); fflush(stdout);
            pflx_message_destroy(msg_recvd);
            continue;
        }
    }
    free(buffer);
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
    pflx->network_busy = pflx->network_busy == 0 ? 1 : 0;
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
    socket->nonConsequentAcks = malloc(sizeof(bst_set*) * phonebook_size);
    for(size_t i = 0; i < phonebook_size; i++){
        socket->nonConsequentAcks[i] = bst_set_init();
        if (!socket->nonConsequentAcks[i]){
            free(socket);
            return NULL;
        }
    }
    
    UDP* udpSocket = udp_init(port);

    socket->upQueue = queue_init();
    socket->downQueue = queue_init();

    socket->udpSocket = udpSocket;
    socket->phonebook = phonebook;
    socket->ownMessageID = 1; // start at 1 to match expectedConsequentAck default
    pthread_mutex_init(&socket->ownMessageID_mutex, NULL);

    // Init expectedConsequentAck as ack_state array
    socket->expectedConsequentAck = malloc(sizeof(ack_state)*phonebook_size);
    for(size_t i = 0; i < phonebook_size; i++){
        pthread_mutex_init(&socket->expectedConsequentAck[i].mutex, NULL);
        socket->expectedConsequentAck[i].value = 1;
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

    // Destroy ack_state array
    for(size_t i = 0; i < socket->phonebook_size; i++){
        pthread_mutex_destroy(&socket->expectedConsequentAck[i].mutex);
    }
    free(socket->expectedConsequentAck);

    for(size_t i = 0; i < socket->phonebook_size ; i++){
        bst_set_destroy(socket->nonConsequentAcks[i]);
    }
    free(socket->nonConsequentAcks);
    pthread_mutex_destroy(&socket->network_busy_mutex);

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
size_t ack_read(ack_state* s){
    if (!s) return 0;
    pthread_mutex_lock(&s->mutex);
    size_t v = s->value;
    pthread_mutex_unlock(&s->mutex);
    return v;
}
void ack_write(ack_state* s, size_t v){
    if (!s) return;
    pthread_mutex_lock(&s->mutex);
    s->value = v;
    pthread_mutex_unlock(&s->mutex);
}

int ack_compare(ack_state* s, size_t v){
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