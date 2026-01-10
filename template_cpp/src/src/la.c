// external headers
#include<stdatomic.h>
#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<errno.h>
#include<pthread.h>

// internal
#include"pflx.h"
#include"queue.h"
#include"bst_set.h"
#include"ba.h"
#include"dict.h"
#include"la.h"
#include"threads_entry.h"
#include"utils.h"


static const int BUFFERED_PROPOSALS = 16;
static const int CONGESTION_CONTROL = 0;
static const int BUFFERSIZE = 512;
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

static inline void handle_populate(la_handle* incoming_proposal, int* frame){
    incoming_proposal->type_of_msg = frame[LA_TYPE_OF_MSG_HID];
    incoming_proposal->dest = frame[LA_TARGET_ID_HID];
    incoming_proposal->ID = frame[LA_ID_HID];
    incoming_proposal->retransmit = frame[LA_RETRANSMIT_HID];

    return;
}

int la_start(la* la){
    // Start pflx layer first
    int res = pflx_start(la->pflx_layer);
    if (res != 0) {
        return -1;
    }
    
    _threads_entry* e = _thr_get(la, 1);
    if (!e) {
        pflx_stop(la->pflx_layer);
        return -1;
    }

    // reset graceful stop flag
    atomic_store(&la->shouldStop, 0);

    // Start receiver first
    thread_wrapper_args* recvArgs = thread_wrapper_args_init(_recv_wrapper, la, la);
    if (!recvArgs || pthread_create(&e->recv_tid, NULL, generic_thread_wrapper, recvArgs) != 0) {
        free(recvArgs);
        pflx_stop(la->pflx_layer);
        return -1;
    }
    e->recv_started = 1;

    // Start sender
    thread_wrapper_args* sendArgs = thread_wrapper_args_init(_send_wrapper, la, la);
    if (!sendArgs || pthread_create(&e->send_tid, NULL, generic_thread_wrapper, sendArgs) != 0) {
        free(sendArgs);
        // rollback receiver
        pthread_cancel(e->recv_tid);
        pthread_join(e->recv_tid, NULL);
        e->recv_started = 0;
        pflx_stop(la->pflx_layer);
        return -1;
    }
    e->send_started = 1;

    return 0;
}

int la_stop(la* la){
    _threads_entry* e = _thr_get(la, 0);
    if (!e) {
        return 0; // Not an error - already stopped
    }

    // Request graceful stop FIRST
    atomic_store(&la->shouldStop, 1);

    // Wake sender loop (if waiting on downQueue) BEFORE joining
    if (e->send_started) {
        (void)queue_push(la->downQueue, LA_SHUTDOWN_SENTINEL, sizeof(void*));
    } 
    printf("%zu-LA STOP: pushing poison pill\n", la->pid); fflush(stdout);
    // Join threads BEFORE removing entry
    if (e->send_started) { 
        pthread_join(e->send_tid, NULL); 
        printf("%zu-LA STOP: joined send\n", la->pid); fflush(stdout);
        e->send_started = 0;
    }
    if (e->recv_started) { 
        pthread_join(e->recv_tid, NULL); 
        printf("%zu-LA STOP: joined recv\n", la->pid); fflush(stdout);
        e->recv_started = 0;
    }

    _thr_remove(la);
    
    printf("%zu-LA STOP: stopping pflx\n", la->pid); fflush(stdout);
    // Stop pflx and beb layers AFTER joining threads
    pflx_stop(la->pflx_layer);
    
    return 0;
}

int la_send(la* la, la_handle* handle){
    int res_queue = queue_push(la->downQueue, handle, sizeof(la_handle*));
    if(res_queue != 0){
        if(res_queue == ETIMEDOUT){
            printf("%zu-LA SEND: timeout\n", la->pid); fflush(stdout);
        }else{
            printf("%zu-LA SEND: error\n", la->pid); fflush(stdout);
            return res_queue;
        }
    }

    return 0;
}

int la_recv(la* la, int* buffer_set, size_t* size_set){
    int* received_set = NULL;
    size_t received_size = 0;
    
    int res_queue = queue_pop_timed(la->upQueue, (void**)&received_set, &received_size, TIMEOUT_QUEUE_POP);
    if(res_queue != 0){
        if(res_queue == ETIMEDOUT){
            printf("%zu-LA RECV: timeout\n", la->pid); fflush(stdout);
        }else{
            printf("%zu-LA RECV: error\n", la->pid); fflush(stdout);
        }
        return res_queue;
    }
    
    // received_size is in bytes, copy to caller's buffer
    if (received_set && received_size > 0) {
        memcpy(buffer_set, received_set, received_size);
        *size_set = received_size;
        free(received_set);  // Free the heap-allocated set from la_deliver
    } else {
        *size_set = 0;
    }
    
    printf("%zu-LA RECV: retrieved buffer_set of len: %zu bytes\n", la->pid, *size_set); fflush(stdout);

    return 0;
}

int la_send_routine(la* la){
    // vars for loop
    int buffer_idx = 0;
    int frame[BUFFERSIZE_OUTGOING];

    while(1){
        
        if (atomic_load_explicit(&la->shouldStop, memory_order_acquire) == 1){
            printf("%zu-LA SEND ROUTINE: stop requested, exiting\n", la->pid); fflush(stdout);
            break;
        }

        if(pflx_network_status(la->pflx_layer) == 0){
            printf("%zu-LA SEND ROUTINE: pflx network not busy\n", la->pid); fflush(stdout);
            la->network_busy = 0;
        }

        void* popped = NULL; size_t msgSize;
        int res = queue_pop_timed(la->downQueue, &popped, &msgSize, TIMEOUT_QUEUE_POP);
        if (res != 0){
            if (res == ETIMEDOUT){
                continue;
            }
            printf("%zu-LA SEND ROUTINE: queue_pop_timed failed with error %d\n", la->pid, res); fflush(stdout);
            return res;
        }

        // Check for shutdown sentinel
        if (popped == LA_SHUTDOWN_SENTINEL) {
            printf("%zu-LA SEND ROUTINE: shutdown sentinel received, exiting\n", la->pid); fflush(stdout);
            break;
        }
        
        // NOW MANAGING MESSAGE 
        la_handle* msg_handle = (la_handle*)popped;
        
        printf("%zu-LA SEND ROUTINE: handling handle with id:%d type:%d\n", la->pid, msg_handle->ID, msg_handle->type_of_msg); fflush(stdout);
        
        // is ID in buffer?
        pthread_mutex_lock(&la->sender_buffer_mutex);
        buffer_idx = translate_index_buffer(la, msg_handle->ID, BUFFERED_PROPOSALS);
        if (buffer_idx == -1){
            printf("%zu-LA SEND ROUTINE: recieved bad ID: %d, type: %d, round: %d, BUFFERED_PROPOSALS: %d \n", la->pid, msg_handle->ID, msg_handle->type_of_msg,atomic_load(&la->round), BUFFERED_PROPOSALS); fflush(stdout);
            pthread_mutex_unlock(&la->sender_buffer_mutex);
            continue;
        }
        
        // is retransmit correct?
        pthread_mutex_lock(&la->buffered_proposals[buffer_idx].prop_lock);
        if (la->buffered_proposals[buffer_idx].prop.restransmits != msg_handle->retransmit){
            printf("%zu-LA SEND ROUTINE: wrong retransmit id:%d type:%d\n", la->pid, msg_handle->ID, msg_handle->type_of_msg); fflush(stdout);
            pthread_mutex_unlock(&la->buffered_proposals[buffer_idx].prop_lock);
            pthread_mutex_unlock(&la->sender_buffer_mutex);
            continue;
        } 

        // has it been delivered already?
        if (atomic_load(&la->next_tbd) > msg_handle->ID){
            printf("%zu-LA SEND ROUTINE: handle has been delivered already id:%d type:%d\n", la->pid, msg_handle->ID, msg_handle->type_of_msg); fflush(stdout);
            pthread_mutex_unlock(&la->buffered_proposals[buffer_idx].prop_lock);
            pthread_mutex_unlock(&la->sender_buffer_mutex);
            continue;
        }

        // make frame
        int proposal_size = la->buffered_proposals[buffer_idx].prop.proposal_len;
        int frame_size = 0;

        // give more space only if is not ack
        if(msg_handle->type_of_msg != LA_ACK_TYPE){
            frame_size = (int)sizeof(int) * (LA_FRAME_HEADER_SIZE + proposal_size);
        }
        else{
            // +1 to accomodate the empty set of proposals
            frame_size = sizeof(int) * (LA_FRAME_HEADER_SIZE + 1);
        }
        int res_p2f = la_msg_to_frame(la, msg_handle, frame);
        // prop has been copied and can be unlocked 
        pthread_mutex_unlock(&la->buffered_proposals[buffer_idx].prop_lock);
        pthread_mutex_unlock(&la->sender_buffer_mutex);

        if(res_p2f != 0){
            printf("%zu-LA SEND ROUTINE: could not transform msg to frame id:%d type:%d\n", la->pid, msg_handle->ID, msg_handle->type_of_msg); fflush(stdout);
            queue_push(la->downQueue, msg_handle, sizeof(la_handle*));
            continue;
        }

        // broadcasting a new proposal
        if(msg_handle->type_of_msg == LA_PROPOSAL_TYPE){

            // broadcast frame
            printf("%zu-LA SEND ROUTINE: broadcasting frame id:%d\n", la->pid, msg_handle->ID); fflush(stdout);
            int res_beb = beb_with_pflx(la->pflx_layer, (size_t)la->pid, frame, (size_t)frame_size);

            if(res_beb != 0){
                // beb has to be retried
                printf("%zu-LA SEND ROUTINE: broadcast failed for id:%d, err:%d\n", la->pid, msg_handle->ID, res_beb); fflush(stdout);
                queue_push(la->downQueue, msg_handle, sizeof(la_handle*));
                continue;
            }
        }else if(msg_handle->type_of_msg == LA_ACK_TYPE || msg_handle->type_of_msg == LA_NACK_TYPE){
            // broadcast frame
            printf("%zu-LA SEND ROUTINE: transmitting p2p id:%d type:%d\n", la->pid, msg_handle->ID, msg_handle->type_of_msg); fflush(stdout);
            int res_p2p = pflx_send(la->pflx_layer, frame, (size_t)frame_size, la->pid, (size_t)msg_handle->dest);

            if(res_p2p != 0){
                // p2p has to be retried
                printf("%zu-LA SEND ROUTINE: transmitting p2p FAILED id:%d type:%d\n", la->pid, msg_handle->ID, msg_handle->type_of_msg); fflush(stdout);
                queue_push(la->downQueue, msg_handle, sizeof(la_handle*));
                continue;
            }
        }else{
            printf("%zu-LA SEND ROUTINE: got bad type_of_msg: %d\n", la->pid, msg_handle->type_of_msg); fflush(stdout);
        }
        printf("%zu-LA SEND ROUTINE: finished handling handle with ID: %d, type: %d\n", la->pid, msg_handle->ID, msg_handle->type_of_msg); fflush(stdout);
        

    }
    return 0;
}

int la_recv_routine(la* la){
    // init
    int popped[BUFFERSIZE];
    int msgSize;
    int len_of_proposal = 0;
    int f = (int)la->pflx_layer->phonebook_size / 2;
    bool remaining_queries_from_future = false;
    int* frame = NULL;
    void* popped_future = NULL;
    bool frame_is_from_future = false; // Track if current frame needs freeing

    la_handle* incoming_proposal = la_handle_init(0, 0, 0, 0);
    printf("%zu-LA RECV ROUTINE: started\n", la->pid); fflush(stdout);

    while(1){
        if (atomic_load_explicit(&la->shouldStop, memory_order_acquire) == 1){
            printf("%zu-LA RECV ROUTINE: stop requested, exiting\n", la->pid); fflush(stdout);
            break;
        }
        if(pflx_network_status(la->pflx_layer) == 0){
            printf("%zu-LA RECV ROUTINE: pflx network not busy\n", la->pid); fflush(stdout);
            la->network_busy = 0;
        }

        // POP EITHER FROM FUTURE PROPOSALS OR PFLX INTERFACE
        if(remaining_queries_from_future){
            int res = queue_pop_timed(la->future_proposals, &popped_future, (size_t*)&msgSize, TIMEOUT_QUEUE_POP / 10);
            if (res != 0){
                if (res == ETIMEDOUT){
                    printf("%zu-LA RECV ROUTINE: future_proposals timed out, no more unfinished business\n", la->pid); fflush(stdout);
                    remaining_queries_from_future = false;
                    continue;
                }
                printf("%zu-LA RECV ROUTINE: popping from future_proposals failed with error %d\n", la->pid, res); fflush(stdout);
                free(incoming_proposal);
                return res;
            }
            frame = (int*)popped_future;
            frame_is_from_future = true;
        }else{
            int res = pflx_recv(la->pflx_layer, popped, (size_t*)&msgSize);
            
            if (res != 0){
                if (res == ETIMEDOUT){
                    printf("%zu-LA RECV ROUTINE: pflx_recv timed out\n", la->pid); fflush(stdout);
                    continue;
                }
                printf("%zu-LA RECV ROUTINE: pflx_recv failed with error %d\n", la->pid, res); fflush(stdout);
                free(incoming_proposal);
                return res;
            }
            frame = (int*)popped;
            frame_is_from_future = false;
        }

        // RECONSTRUCT THE FRAME 
        // populate the incoming proposal
        handle_populate(incoming_proposal, frame);
        
        // Skip messages from ourselves
        if (frame[LA_ORIGIN_ID_HID] == (int)la->pid) {
            printf("%zu-LA RECV ROUTINE: skipping message from self, id:%d\n", la->pid, incoming_proposal->ID);
            fflush(stdout);
            if(frame_is_from_future) free(frame);
            continue;
        }
        
        // CHECK HANDLE IS RELEVANT
        // is ID in buffer?
        pthread_mutex_lock(&la->reciever_buffer_mutex);
        int buffer_idx = translate_index_buffer(la, incoming_proposal->ID, BUFFERED_PROPOSALS);
        if (buffer_idx == -1){
            printf("%zu-LA RECV ROUTINE: recieved future ID: %d, type: %d, round: %d, BUFFERED_PROPOSALS: %d \n", la->pid, incoming_proposal->ID, incoming_proposal->type_of_msg, atomic_load(&la->round), BUFFERED_PROPOSALS); fflush(stdout);
            size_t frame_size = sizeof(int) * (size_t)(frame[LA_SIZE_PROPOSAL_HID] + LA_FRAME_HEADER_SIZE);
            int* frame_cpy = malloc(frame_size);
            memcpy(frame_cpy, frame, frame_size);
            queue_push(la->future_proposals, frame_cpy, frame_size);
            pthread_mutex_unlock(&la->reciever_buffer_mutex);
            if(frame_is_from_future) free(frame);
            continue;

        }else if(buffer_idx == -2 && incoming_proposal->type_of_msg == LA_PROPOSAL_TYPE){
            printf("%zu-LA RECV ROUTINE: !PANIC! recieved old ID: %d, type: %d, round: %d, BUFFERED_PROPOSALS: %d \n", la->pid, incoming_proposal->ID, incoming_proposal->type_of_msg, atomic_load(&la->round), BUFFERED_PROPOSALS); fflush(stdout);
            pthread_mutex_unlock(&la->reciever_buffer_mutex);
            free(incoming_proposal);
            return 1;
        }
        
        // is retransmit correct?
        pthread_mutex_lock(&la->buffered_proposals[buffer_idx].prop_lock);
        if (la->buffered_proposals[buffer_idx].prop.restransmits != incoming_proposal->retransmit){
            pthread_mutex_unlock(&la->buffered_proposals[buffer_idx].prop_lock);
            pthread_mutex_unlock(&la->reciever_buffer_mutex);
            printf("%zu-LA RECV ROUTINE: wrong retransmit id:%d type:%d\n", la->pid, incoming_proposal->ID, incoming_proposal->type_of_msg); fflush(stdout);
            if(frame_is_from_future) free(frame);
            continue;
        } 
        
        // HANDLING MESSAGES
        len_of_proposal = frame[LA_SIZE_PROPOSAL_HID];
        int* incoming_proposed_set = &frame[LA_FRAME_HEADER_SIZE];

        printf("%zu-LA RECV ROUTINE: ABOUT TO PROCESS id:%d, from: %d, type of msg:%d, active: %d\n", la->pid, incoming_proposal->ID, frame[LA_ORIGIN_ID_HID], incoming_proposal->type_of_msg, la->buffered_proposals[buffer_idx].prop.active); fflush(stdout);
        
        // recieving a proposal
        if(incoming_proposal->type_of_msg == LA_PROPOSAL_TYPE){

            // Allocate a FRESH handle for this specific response
            la_handle* outgoing_msg = la_handle_init(
                frame[LA_ID_HID],           // ID
                frame[LA_ORIGIN_ID_HID],    // dest - send back to origin
                frame[LA_RETRANSMIT_HID],   // retransmit
                0                           // type_of_msg - set below
            );
            if (!outgoing_msg) {
                printf("%zu-LA RECV ROUTINE: failed to allocate outgoing_msg\n", la->pid); fflush(stdout);
                pthread_mutex_unlock(&la->buffered_proposals[buffer_idx].prop_lock);
                pthread_mutex_unlock(&la->reciever_buffer_mutex);
                if(frame_is_from_future) free(frame);
                continue;
            }

            printf("%zu-LA RECV ROUTINE: args for is_subset is %p with len %d, accepted values is:%p with len %d\n", la->pid, (void*)incoming_proposed_set, len_of_proposal, (void*)la->buffered_proposals[buffer_idx].accepted_values, la->buffered_proposals[buffer_idx].accepted_len); fflush(stdout);
            if (is_subset(incoming_proposed_set, len_of_proposal,
            la->buffered_proposals[buffer_idx].accepted_values, la->buffered_proposals[buffer_idx].accepted_len)){
                printf("%zu-LA RECV ROUTINE: acceptance set of id:%d is subset, sending ack to %d\n", la->pid, incoming_proposal->ID, outgoing_msg->dest); fflush(stdout);

                outgoing_msg->type_of_msg = LA_ACK_TYPE;

                // update own accept set
                memcpy(la->buffered_proposals[buffer_idx].accepted_values, incoming_proposed_set, (size_t)len_of_proposal * sizeof(int));
                la->buffered_proposals[buffer_idx].accepted_len = len_of_proposal;

            } else {
                printf("%zu-LA RECV ROUTINE: acceptance set of id:%d is not subset, sending nack to %d\n", la->pid, incoming_proposal->ID, outgoing_msg->dest); fflush(stdout);

                outgoing_msg->type_of_msg = LA_NACK_TYPE;

                // union on accept so that send routing can create the corresponding frame
                union_arrays(la->buffered_proposals[buffer_idx].accepted_values, &la->buffered_proposals[buffer_idx].accepted_len,
                incoming_proposed_set, len_of_proposal);

            }
            printf("%zu-LA RECV ROUTINE: sending back response for id:%d to dest:%d, type:%d\n", la->pid, outgoing_msg->ID, outgoing_msg->dest, outgoing_msg->type_of_msg); fflush(stdout);
            if(la_send(la, outgoing_msg) != 0){
                    printf("%zu-LA RECV ROUTINE: error in la_sending id:%d\n", la->pid, outgoing_msg->ID); fflush(stdout);
                    free(outgoing_msg);  // Free on error
                    pthread_mutex_unlock(&la->buffered_proposals[buffer_idx].prop_lock);
                    pthread_mutex_unlock(&la->reciever_buffer_mutex);
                    if(frame_is_from_future) free(frame);
                    continue;
                }

            // outgoing_msg ownership transferred to queue, don't free it here
            pthread_mutex_unlock(&la->buffered_proposals[buffer_idx].prop_lock);
            pthread_mutex_unlock(&la->reciever_buffer_mutex);
            if(frame_is_from_future) free(frame);
                
        }else if((incoming_proposal->type_of_msg == LA_ACK_TYPE || incoming_proposal->type_of_msg == LA_NACK_TYPE)
        && la->buffered_proposals[buffer_idx].prop.active){

            if (incoming_proposal->type_of_msg == LA_ACK_TYPE){
                printf("%zu-LA RECV ROUTINE: increasing ack for id:%d\n", la->pid, incoming_proposal->ID); fflush(stdout);
                la->buffered_proposals[buffer_idx].prop.n_acks++;
            }else if(incoming_proposal->type_of_msg == LA_NACK_TYPE){
                printf("%zu-LA RECV ROUTINE: increasing Nack for id:%d\n", la->pid, incoming_proposal->ID); fflush(stdout);
                la->buffered_proposals[buffer_idx].prop.n_nacks++;
                union_arrays(la->buffered_proposals[buffer_idx].prop.proposed_data, &la->buffered_proposals[buffer_idx].prop.proposal_len,
                    incoming_proposed_set, len_of_proposal);
            }

            // should nack_reset?
            if(la->buffered_proposals[buffer_idx].prop.n_acks + la->buffered_proposals[buffer_idx].prop.n_nacks > f + 1 &&
            la->buffered_proposals[buffer_idx].prop.n_nacks > 0){
                printf("%zu-LA RECV ROUTINE: nack resetting of id:%d\n", la->pid, incoming_proposal->ID); fflush(stdout);
                // union on accept so that send routing can create the corresponding frame
                la->buffered_proposals[buffer_idx].prop.n_acks = 0;
                la->buffered_proposals[buffer_idx].prop.n_nacks = 0;
                la->buffered_proposals[buffer_idx].prop.restransmits++;
                
                // Re-broadcast the proposal with new retransmit count
                la_handle* retransmit_handle = la_handle_init(
                    incoming_proposal->ID,
                    0,
                    la->buffered_proposals[buffer_idx].prop.restransmits,
                    LA_PROPOSAL_TYPE
                );
                if (retransmit_handle) {
                    la_send(la, retransmit_handle);
                }
            }
            // deliver check
            else if(la->buffered_proposals[buffer_idx].prop.n_acks > f + 1 && la->next_tbd == incoming_proposal->ID){
                printf("%zu-LA RECV ROUTINE: delivery in order of id:%d\n", la->pid, incoming_proposal->ID); fflush(stdout);
                int deliver_res = la_deliver(la, incoming_proposal->ID);
                if (deliver_res != 0){
                    printf("%zu-LA RECV ROUTINE: error in delivery of id:%d, code: %d\n", la->pid, incoming_proposal->ID, deliver_res); fflush(stdout);
                    pthread_mutex_unlock(&la->buffered_proposals[buffer_idx].prop_lock);
                    pthread_mutex_unlock(&la->reciever_buffer_mutex);
                    if(frame_is_from_future) free(frame);
                    continue;
                }
                
                pthread_mutex_unlock(&la->buffered_proposals[buffer_idx].prop_lock);
                pthread_mutex_unlock(&la->reciever_buffer_mutex);

                atomic_fetch_add(&la->next_tbd, 1);
                if(atomic_load(&la->next_tbd) > (atomic_load(&la->round) * BUFFERED_PROPOSALS) && la->round > 0 && atomic_load(&la->next_tbd) <= la->proposals_len){
                    printf("%zu-LA RECV ROUTINE: loading next proposals\n", la->pid); fflush(stdout);

                    int res_loading_proposals = proposal_load_next(la, BUFFERED_PROPOSALS);
                    if(res_loading_proposals != 0){
                        // in case total rounds is not a multiple of buffered props 
                    }
                    remaining_queries_from_future = true; // finish unfinished business

                }

                if(frame_is_from_future) free(frame);
                continue;
            }

            pthread_mutex_unlock(&la->buffered_proposals[buffer_idx].prop_lock);
            pthread_mutex_unlock(&la->reciever_buffer_mutex);
            if(frame_is_from_future) free(frame);

        }else{
            // malformed header
            printf("%zu-LA RECV ROUTINE: malformed header or inactive for id:%d, type_of_msg: %d, active: %d\n", la->pid, incoming_proposal->ID, incoming_proposal->type_of_msg, la->buffered_proposals[buffer_idx].prop.active); fflush(stdout);
            pthread_mutex_unlock(&la->buffered_proposals[buffer_idx].prop_lock);
            pthread_mutex_unlock(&la->reciever_buffer_mutex);
            if(frame_is_from_future) free(frame);
            continue;
        }
        printf("%zu-LA RECV ROUTINE: finished handling incoming msg with ID: %d, type: %d\n", la->pid, incoming_proposal->ID, incoming_proposal->type_of_msg); fflush(stdout);

    }
    free(incoming_proposal);

    return 0;
}

int la_deliver(la* la, int ID){
    int buffer_index = translate_index_buffer(la, ID, BUFFERED_PROPOSALS);

    if (buffer_index == -1){
        return 1;
    }

    if(la->buffered_proposals[buffer_index].prop.active){
        la->buffered_proposals[buffer_index].prop.active = false;
    }else{
        return 1;
    }

    size_t num_elements = (size_t)la->buffered_proposals[buffer_index].prop.proposal_len;
    size_t size_in_bytes = num_elements * sizeof(int);
    int* delivered_set = malloc(size_in_bytes);
    
    if(delivered_set == NULL){
        la->buffered_proposals[buffer_index].prop.active = true;
        return 1;
    }
    memcpy(delivered_set, la->buffered_proposals[buffer_index].prop.proposed_data, size_in_bytes);

    printf("%zu-LA DELIVER: buffered_proposals at idx %d has %d\n",la->pid, buffer_index, la->buffered_proposals[buffer_index].prop.proposed_data[0]);fflush(stdout);
    char log_buffer[4096];
    int offset = snprintf(log_buffer, sizeof(log_buffer), "%zu-LA DELIVER: idx: %d, for ID %d, size decision: %zu [", la->pid, buffer_index, ID, num_elements);
    for (size_t i = 0; i < num_elements && offset < (int)sizeof(log_buffer) - 10; i++) {
        offset += snprintf(log_buffer + offset, sizeof(log_buffer) - (size_t)offset, 
        "%d%s", delivered_set[i], (i < num_elements - 1) ? ", " : "");
    }
    snprintf(log_buffer + offset, sizeof(log_buffer) - (size_t)offset, "]\n");
    printf("%s", log_buffer);
    fflush(stdout);
    
    queue_push(la->upQueue, delivered_set, size_in_bytes);


    return 0;
    
}

la* la_init(pflx* pflx_layer, const char* config_path, size_t pid, int p, int ds, int vs){
    la* la_layer = malloc(sizeof(la));
    if (la_layer == NULL){
        return NULL;
    }

    la_layer->pid = pid;
    la_layer->network_busy = 0;

    la_layer->proposals_len = p;
    la_layer->ds = ds;
    la_layer->vs = vs;
    la_layer->config_path = config_path;



    la_layer->upQueue = queue_init();
    if(la_layer->upQueue == NULL){
        free(la_layer);
        return NULL;
    }
    la_layer->downQueue = queue_init();
    if(la_layer->downQueue == NULL){
        queue_destroy(la_layer->upQueue);
        free(la_layer);
        return NULL;
    }

    la_layer->round = 0;
    la_layer->buffered_proposals = malloc(sizeof(la_buffered_entry) * (size_t)BUFFERED_PROPOSALS);
    if(la_layer->buffered_proposals == NULL){
        queue_destroy(la_layer->downQueue);
        queue_destroy(la_layer->upQueue);
        free(la_layer);
        return NULL;
    }

    la_layer->future_proposals = queue_init();
    if(la_layer->future_proposals == NULL){
        queue_destroy(la_layer->downQueue);
        queue_destroy(la_layer->upQueue);
        free(la_layer->buffered_proposals);
        free(la_layer);
        return NULL;
    }

    for(int i = 0; i<BUFFERED_PROPOSALS; i++){
        pthread_mutex_init(&la_layer->buffered_proposals[i].prop_lock, NULL);
    }

    pthread_mutex_init(&la_layer->sender_buffer_mutex, NULL);
    pthread_mutex_init(&la_layer->reciever_buffer_mutex, NULL);


    la_layer->next_tbd = 1;

    la_layer->pflx_layer = pflx_layer;


    proposal_load_next(la_layer, BUFFERED_PROPOSALS);

    return la_layer;
}

int la_destroy(la* la_layer){
    if (!la_layer) return -1;

    // Destroy queues
    if (la_layer->upQueue) {
        queue_destroy(la_layer->upQueue);
    }
    if (la_layer->downQueue) {
        queue_destroy(la_layer->downQueue);
    }

    for(int i = 0; i < BUFFERED_PROPOSALS; i++){
        pthread_mutex_destroy(&la_layer->buffered_proposals[i].prop_lock);
    }

    pthread_mutex_destroy(&la_layer->sender_buffer_mutex);
    pthread_mutex_destroy(&la_layer->reciever_buffer_mutex);


    // Destroy buffered proposals
    if (la_layer->buffered_proposals) {
        free(la_layer->buffered_proposals);
    }

    int res = pflx_destroy(la_layer->pflx_layer);

    free(la_layer);
    return 0;
}

int beb_with_pflx(pflx* beb, size_t own_pid, void* message, size_t message_size){
    if(message == NULL){
        return 1;
    }
    for(size_t i = 1; i<=beb->phonebook_size; i++){
        if(i == own_pid) continue;
        int res_pflx = pflx_send(beb, message, message_size, own_pid, i);
        if (res_pflx != 0 && message != NULL){
            i--; // might block indefinetly if pflx_send keeps failing
        }
    }

    return 0;
}

int proposal_load_next(la* la_layer, int buffer_size){
    
    printf("%zu-LA LOAD NEXT: starting round: %d\n", la_layer->pid, atomic_load(&la_layer->round)); fflush(stdout);
    FILE* fp = fopen(la_layer->config_path, "r");
    if (!fp) {
        perror("Failed to open config file");
        return -1;
    }
    // Skip header line (proposals_len vs ds)
    char line[1152];
    int round = atomic_load(&la_layer->round);
    // Skip to the start line for this round
    int start_read_line = round * BUFFERED_PROPOSALS;
    // i = -1 to skip also the header, did not add to start_read_line as compiler complained assuming signed overflow
    for (int i = -1; i < start_read_line; i++) {
        if (!fgets(line, sizeof(line), fp)) {
            printf("%zu-LA LOAD NEXT: end of file reached upon skipping to starting line\n", la_layer->pid); fflush(stdout);
            fclose(fp);
            return -1;
        }
    }

    pthread_mutex_lock(&la_layer->sender_buffer_mutex);
    pthread_mutex_lock(&la_layer->reciever_buffer_mutex);

    int partial_ID = 0;
    // Read and process BUFFERED_PROPOSALS lines
    for (int i = 0; i < buffer_size && i < BUFFERED_PROPOSALS; i++) {
        if (!fgets(line, sizeof(line), fp)) {
            // End of file reached
            printf("%zu-LA LOAD NEXT: end of file reached upon populating buffer at index: %d\n", la_layer->pid, i); fflush(stdout);
            partial_ID = i+1;
            break;
        }
        
        // Initialize the proposal's structure
        la_layer->buffered_proposals[i].prop.n_acks = 1; // every process acks its own
        la_layer->buffered_proposals[i].prop.n_nacks = 0;
        la_layer->buffered_proposals[i].prop.restransmits = 0;
        la_layer->buffered_proposals[i].prop.proposal_len = 0; 
        la_layer->buffered_proposals[i].prop.active = true;
        la_layer->buffered_proposals[i].accepted_len = 0;
        
        int ID = (round * BUFFERED_PROPOSALS) + (i + 1);
        // Tokenize and translate numbers to indices
        char* token = strtok(line, " ");
        int num_tokens = 0;
        while (token != NULL) {

            la_layer->buffered_proposals[i].prop.proposed_data[num_tokens] = atoi(token);
            la_layer->buffered_proposals[i].prop.proposal_len++; 

            printf("%zu-LA LOAD NEXT: id:%d, index is %d, now size is: %d, last added value: %d\n", 
                la_layer->pid, ID, i, la_layer->buffered_proposals[i].prop.proposal_len, la_layer->buffered_proposals[i].prop.proposed_data[num_tokens]); fflush(stdout);
            
            token = strtok(NULL, " ");
            num_tokens++;
        }
        
        // create proposal and send it
        la_handle* handle = la_handle_init(ID, 0, 0, LA_PROPOSAL_TYPE);
        if(handle == NULL){
            printf("%zu-LA LOAD NEXT: CRITICAL ERROR handle init not succesful", la_layer->pid); fflush(stdout);
            return -1;
        }
        la_send(la_layer, handle);
    }

    atomic_fetch_add(&la_layer->round, 1);
    pthread_mutex_unlock(&la_layer->sender_buffer_mutex);
    pthread_mutex_unlock(&la_layer->reciever_buffer_mutex);

    printf("%zu-LA LOAD NEXT: finished succesfully\n", la_layer->pid); fflush(stdout);
    fclose(fp);
    return partial_ID;
}

la_handle* la_handle_init(int ID, int dest, int retransmit, int type_of_msg){
    la_handle* handle = malloc(sizeof(la_handle));
    if (handle == NULL){
        return NULL;
    }

    handle->type_of_msg = type_of_msg;
    handle->dest = dest;
    handle->ID = ID;
    handle->retransmit = retransmit;

    return handle;
}

int la_msg_to_frame(la* la, la_handle* handle, int frame[BUFFERSIZE_OUTGOING]){
    if(!frame){
        return 1;
    }

    size_t size_set = 0;
    frame[LA_FRAME_HEADER_SIZE] = 0;

    // is frame for ack
    if(handle->type_of_msg != LA_ACK_TYPE){
        // load the current proposal
        int buffer_idx = translate_index_buffer(la, handle->ID, BUFFERED_PROPOSALS);
        if(buffer_idx == -1 || handle->type_of_msg == LA_ACK_TYPE){
            return -1;
        }

        if(handle->type_of_msg == LA_PROPOSAL_TYPE){
            la_proposal prop = la->buffered_proposals[buffer_idx].prop;
            size_set = (size_t)prop.proposal_len;
            
            memcpy(&frame[LA_FRAME_HEADER_SIZE], prop.proposed_data, size_set);
        }else if(handle->type_of_msg == LA_NACK_TYPE){
            // union incoming proposed value + accepted value
            int* accepted_vals = la->buffered_proposals[buffer_idx].accepted_values;
            size_set = (size_t)la->buffered_proposals[buffer_idx].accepted_len;
            
            memcpy(&frame[LA_FRAME_HEADER_SIZE], accepted_vals, size_set);
        }else{
            return 1;
        }
    }
    
    frame[LA_TYPE_OF_MSG_HID] = handle->type_of_msg;
    frame[LA_ORIGIN_ID_HID] = (int)la->pid;
    frame[LA_TARGET_ID_HID] = handle->dest;
    frame[LA_ID_HID] = handle->ID;
    frame[LA_RETRANSMIT_HID] = handle->retransmit;
    frame[LA_SIZE_PROPOSAL_HID] = (int)size_set;

    return 0;
}

la_proposal* la_proposal_init(){
    la_proposal* prop = malloc(sizeof(la_proposal));
    if(prop == NULL){
        return NULL;
    }

    prop->active = false;
    prop->n_acks = 0;
    prop->n_nacks = 0;
    prop->proposal_len = 0;
    prop->restransmits = 0;
    memset(prop->proposed_data, 0, LA_UNIQUE_VALUES * sizeof(int));
    
    return prop;
}

void la_proposal_destroy(la_proposal* la_prop){
    if(la_prop == NULL){
        return;
    }
    free(la_prop);

    return;
}