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


static const int BUFFERED_PROPOSALS = 32;
static const int CONGESTION_CONTROL = 0;
static const int BUFFERSIZE = 512 * sizeof(int);
static const int BUFFERSIZE_OUTGOING = 512 * sizeof(int) * 2;
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

    // Join threads BEFORE removing entry
    if (e->send_started) { 
        pthread_join(e->send_tid, NULL); 
        e->send_started = 0;
    }
    if (e->recv_started) { 
        pthread_join(e->recv_tid, NULL); 
        e->recv_started = 0;
    }

    _thr_remove(la);
    
    // Stop pflx and beb layers AFTER joining threads
    pflx_stop(la->pflx_layer);
    
    return 0;
}

int la_send(la* la, la_handle* handle){
    int res_queue = queue_push(la->downQueue, handle, sizeof(la_handle*));
    if(res_queue != 0){
        return res_queue;
    }

    return 0;
}

int la_recv(la* la, int* buffer_set, size_t* size_set){
    int res_queue = queue_pop_timed(la->upQueue, &buffer_set, size_set, TIMEOUT_QUEUE_POP);
    if(res_queue != 0){
        return res_queue;
    }

    return 0;
}

int la_send_routine(la* la){
    // vars for loop
    int buffer_idx = 0;
    int* frame[BUFFERSIZE_OUTGOING];

    while(1){
        
        if (atomic_load_explicit(&la->shouldStop, memory_order_acquire) == 1){
            printf("%zu-LA SEND ROUTINE: stop requested, exiting\n", la->pid); fflush(stdout);
            break;
        }

        if(pflx_network_status(la->pflx_layer) == 0){
            printf("%zu-LA SEND ROUTINE: pflx network not busy\n", la->pid); fflush(stdout);
            la->network_busy = 0;
        }

        struct timespec ts = { .tv_sec = 0, .tv_nsec = CONGESTION_CONTROL }; // 10ms = 10000000
        nanosleep(&ts, NULL);

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


        // is ID in buffer?
        buffer_idx = translate_index_buffer(la, msg_handle->ID, BUFFERED_PROPOSALS);
        if (buffer_idx == -1){
            printf("%zu-LA SEND ROUTINE: recieved bad ID: %d, round: %d, BUFFERED_PROPOSALS: %d \n", la->pid, msg_handle->ID, la->round, BUFFERED_PROPOSALS); fflush(stdout);
            continue;
        }

        // is retransmit correct?
        pthread_mutex_lock(&la->buffered_proposals[buffer_idx].prop_lock);
        if (la->buffered_proposals[buffer_idx].prop.restransmits != msg_handle->retransmit){
            pthread_mutex_unlock(&la->buffered_proposals[buffer_idx].prop_lock);
            continue;
        } 

        // has it been delivered already?
        if (atomic_load(&la->next_tbd) > msg_handle->ID){
            pthread_mutex_unlock(&la->buffered_proposals[buffer_idx].prop_lock);
            continue;
        }

        // make frame
        int proposal_size = la->buffered_proposals[buffer_idx].prop.proposal_len;
        int frame_size = 0;

        // give more space only if is not ack
        if(msg_handle->type_of_msg != LA_ACK_TYPE){
            frame_size = sizeof(int) * (LA_FRAME_HEADER_SIZE + proposal_size);
        }
        else{
            // +1 to accomodate the empty set of proposals
            frame_size = sizeof(int) * (LA_FRAME_HEADER_SIZE + 1);
        }
        
        int res_p2f = la_msg_to_frame(la, msg_handle, &frame);
        // prop has been copied and can be unlocked 
        pthread_mutex_unlock(&la->buffered_proposals[buffer_idx].prop_lock);

        if(res_p2f != 0){
            queue_push(la->downQueue, msg_handle, sizeof(la_handle*));
            continue;
        }

        // broadcasting a new proposal
        if(msg_handle->type_of_msg == LA_PROPOSAL_TYPE){

            // broadcast frame
            int res_beb = beb_with_pflx(la->pflx_layer, la->pid, frame, frame_size);

            if(res_beb != 0){
                // beb has to be retried
                queue_push(la->downQueue, msg_handle, sizeof(la_handle*));
                continue;
            }
        }else if(msg_handle->type_of_msg == LA_ACK_TYPE || msg_handle->type_of_msg == LA_NACK_TYPE){
            // broadcast frame
            int res_p2p = pflx_send(la->pflx_layer, frame, (size_t)frame_size, la->pid, msg_handle->dest);

            if(res_p2p != 0){
                // p2p has to be retried
                queue_push(la->downQueue, msg_handle, sizeof(la_handle*));
                continue;
            }
        }else{
            printf("%zu-LA SEND ROUTINE: got bad type_of_msg: %d\n", la->pid, msg_handle->type_of_msg); fflush(stdout);
        }
    }
    return 0;
}

int la_recv_routine(la* la){
    // init
    void* popped[BUFFERSIZE];
    int msgSize;
    int len_of_proposal = 0;
    int f = la->pflx_layer->phonebook_size / 2;
    
    la_handle* incoming_proposal = la_handle_init(0, 0, 0, 0);
    la_handle* outgoing_msg = la_handle_init(0, 0, 0, 0);
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

        int res = pflx_recv(la->pflx_layer, popped, &msgSize);
        if (res != 0){
            if (res == ETIMEDOUT){
                printf("%zu-LA RECV ROUTINE: pflx_recv timed out\n", la->pid); fflush(stdout);
                continue;
            }
            printf("%zu-LA RECV ROUTINE: pflx_recv failed with error %d\n", la->pid, res); fflush(stdout);
            free(popped);
            return res;
        }

        // CHECK HANDLE IS RELEVANT
        
        // is ID in buffer?
        int buffer_idx = translate_index_buffer(la, incoming_proposal->ID, BUFFERED_PROPOSALS);
        if (buffer_idx == -1){
            continue;
        }
        
        // is retransmit correct?
        pthread_mutex_lock(&la->buffered_proposals[buffer_idx].prop_lock);
        if (la->buffered_proposals[buffer_idx].prop.restransmits != incoming_proposal->retransmit){
            pthread_mutex_unlock(&la->buffered_proposals[buffer_idx].prop_lock);
            continue;
        } 
        
        // RECONSTRUCT THE FRAME

        int* frame = (int*)popped;
        // populate the incoming proposal
        handle_populate(incoming_proposal, frame);
        
        // HANDLING MESSAGES
        len_of_proposal = frame[LA_SIZE_PROPOSAL_HID];
        int* incoming_proposed_set = frame[LA_FRAME_HEADER_SIZE];

        // recieving a proposal
        if(incoming_proposal->type_of_msg == LA_PROPOSAL_TYPE){

            outgoing_msg->dest = frame[LA_ORIGIN_ID_HID];
            outgoing_msg->ID = frame[LA_ID_HID];
            outgoing_msg->retransmit = frame[LA_RETRANSMIT_HID];
            
            if (is_subset(incoming_proposed_set, len_of_proposal,
            la->buffered_proposals[buffer_idx].accepted_values, la->buffered_proposals[buffer_idx].accepted_len)){
                outgoing_msg->type_of_msg = LA_ACK_TYPE;

                // update own accept set
                memcpy(la->buffered_proposals[buffer_idx].accepted_values, incoming_proposed_set, len_of_proposal);
                la->buffered_proposals[buffer_idx].accepted_len = len_of_proposal;

            } else {
                outgoing_msg->type_of_msg = LA_NACK_TYPE;

                // union on accept so that send routing can create the corresponding frame
                union_arrays(la->buffered_proposals[buffer_idx].accepted_values, &la->buffered_proposals[buffer_idx].accepted_len,
                incoming_proposed_set, len_of_proposal);

            }

            if(la_send(la, outgoing_msg) != 0){
                    pthread_mutex_unlock(&la->buffered_proposals[buffer_idx].prop_lock);
                    continue;
                }

        }else if((incoming_proposal->type_of_msg == LA_ACK_TYPE || incoming_proposal->type_of_msg == LA_NACK_TYPE)
        && la->buffered_proposals[buffer_idx].prop.active){

            if (incoming_proposal->type_of_msg == LA_ACK_TYPE){
                la->buffered_proposals[buffer_idx].prop.n_acks++;
            }else if(incoming_proposal->type_of_msg == LA_NACK_TYPE){
                la->buffered_proposals[buffer_idx].prop.n_nacks++;
                union_arrays(la->buffered_proposals[buffer_idx].prop.proposed_data, &la->buffered_proposals[buffer_idx].prop.proposal_len,
                    incoming_proposed_set, len_of_proposal);
            }

            // should nack_reset?
            if(la->buffered_proposals[buffer_idx].prop.n_acks + la->buffered_proposals[buffer_idx].prop.n_nacks > f + 1 &&
            la->buffered_proposals[buffer_idx].prop.n_nacks > 0){
                // union on accept so that send routing can create the corresponding frame
                la->buffered_proposals[buffer_idx].prop.n_acks = 0;
                la->buffered_proposals[buffer_idx].prop.restransmits++;
            }
            // deliver check
            else if(la->buffered_proposals[buffer_idx].prop.n_acks > f + 1 ){
                if(la->next_tbd == incoming_proposal->ID){
                    int deliver_res = la_deliver(la, incoming_proposal->ID);
                    if (deliver_res != 0){
                        pthread_mutex_unlock(&la->buffered_proposals[buffer_idx].prop_lock);
                        continue;
                    }
                    atomic_fetch_add(&la->next_tbd, 1);
                    if(atomic_load(&la->next_tbd) > (1 + la->round) * BUFFERED_PROPOSALS){
                        proposal_load_next(la, BUFFERED_PROPOSALS);
                    }
                }
            }
        }else{
            // malformed header
            pthread_mutex_unlock(&la->buffered_proposals[buffer_idx].prop_lock);
            continue;
        }
    }
    free(incoming_proposal); free(outgoing_msg);

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

    int size_delivered_set = la->buffered_proposals[buffer_index].prop.proposal_len;
    int* delivered_set = malloc(sizeof(int) * size_delivered_set);
    
    if(delivered_set == NULL){
        la->buffered_proposals[buffer_index].prop.active = true;
        return 1;
    }
    memcpy(delivered_set, la->buffered_proposals[buffer_index].prop.proposed_data, size_delivered_set);

    queue_push(la->upQueue, delivered_set, size_delivered_set);

    return 0;
    
}

la* la_init(pflx* pflx_layer, char* config_path, size_t pid){
    la* la_layer = malloc(sizeof(la));
    if (la_layer == NULL){
        return NULL;
    }

    la_layer->pid = pid;
    la_layer->network_busy = 0;

    la_layer->proposals_len = 0;
    la_layer->ds = 0;
    la_layer->vs = 0;
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
    la_layer->buffered_proposals = malloc(sizeof(la_buffered_entry) * BUFFERED_PROPOSALS);
    if(la_layer->buffered_proposals == NULL){
        queue_destroy(la_layer->downQueue);
        queue_destroy(la_layer->upQueue);
        free(la_layer);
        return NULL;
    }
    la_layer->next_tbd = 1;

    la_layer->pflx_layer = pflx_layer;

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

    // Destroy buffered proposals
    if (la_layer->buffered_proposals) {
        for (int i = 0; i < BUFFERED_PROPOSALS; i++) {
            if (&la_layer->buffered_proposals[i].prop.proposed_data) {
                ba_destroy(&la_layer->buffered_proposals[i].prop.proposed_data);
            }
            if (&la_layer->buffered_proposals[i].accepted_values) {
                ba_destroy(&la_layer->buffered_proposals[i].accepted_values);
            }
        }
        free(la_layer->buffered_proposals);
    }

    int res = pflx_destroy(la_layer->pflx_layer);

    free(la_layer);
    return 0;
}

int beb_with_pflx(pflx* beb, int own_pid, void* message, size_t message_size){
    if(message == NULL){
        return 1;
    }
    for(int i = 1; i<beb->phonebook_size; i++){
        if(i == own_pid) continue;
        int res_pflx = pflx_send(beb, message, message_size, own_pid, i);
        if (res_pflx != 0 && message != NULL){
            i--; // might block indefinetly if pflx_send keeps failing
        }
    }

    return 0;
}

int proposal_load_next(la* la_layer, int buffer_size){
    FILE* fp = fopen(la_layer->config_path, "r");
    if (!fp) {
        perror("Failed to open config file");
        return -1;
    }

    // Skip header line (proposals_len vs ds)
    char line[1152];
    fgets(line, sizeof(line), fp);
    if(la_layer->round == 0){
        sscanf(line, "%d %d %d", &la_layer->proposals_len, &la_layer->vs, &la_layer->ds) == 3;
    }
    
    // If round > 0, write back accepted values from previous round
    // UNCOMMENT AND IMPLEMENT IF DOES NOT PASS CORRECTNESS
    /*

    if (la_layer->round > 0) {
        int start_line = (la_layer->round - 1) * BUFFERED_PROPOSALS;
        
        // Skip to the start line for writing
        for (int i = 0; i < start_line; i++) {
            if (!fgets(line, sizeof(line), fp)) {
                fclose(fp);
                return -1;
            }
        }
        
        // TODO: Actual file writing would require reopening in r+ mode
        // For now, this is a read-only implementation placeholder
        // You may need to use a temporary file or different approach for writing
    }*/
    
    // Calculate which line to start reading from
    int start_read_line = la_layer->round * BUFFERED_PROPOSALS;
    
    // Read and process BUFFERED_PROPOSALS lines
    for (int i = 0; i < buffer_size && i < BUFFERED_PROPOSALS; i++) {
        if (!fgets(line, sizeof(line), fp)) {
            // End of file reached
            fclose(fp);
            return -1;
        }
        
        // Initialize the proposal's structure
        memset(la_layer->buffered_proposals[i].prop.proposed_data, 
        la_layer->buffered_proposals[i].prop.proposal_len * sizeof(int), 0);
        
        // Initialize other fields
        la_layer->buffered_proposals[i].prop.n_acks = 1; // every process acks its own
        la_layer->buffered_proposals[i].prop.restransmits = 0;
        la_layer->buffered_proposals[i].prop.proposal_len = 0; 
        la_layer->buffered_proposals[i].prop.active = true;
        
        // Initialize accepted_values
        memset(la_layer->buffered_proposals[i].accepted_values, 
        la_layer->buffered_proposals[i].accepted_len * sizeof(int), 0);

        la_layer->buffered_proposals[i].accepted_len = 0;
        
        // Tokenize and translate numbers to indices
        char* token = strtok(line, " ");
        while (token != NULL) {

            la_layer->buffered_proposals[i].prop.proposed_data[i] = atoi(token);
            la_layer->buffered_proposals[i].prop.proposal_len++; 
            
            token = strtok(NULL, " ");
        }
    }
    
    fclose(fp);
    return 0;
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

static inline void handle_populate(la_handle* incoming_proposal, int* frame){
    incoming_proposal->type_of_msg = frame[LA_TYPE_OF_MSG_HID];
    incoming_proposal->dest = frame[LA_TARGET_ID_HID];
    incoming_proposal->ID = frame[LA_ID_HID];
    incoming_proposal->retransmit = frame[LA_RETRANSMIT_HID];

    return;
}

int la_msg_to_frame(la* la, la_handle* handle, int** frame){
    if(!frame || !*frame){
        return 1;
    }

    int size_set = 0;
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
            size_set = prop.proposal_len * sizeof(int);
            frame[LA_FRAME_HEADER_SIZE] = malloc(size_set);
            if (frame[LA_FRAME_HEADER_SIZE] == NULL){
                return 1;
            }
            memcpy(frame[LA_FRAME_HEADER_SIZE], prop.proposed_data, size_set);
        }else if(handle->type_of_msg == LA_NACK_TYPE){
            // union incoming proposed value + accepted value
            int* accepted_vals = la->buffered_proposals[buffer_idx].accepted_values;
            size_set = la->buffered_proposals[buffer_idx].accepted_len * sizeof(int);
            frame[LA_FRAME_HEADER_SIZE] = malloc(size_set);
            if (frame[LA_FRAME_HEADER_SIZE] == NULL){
                return 1;
            }
            memcpy(frame[LA_FRAME_HEADER_SIZE], accepted_vals, size_set);
        }else{
            return 1;
        }
    }
    
    frame[LA_TYPE_OF_MSG_HID] = handle->type_of_msg;
    frame[LA_ORIGIN_ID_HID] = la->pid;
    frame[LA_TARGET_ID_HID] = handle->dest;
    frame[LA_ID_HID] = handle->ID;
    frame[LA_RETRANSMIT_HID] = handle->retransmit;
    frame[LA_SIZE_PROPOSAL_HID] = size_set;

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
    memset(prop->proposed_data, LA_UNIQUE_VALUES * sizeof(int), 0);
    
    return prop;
}

void la_proposal_destroy(la_proposal* la_prop){
    if(la_prop == NULL){
        return;
    }
    free(la_prop);

    return;
}