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
static const int BUFFERSIZE = 256;
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
    return 1;
}

int la_recv_routine(la* la){
    return 1;
}

int la_deliver(la* la, la_proposal* msg){
    return 1;
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
    la_layer->buffered_proposals = malloc(sizeof(la_buffered_entry*) * BUFFERED_PROPOSALS);
    if(la_layer->buffered_proposals == NULL){
        queue_destroy(la_layer->downQueue);
        queue_destroy(la_layer->upQueue);
        free(la_layer);
        return NULL;
    }

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
        la_layer->buffered_proposals[i].prop.proposal_len, 0);
        
        // Initialize other fields
        la_layer->buffered_proposals[i].prop.n_acks = 0;
        la_layer->buffered_proposals[i].prop.restransmits = 0;
        la_layer->buffered_proposals[i].prop.proposal_len = 0; 
        la_layer->buffered_proposals[i].prop.active = true;
        
        // Initialize accepted_values
        memset(la_layer->buffered_proposals[i].accepted_values, 
        la_layer->buffered_proposals[i].accepted_len, 0);
        
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

/*la_frame
int type_of_msg         4B
int origin_ID           4B
int index               4B
int retransmit          4B
int size_proposal       4B
int* proposal           size_proposal * 4B

type_of_msg = 0 -> bootstrap
            = 1 -> proposal
            = 2 -> ack
            = 3 -> nack
*/
int la_to_frame(la_proposal prop, int pid, int index, int** frame, int* frame_len){
    int size_proposal = prop.proposal_len * sizeof(int);
    frame[5] = malloc(size_proposal);
    if (frame[5] == NULL){
        return 1;
    }
    memcpy(frame[5], prop.proposed_data, size_proposal);
    
    frame[0] = 1;
    frame[1] = pid;
    frame[2] = index;
    frame[3] = prop.restransmits;
    frame[4] = size_proposal;

    return 0;
}

int* frame_to_proposal_array(int* frame, int* len_of_proposal){
    *len_of_proposal = frame[4];
    return frame[5];
}

la_proposal* la_proposal_init(){
    la_proposal* prop = malloc(sizeof(la_proposal));
    if(prop == NULL){
        return NULL;
    }

    prop->active = false;
    prop->n_acks = 0;
    prop->proposal_len = 0;
    prop->restransmits = 0;
    memset(prop->proposed_data, LA_UNIQUE_VALUES, 0);
    
    return prop;
}

void la_proposal_destroy(la_proposal* la_prop){
    if(la_prop == NULL){
        return;
    }
    free(la_prop);

    return;
}