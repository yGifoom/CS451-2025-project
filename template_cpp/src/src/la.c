// external headers
#include<stdatomic.h>
#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<errno.h>

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

int la_send(la* la, void* message, size_t originID){
    return 1;
}

int la_recv(la* la, void* message, size_t* messageSize){
    return 1;
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
    la_layer->proposal_to_index_translation = NULL;
    la_layer->index_to_proposal_int_translation = NULL;



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
            if (la_layer->buffered_proposals[i].prop.proposed_data) {
                ba_destroy(la_layer->buffered_proposals[i].prop.proposed_data);
            }
            if (la_layer->buffered_proposals[i].accepted_values) {
                ba_destroy(la_layer->buffered_proposals[i].accepted_values);
            }
        }
        free(la_layer->buffered_proposals);
    }

    // Destroy dictionary
    if (la_layer->proposal_to_index_translation) {
        dic_delete(la_layer->proposal_to_index_translation);
    }

    // Free translation array
    if (la_layer->index_to_proposal_int_translation) {
        free(la_layer->index_to_proposal_int_translation);
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
    char line[1024];
    fgets(line, sizeof(line), fp);
    
    // If round > 0, write back accepted values from previous round
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
    }
    
    // Calculate which line to start reading from
    int start_read_line = la_layer->round * BUFFERED_PROPOSALS;
    
    // Skip to start_read_line
    for (int i = 0; i < start_read_line; i++) {
        if (!fgets(line, sizeof(line), fp)) {
            fclose(fp);
            return -1;
        }
    }
    
    // Read and process BUFFERED_PROPOSALS lines
    for (int i = 0; i < buffer_size && i < BUFFERED_PROPOSALS; i++) {
        if (!fgets(line, sizeof(line), fp)) {
            // End of file reached
            fclose(fp);
            return -1;
        }
        
        // Initialize the proposal's ba structure
        la_layer->buffered_proposals[i].prop.proposed_data = ba_init(la_layer->ds);
        if (!la_layer->buffered_proposals[i].prop.proposed_data) {
            fclose(fp);
            return -1;
        }
        
        // Initialize other fields
        la_layer->buffered_proposals[i].prop.n_acks = 0;
        la_layer->buffered_proposals[i].prop.restransmits = 0;
        la_layer->buffered_proposals[i].prop.active = true;
        
        // Initialize accepted_values
        la_layer->buffered_proposals[i].accepted_values = ba_init(la_layer->ds);
        if (!la_layer->buffered_proposals[i].accepted_values) {
            ba_destroy(la_layer->buffered_proposals[i].prop.proposed_data);
            fclose(fp);
            return -1;
        }
        
        // Tokenize and translate numbers to indices
        char* token = strtok(line, " \t\n");
        while (token != NULL) {
            // Look up the index for this proposal number
            int key_len = strlen(token);
            if (dic_find(la_layer->proposal_to_index_translation, token, key_len)) {
                int index = *la_layer->proposal_to_index_translation->value;
                // Set the bit at this index
                ba_set(la_layer->buffered_proposals[i].prop.proposed_data, index);
            }
            
            token = strtok(NULL, " \t\n");
        }
    }
    
    fclose(fp);
    return 0;
}

int la_bootstrap_from_config(la* la, char* config_path){
    // dict for conversion
    struct dictionary* num_to_index = dic_new(0);
    
    // PARSE CONFIG FILE FOR FIRST DATA
    FILE* fp = fopen(config_path, "r");
    if (!fp) {
        perror("Failed to open config file");
        return -1;
    }

    // Parse first line: proposals_len vs ds
    if (fscanf(fp, "%d %d %d", &la->proposals_len, &la->vs, &la->ds) != 3) {
        fprintf(stderr, "Failed to parse first line\n");
        fclose(fp);
        return -1;
    }

    char line[1024];
    int unique_proposal_nums = 0;
    // Skip to next line
    fgets(line, sizeof(line), fp);
    
    // Process each proposal line
    for (int i = 0; i < la->proposals_len; i++) {
        if (!fgets(line, sizeof(line), fp)) {
            fprintf(stderr, "Unexpected end of file at line %d\n", i + 2);
            fclose(fp);
            dic_delete(num_to_index);
            return -1;
        }

        // Tokenize the line
        char* token = strtok(line, " \t\n");
        while (token != NULL) {
            int num = atoi(token);
            char key[32];
            snprintf(key, sizeof(key), "%d", num);
            
            if(!dic_add(num_to_index, key, strlen(key))){
                unique_proposal_nums++;
            }
            
            token = strtok(NULL, " \t\n");
        }
    }
    
    fclose(fp);
    
    // MAKE FRAME AND SHARE WITH PEERS
    int recieved_full_proposal_nums = 0; // has process recieved message with all of the possible proposals already?
    size_t message_size = sizeof(int) * (la->ds + 2);

    int* bootstrap_frame = malloc(message_size);
    bootstrap_frame[0] = 0;                     // type of message
    bootstrap_frame[1] = unique_proposal_nums;  // how many numbers 

    // send and collect results
    int* incoming_data = NULL; size_t incoming_data_size; 
    int random_pid = rand() % la->pflx_layer->phonebook_size + 1;
    while(unique_proposal_nums < la->ds){
        pflx_send(la->pflx_layer, bootstrap_frame, message_size, la->pid, random_pid);
        
        // pop from beb_queue to see if anyone has finished
        int queue_pop_res = queue_pop_timed(la->pflx_layer->upQueue, &incoming_data, &incoming_data_size, TIMEOUT_QUEUE_POP);
        if(queue_pop_res != 0){
            if(queue_pop_res == ETIMEDOUT){}
                else{
                dic_delete(num_to_index);
                return queue_pop_res;
            }
        }else{
            recieved_full_proposal_nums = 1;
            break;
        }
        
        queue_pop_res = queue_pop_timed(la->pflx_layer->upQueue, &incoming_data, &incoming_data_size, TIMEOUT_QUEUE_POP);
        if(queue_pop_res != 0){
            if(queue_pop_res == ETIMEDOUT){}
                else{
                dic_delete(num_to_index);
                return queue_pop_res;
            }
        }else{
            _bootstrap_merge_incoming_proposals(&bootstrap_frame, &incoming_data, &num_to_index, &unique_proposal_nums, la->ds);
        }
    }

    if (recieved_full_proposal_nums){
        // spread the complete array and wait until all peers recieved
        beb_with_pflx(la->pflx_layer, la->pid, bootstrap_frame, message_size);
        int pops = 0;
        while(pops < la->pflx_layer->phonebook_size){
            int res = queue_pop(la->pflx_layer->upQueue, &incoming_data, &incoming_data_size);
            if (res != 0){
                dic_delete(num_to_index);
                return res;
            }

            if(incoming_data[0] == 0 && incoming_data[1] == la->pid){
                pops++;
            }
        }
    }

    // SORT AND STORE
    memcpy(la->index_to_proposal_int_translation, 
           &bootstrap_frame[2], 
           sizeof(int) * la->ds);
    
    // Sort the translation array
    qsort(la->index_to_proposal_int_translation, 
          la->ds, 
          sizeof(int), 
          compare_ints);
    
    la->proposal_to_index_translation = num_to_index;

    // Rebuild the dictionary with sorted indices
    for(int i = 0; i < la->ds; i++){
        char key[32];
        snprintf(key, sizeof(key), "%d", la->index_to_proposal_int_translation[i]);
        dic_add(la->proposal_to_index_translation, key, strlen(key));
        *la->proposal_to_index_translation->value = i;
    }

    
    // load first batch of proposals
    int prop_load_res = proposal_load_next(la, BUFFERED_PROPOSALS);
    if (prop_load_res != 0){
        return prop_load_res;
    }

    return 0;
}

la_proposal* la_proposal_copy(la_proposal*){
    return NULL;
}

void* la_to_frame(la_proposal){
    return NULL;
}

la_proposal* frame_to_la(void*){
    return NULL;
}

la_proposal* la_proposal_init(ba*, void*){
    return NULL;
}

int la_proposal_destroy(la_proposal* la_prop){
    return 1;
}