#ifndef LA_H
#define LA_H

// external headers
#include<stdatomic.h>
#include<stdbool.h>
#include<pthread.h>

// internal
#include"pflx.h"
#include"queue.h"
#include"bst_set.h"
#include"ba.h"
#include"dict.h"

#define LA_UNIQUE_VALUES 1000

#define LA_FRAME_HEADER_SIZE 6
/*la_frame
int type_of_msg         4B
int origin_ID           4B
int target_ID           4B
int index               4B
int retransmit          4B
int size_proposal       4B
int* proposal           size_proposal * 4B
*/

typedef struct{
    unsigned int n_acks;
    bool active;
    unsigned int restransmits;
    int proposal_len;
    int proposed_data[LA_UNIQUE_VALUES];
}la_proposal;

/* handle to be put in downQueue 
type_of_msg = 1 -> proposal
            = 2 -> ack
            = 3 -> nack
*/
typedef struct{
    int type_of_msg;  
    int dest;
    int index;
    int retransmit;
}la_handle;

typedef struct{
    la_proposal prop;
    pthread_mutex_t prop_lock;
    int accepted_len;
    int accepted_values[LA_UNIQUE_VALUES];

}la_buffered_entry;

typedef struct{
    size_t pid;

    queue_t* upQueue;
    queue_t* downQueue;
    int network_busy;

    int proposals_len;
    int ds;
    int vs;

    char* config_path;    

    atomic_int next_tbd;
    int round; 
    la_buffered_entry* buffered_proposals;

    atomic_int shouldStop;

    pflx* pflx_layer;
}la;

int la_start(la* la);
int la_stop(la* la);
int la_send(la* la, void* handle);
int la_recv(la* la, void* message, size_t* messageSize);
int la_send_routine(la* la);
int la_recv_routine(la* la);
int la_deliver(la* la, la_proposal* msg);
la* la_init(pflx* pflx_layer, char* config_path, size_t pid);
int la_destroy(la*);

int proposal_load_next(la*, int);

int beb_with_pflx(pflx*, int, void*, size_t);
int la_bootstrap_from_config(la*, char*);

int la_msg_to_frame(la* la, la_handle* handle, int** frame);
int* frame_to_proposal_array(int* frame, int* len_of_proposal);
la_proposal* la_proposal_init();
void la_proposal_destroy(la_proposal* la_prop);

#endif