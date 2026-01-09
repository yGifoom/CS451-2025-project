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
static const int BUFFERSIZE_OUTGOING = 512 * sizeof(int) * 2;

/*la_frame
int type_of_msg         4B
int origin_ID           4B
int target_ID           4B
int ID               4B
int retransmit          4B
int size_proposal       4B
int* proposal           size_proposal * 4B
*/

#define LA_FRAME_HEADER_SIZE 6
#define LA_PROPOSAL_TYPE 1
#define LA_ACK_TYPE 2
#define LA_NACK_TYPE 3

#define LA_TYPE_OF_MSG_HID 0
#define LA_ORIGIN_ID_HID 1
#define LA_TARGET_ID_HID 2
#define LA_ID_HID 3
#define LA_RETRANSMIT_HID 4
#define LA_SIZE_PROPOSAL_HID 5


typedef struct{
    int n_acks;
    int n_nacks;
    bool active;
    int restransmits;
    int proposal_len;
    int proposed_data[LA_UNIQUE_VALUES];
}la_proposal;

typedef struct{
    int type_of_msg;  
    int dest;
    int ID;
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

    const char* config_path;    

    atomic_int next_tbd;
    atomic_int round; 
    pthread_mutex_t loading_buffer_mutex;
    la_buffered_entry* buffered_proposals;
    queue_t* future_proposals;

    atomic_int shouldStop;

    pflx* pflx_layer;
}la;

int la_start(la* la);
int la_stop(la* la);
int la_send(la* la, la_handle* handle);
int la_recv(la* la, int* buffer_set, size_t* size_set);
int la_send_routine(la* la);
int la_recv_routine(la* la);
int la_deliver(la* la, int ID);
la* la_init(pflx* pflx_layer, const char* config_path, size_t pid, int p, int ds, int vs);
int la_destroy(la*);

int proposal_load_next(la*, int);

int beb_with_pflx(pflx*, size_t, void*, size_t);
int la_bootstrap_from_config(la*, char*);

int la_msg_to_frame(la* la, la_handle* handle, int frame[BUFFERSIZE_OUTGOING]);
int* frame_to_proposal_array(int* frame, int* len_of_proposal);
la_proposal* la_proposal_init();
void la_proposal_destroy(la_proposal* la_prop);
la_handle* la_handle_init(int ID, int dest, int retransmit, int type_of_msg);

#endif