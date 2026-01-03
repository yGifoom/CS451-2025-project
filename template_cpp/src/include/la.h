#ifndef LA_H
#define LA_H

// external headers
#include<stdatomic.h>
#include<stdbool.h>

// internal
#include"pflx.h"
#include"queue.h"
#include"bst_set.h"
#include"ba.h"
#include"dict.h"


typedef struct{
    size_t pid;

    queue_t* upQueue;
    queue_t* downQueue;
    int network_busy;

    int proposals_len;
    int ds;
    struct dictionary* proposal_to_index_translation;
    int* index_to_proposal_int_translation; // dual is used only in bootstrap 

    int ds;
    int vs;
    char* config_path;    

    int round; 
    la_buffered_entry* buffered_proposals;

    atomic_int shouldStop;

    pflx* pflx_layer;
}la;

typedef struct{
    la_proposal prop;
    ba accepted_values;
}la_buffered_entry;

/*la_frame
int type_of_msg         4B
int origin_ID           4B
int index               4B
int retransmit          4B
ba(unrolled) proposal   X * 8B

where X is ceiling(ds/64)
type_of_msg = 0 -> bootstrap
            = 1 -> proposal
            = 2 -> ack
            = 3 -> nack

wh ds <= 64 ; 24B per message
*/
typedef struct{
    ba proposed_data;
    unsigned int n_acks;
    unsigned int restransmits;
    bool active;
}la_proposal;

// handle to be put in downQueue 
typedef struct{
    int dest;
    int index;
    int retransmit;
}la_ack_handle;

// handle to be put in downQueue 
typedef int proposal_handle;


int la_start(la* la);
int la_stop(la* la);
int la_send(la* la, void* handle, size_t originID);
int la_recv(la* la, void* message, size_t* messageSize);
int la_send_routine(la* la);
int la_recv_routine(la* la);
int la_deliver(la* la, la_proposal* msg);
la* la_init(pflx* pflx_layer, char* config_path, size_t pid);
int la_destroy(la*);

int proposal_load_next(la*, int);

int beb_with_pflx(pflx*, int, void*, size_t);
int la_bootstrap_from_config(la*, char*);

la_proposal* la_proposal_copy(la_proposal*);
void* la_to_frame(la_proposal);
la_proposal* frame_to_la(void*);
la_proposal* la_proposal_init(ba*, void*);
int la_proposal_destroy(la_proposal* la_prop);

#endif