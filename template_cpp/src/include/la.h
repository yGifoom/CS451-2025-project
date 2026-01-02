#ifndef LA_H
#define LA_H

// external headers
#include<stdatomic.h>

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

    int* index_to_proposal_int_translation; // dual is used only in bootstrap 
    la_proposal* proposals;
    ba* accepted_values;
    int ds;    

    size_t next_id_tbd;
    bst_set* id_tbd;

    atomic_int shouldStop;

    pflx* pflx_layer;
    pflx* beb_layer;
}la;

/*la_frame
bool ack_or_message     1B
bool ack_or_nack        1B
int origin_ID           4B
int index               4B
int retransmit          4B
ba(unrolled) proposal   X * 8B

wh ds <= 64 ; 22B per message
where X is ceiling(ds/64)
*/
typedef struct{
    ba proposed_data;
    unsigned int n_acks;
    unsigned int restransmits;
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
la* la_init(pflx* pflx, size_t pid);
int la_destroy(la*);

int beb_with_pflx(pflx*, void*);
int la_bootstrap_from_config(la*, char*);

la_proposal* la_proposal_copy(la_proposal*);
void* la_to_frame(la_proposal);
la_proposal* frame_to_la(void*);
la_proposal* la_proposal_init(ba*, void*);
int la_proposal_destroy(la_proposal* la_prop);

#endif