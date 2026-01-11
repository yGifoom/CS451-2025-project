#ifndef NODE_H
#define NODE_H

#include<udp.h>
#include<logger.h>
#include<la.h>

typedef struct{
    size_t processId;
    size_t nextMessageId;
    size_t nOfMessages;
    la* socket;

    Logger* logger;
}Node;

// main loop of a node
int node_loop(Node* node);

// construct node
Node* node_init(size_t id, size_t numOfMessages, const Host* phonebook, size_t phonebook_size, 
                const char* logfile, const char* config_path, int ds, int vs);

// destructor node
int node_destroy(Node* node);

#endif