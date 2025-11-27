#ifndef NODE_H
#define NODE_H

#include<udp.h>
#include<logger.h>
#include<fifo.h>

typedef struct{
    size_t processId;
    size_t nextMessageId;
    size_t nOfMessages;
    fifo* socket;

    Logger* logger;
}Node;

// main loop of a node
int node_loop(Node* node);

// construct node
Node* node_init(size_t id, size_t numOfMessages, const Host* phonebook, size_t phonebook_size, const char* logfile);

// desctructor node
int node_destroy(Node* node);

#endif