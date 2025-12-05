#include"node.h"
#include"pflx.h"
#include"fifo.h"
#include"udp.h"
#include"parser.h"
#include<stdlib.h>
#include<time.h>
#include<string.h>
#include<unistd.h>
#include<arpa/inet.h>
#include<sys/socket.h>
#include<sys/select.h>
#include<signal.h>
#include<errno.h>
#include<stdio.h>


const int DEBUG = 0;
const size_t BUFFER_SIZE = 256;
const double FLUSH_INTERVAL = 0.1; // seconds between logger flush
const size_t MAX_DOWN_QUEUE_SIZE = 100; 

// Global node pointer for signal handler
static Node* g_current_node = NULL;

// interrupt handler
static void stop(int sig) {
    signal(SIGTERM, SIG_DFL);
    signal(SIGINT, SIG_DFL);
    
    printf("Immediately stopping network packet processing.\n");
    printf("Writing output.\n");
    
    // Flush logger if node exists
    char sanity[256];
    if (g_current_node && g_current_node->logger) {
        if (DEBUG == 1){
            snprintf(sanity, sizeof(sanity), "logger=%d", g_current_node->logger->debug);
            logger_add(g_current_node->logger, sanity);
        }
        logger_flush(g_current_node->logger);
    }

    // stop network
    pflx_stop(g_current_node->socket->pflx_layer);
    fifo_stop(g_current_node->socket);
    
    exit(0);
}

// ---------- NODE FUNCTIONS ----------

int node_loop(Node *node) {
    // Set global node pointer for signal handler
    g_current_node = node;
    
    // initialization
    void* buffer = malloc(BUFFER_SIZE);
    if(!buffer){
        fprintf(stderr, "Failed to allocate buffer\n");
        return -1;
    }
    char logBuffer[256]; size_t lenRecvMessage = 0; 
    size_t messagesSent = 0;
    char* res;
    int lenMessage;
    time_t last_flush = time(NULL);
    // reciever expects n-1 senders
    size_t expectedMsgs = node->socket->pflx_layer->phonebook_size * node->nOfMessages;

    // interrupt signals
    signal(SIGTERM, stop);
    signal(SIGINT, stop);

    //start the network interface
    fifo_start(node->socket);
    pflx_start(node->socket->pflx_layer);
    //////////////////////////////////////////
    if(node->logger->debug == 1){
        logger_add(node->logger, "BEGIN");
        snprintf(logBuffer, sizeof(logBuffer), "My process ID: %zu", node->processId);
        logger_add(node->logger, logBuffer);
        snprintf(logBuffer, sizeof(logBuffer), "port number: %d", ntohs(node->socket->pflx_layer->udpSocket->addr.sin_port));
        logger_add(node->logger, logBuffer);
        logger_flush(node->logger);
    }
    while (1) {
        // reciever behaviour
        if(DEBUG == 1){
            logger_add(node->logger, "Receiver waiting for message...");
            logger_flush(node->logger);
        }
        int res = fifo_recv(node->socket, buffer, &lenRecvMessage);
        if (res != 0){
            if (res == ETIMEDOUT){
                if(DEBUG == 1 && buffer != NULL){
                    snprintf(logBuffer, sizeof(logBuffer), "fifo recv timed out!");
                    logger_add(node->logger, logBuffer);
                    logger_flush(node->logger);
                }
            } else{
                if(DEBUG == 1 && buffer != NULL){
                    snprintf(logBuffer, sizeof(logBuffer), "fifo recv returned an error, panic");
                    logger_add(node->logger, logBuffer);
                    logger_flush(node->logger);
                }
                return res;  
            }
        } else {
            // Only process message if recv succeeded
            if(DEBUG == 1 && buffer != NULL){
                snprintf(logBuffer, sizeof(logBuffer), "recieved: '%s', len msg: '%zu'", (char* )buffer, lenRecvMessage);
                logger_add(node->logger, logBuffer);
                logger_flush(node->logger);
            }
            if(buffer != NULL){
                size_t senderId, msgId;
                snprintf(logBuffer, sizeof(logBuffer), "d %s", (char* )buffer);
                logger_add(node->logger, logBuffer);
                if (sscanf((char*)buffer, "%zu %zu", &senderId, &msgId) == 2  
                   && senderId >= node->processId){
                    node->nextMessageId++;
                } else{
                    expectedMsgs--;
                }
            }
        }
        if (pflx_network_status(node->socket->pflx_layer) == 0 || expectedMsgs == 0){
            break;
        }

        // sender behaviour
        // should we add some more messages?
        if (messagesSent < (node->nextMessageId - 1)){
            // should never happen
            return 1;
        }
        // Calculate how many messages are currently in the queue
        size_t messages_in_queue = messagesSent - (node->nextMessageId - 1);

        while (messages_in_queue < MAX_DOWN_QUEUE_SIZE && messagesSent < node->nOfMessages) {
            snprintf((char*)buffer, BUFFER_SIZE, "%zu %zu", node->processId, messagesSent + 1);

            size_t msg_len = strlen((char*)buffer) + 1;
            lenMessage = fifo_send(node->socket, buffer, msg_len, node->processId);
            if(lenMessage < 0){
                if(DEBUG == 1){
                    snprintf(logBuffer, sizeof(logBuffer), "error in fifoSend! returned %d", lenMessage);
                    logger_add(node->logger, logBuffer);
                    logger_flush(node->logger);
                }
                return lenMessage;
            }
            // log (has to deliver in order)
            snprintf(logBuffer, sizeof(logBuffer), "b %zu", messagesSent + 1);
            logger_add(node->logger, logBuffer);
            if(DEBUG == 1){
                snprintf(logBuffer, sizeof(logBuffer), "sent successfully (%d bytes)", lenMessage);
                logger_add(node->logger, logBuffer);
                logger_flush(node->logger);
            }
            messagesSent++;
            messages_in_queue++;
        }
        
        time_t now = time(NULL);

        // Periodically flush log
        if (difftime(now, last_flush) >= FLUSH_INTERVAL) {
            logger_flush(node->logger);
            last_flush = now;
        }
    }

    if(node->logger->debug == 1){
        logger_add(node->logger, "DONE");
        logger_flush(node->logger);
    }
    
    pflx_stop(node->socket->pflx_layer); // might destroy this before it can stop gracefully
    fifo_stop(node->socket);
    free(buffer);
    node_destroy(node);
    
    // Clear global pointer
    g_current_node = NULL;
    
    return 0;
}

Node* node_init(
    size_t processId, size_t nOfMessages, 
    const Host* phonebook, size_t phonebook_size, 
    const char *logfile) {
    Node* node = malloc(sizeof(Node));
    if (!node) return NULL;

    node->processId = processId;
    node->nextMessageId = 1;
    node->nOfMessages = nOfMessages;
    // phonebook is indexed at 0, process ids from 1
    pflx* pflx_socket = pflx_init(ntohs(phonebook[processId-1].port), phonebook, phonebook_size);
    node->socket = fifo_init(pflx_socket, processId);
    node->logger = logger_init(logfile, DEBUG);
    return node;
}

int node_destroy(Node *node) {
    logger_destroy(node->logger);
    fifo_destroy(node->socket);
    free(node);

    return 0;
}