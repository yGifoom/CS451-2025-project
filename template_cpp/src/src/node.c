#define _POSIX_C_SOURCE      199309L
#include"node.h"
#include"pflx.h"
#include"la.h"
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
const size_t BUFFER_SIZE = 4096;
const double FLUSH_INTERVAL = 0.1; // seconds between logger flush

// Global node pointer for signal handler
static Node* g_current_node = NULL;

// interrupt handler
static void stop(int sig) {
    signal(SIGTERM, SIG_DFL);
    signal(SIGINT, SIG_DFL);
    
    printf("Immediately stopping network packet processing.\n");
    printf("Writing output.\n");
    
    // Flush logger if node exists
    if (g_current_node && g_current_node->logger) {
        logger_flush(g_current_node->logger);
    }

    // IMPORTANT: Gracefully stop network in correct order
    if (g_current_node && g_current_node->socket) {
        la_stop(g_current_node->socket);
        la_destroy(g_current_node->socket);
        g_current_node->socket = NULL;
    }
    
    exit(0);
}

// ---------- NODE FUNCTIONS ----------

int node_loop(Node *node) {
    // Set global node pointer for signal handler
    g_current_node = node;
    
    // initialization
    int* buffer = malloc(BUFFER_SIZE);
    if(!buffer){
        fprintf(stderr, "Failed to allocate buffer\n");
        return -1;
    }
    char logBuffer[4096];
    size_t deliveredCount = 0;
    time_t last_flush = time(NULL);

    // interrupt signals
    signal(SIGTERM, stop);
    signal(SIGINT, stop);

    // start the la layer (which starts pflx internally)
    la_start(node->socket);

    if(node->logger->debug == 1){
        logger_add(node->logger, "BEGIN");
        snprintf(logBuffer, sizeof(logBuffer), "My process ID: %zu", node->processId);
        logger_add(node->logger, logBuffer);
        logger_flush(node->logger);
    }

    while (1) {
        size_t setSize = 0;
        int res = la_recv(node->socket, buffer, &setSize);
        
        if (res != 0) {
            if (res == ETIMEDOUT) {
                // Timeout is normal, continue waiting
                time_t now = time(NULL);
                if (difftime(now, last_flush) >= FLUSH_INTERVAL) {
                    logger_flush(node->logger);
                    last_flush = now;
                }
                continue;
            }
            
            printf("%zu-NODE: la_recv error %d\n", node->processId, res);
            fflush(stdout);
            free(buffer);
            return res;
        }

        // Format output: set elements separated by spaces
        if (setSize > 0) {
            size_t numElements = setSize / sizeof(int);
            int offset = 0;
            
            for (size_t i = 0; i < numElements; i++) {
                if (i == 0) {
                    offset += snprintf(logBuffer + offset, sizeof(logBuffer) - (size_t)offset, "%d", buffer[i]);
                } else {
                    offset += snprintf(logBuffer + offset, sizeof(logBuffer) - (size_t)offset, " %d", buffer[i]);
                }
            }
            
            logger_add(node->logger, logBuffer);
            deliveredCount++;
            
            printf("%zu-NODE: delivered proposal %zu: %s\n", node->processId, deliveredCount, logBuffer);
            fflush(stdout);
        }

        time_t now = time(NULL);
        if (difftime(now, last_flush) >= FLUSH_INTERVAL) {
            logger_flush(node->logger);
            last_flush = now;
        }
    }

    if(node->logger->debug == 1){
        logger_add(node->logger, "DONE");
    }
    logger_flush(node->logger);
    
    printf("%zu-NODE: all proposals delivered, stopping la\n", node->processId);
    fflush(stdout);
    
    la_stop(node->socket);
    
    // Give threads time to exit cleanly
    struct timespec ts_1s = { .tv_sec = 1, .tv_nsec = 0 };
    nanosleep(&ts_1s, NULL);

    free(buffer);
    node_destroy(node);
    
    // Clear global pointer
    g_current_node = NULL;
    printf("NODE: exiting\n");
    fflush(stdout);
    return 0;
}

Node* node_init(
    size_t processId, size_t nOfMessages, 
    const Host* phonebook, size_t phonebook_size, 
    const char *logfile, const char* config_path,
    int ds, int vs) {
    Node* node = malloc(sizeof(Node));
    if (!node) return NULL;

    node->processId = processId;
    node->nextMessageId = 1;
    node->nOfMessages = nOfMessages;
    
    // phonebook is indexed at 0, process ids from 1
    pflx* pflx_socket = pflx_init(ntohs(phonebook[processId-1].port), phonebook, phonebook_size);
    if (!pflx_socket) {
        free(node);
        return NULL;
    }
    
    node->socket = la_init(pflx_socket, config_path, processId, (int)nOfMessages, ds, vs);
    if (!node->socket) {
        pflx_destroy(pflx_socket);
        free(node);
        return NULL;
    }
    
    node->logger = logger_init(logfile, DEBUG);
    if (!node->logger) {
        la_destroy(node->socket);
        free(node);
        return NULL;
    }
    
    return node;
}

int node_destroy(Node *node) {
    if (!node) return -1;
    
    if (node->logger) {
        logger_destroy(node->logger);
    }
    if (node->socket) {
        la_destroy(node->socket);
    }
    free(node);

    return 0;
}