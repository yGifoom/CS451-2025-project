#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "../include_tests/tests.h"
#include"node.h"

const int TESTING = 0;

void tests(char* res, Parser* parser){
    /*
    char* res = malloc(sizeof(char) * 4);
    testLogger(res, parser);
    printf("Test Logger: %s\n", res);
    free(res);

    char* udp_res = malloc(sizeof(char) * 50);
    testUdp(udp_res, parser);
    printf("Test UDP: %s\n", udp_res);
    free(udp_res);
    */
    /*
    char* queue_res = malloc(sizeof(char) * 50);
    testQueue(queue_res, parser);
    printf("Test Queue: %s\n", queue_res);
    free(queue_res);*/
    
    /*
    char* pflx_res = malloc(sizeof(char) * 50);
    testPflx(pflx_res, parser);
    printf("TEST Pflx: %s\n", pflx_res);
    free(pflx_res);*/
    
    /*
    char* bst_res = malloc(sizeof(char) * 50);
    testBstSet(bst_res, parser);
    printf("Test BST Set: %s\n", bst_res);
    free(bst_res);
    *//*
    char* fifo_res = malloc(sizeof(char) * 100);
    testFifo(fifo_res, parser);
    printf("TEST FIFO: %s\n", fifo_res);
    free(fifo_res);*/
    /*
    char* node_res = malloc(sizeof(char) * 50);
    testNodeSeq(node_res, parser);
    printf("Test NodeSeq: %s\n", node_res);
    free(node_res);*/
    
    /*
    char* ba_res = malloc(sizeof(char) * 50);
    testBa(ba_res, parser);
    printf("Test testBa: %s\n", ba_res);
    free(ba_res);*/

    /**/
    char* la_res = malloc(sizeof(char) * 50);
    testLa(la_res, parser);
    printf("Test testLa: %s\n", la_res);
    free(la_res);

    return;
}

int main(int argc, char** argv) {

    Parser* parser = parser_create(argc, argv);
    if (!parser) {
        fprintf(stderr, "Failed to create parser\n");
        return 1;
    }
    
    if (parser_parse(parser) != 0) {
        fprintf(stderr, "Failed to parse arguments\n");
        parser_destroy(parser);
        return 1;
    }
    
    printf("\n");
    
    printf("My PID: %d\n", getpid());
    printf("From a new terminal type `kill -SIGINT %d` or `kill -SIGTERM %d` to stop processing packets\n\n", 
           getpid(), getpid());
    
    printf("My ID: %u\n\n", parser_get_id(parser));
    
    printf("List of resolved hosts is:\n");
    printf("==========================\n");
    
    size_t hosts_count;
    const Host* hosts = parser_get_hosts(parser, &hosts_count);
    for (size_t i = 0; i < hosts_count; i++) {
        printf("%u\n", hosts[i].id);
        printf("Human-readable IP: %s\n", hosts[i].ip_readable);
        printf("Machine-readable IP: %u\n", hosts[i].ip);
        printf("Human-readable Port: %s\n", hosts[i].port_readable);
        printf("Machine-readable Port: %u\n", hosts[i].port);
        printf("\n");
    }
    printf("\n");
    
    printf("Path to output:\n");
    printf("===============\n");
    printf("%s\n\n", parser_get_output_path(parser));
    
    printf("Path to config:\n");
    printf("===============\n");
    printf("%s\n\n", parser_get_config_path(parser));
    
    printf("Doing some initialization...\n\n");
    printf("Broadcasting and delivering messages...\n\n");

    if(TESTING == 1){
        char* res = malloc(256);
        tests(res, parser);
        printf("run all tests\n");
        return 0;
    }
    else{
        
        // Get host information from parser
        size_t hosts_count;
        const Host* hosts = parser_get_hosts(parser, &hosts_count);
        
        const size_t NUM_PROPOSALS = parser_get_num_proposals(parser);
        if (NUM_PROPOSALS == 0) {
            printf("fail - no proposals in config\n");
            parser_destroy(parser);
            return 1;
        }
        
        const size_t nodeId = parser_get_id(parser);
        const char* config_path = parser_get_config_path_for_process(parser, nodeId);
        int ds = (int)parser_get_ds(parser);
        int vs = (int)parser_get_vs(parser);

        // Create temporary log files
        const char* node_log = parser_get_output_path(parser);
        printf("initializing node....\n");
        printf("NUM_PROPOSALS: %zu, ds: %d, vs: %d\n", NUM_PROPOSALS, ds, vs);
        
        // Initialize nodes using node_init
        Node* node = node_init(nodeId, NUM_PROPOSALS, hosts, hosts_count, node_log, config_path, ds, vs);
        if (!node) {
            printf("fail - failed to initialize node %zu\n", nodeId);
            parser_destroy(parser);
            return 1;
        }
        printf("node initialized!\nstarting loop\n");
        

        // this will block indefinetly, or until it crashes
        node_loop(node);

        printf("loop finished!\n");
        
    }

    printf("main has finished\n");

    return 0;
}