#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include<stddef.h>

typedef struct {
    uint32_t id;
    uint32_t ip;
    uint16_t port;
    char* ip_readable;
    char* port_readable;
} Host;

typedef struct Parser {
    int argc;
    char** argv;
    uint32_t my_id;
    Host* hosts;
    size_t hosts_count;
    char* output_path;
    char** config_paths;      // Array of config paths
    size_t config_paths_count; // Number of config paths
    size_t num_messages;
    size_t num_nodes;
    // Lattice Agreement config
    size_t num_proposals;
    size_t vs;
    size_t ds;
} Parser;

// Initialize parser with command line arguments
Parser* parser_create(int argc, char** argv);

// Parse the arguments and config files
int parser_parse(Parser* parser);

// Getters
uint32_t parser_get_id(const Parser* parser);
const Host* parser_get_hosts(const Parser* parser, size_t* count);
const char* parser_get_output_path(const Parser* parser);
const char* parser_get_config_path(const Parser* parser);
// Get config path for a specific process (1-indexed)
const char* parser_get_config_path_for_process(const Parser* parser, size_t process_id);
// Get the number of config paths
size_t parser_get_config_paths_count(const Parser* parser);
size_t parser_get_num_messages(const Parser* parser);

// Lattice Agreement getters
size_t parser_get_num_proposals(const Parser* parser);
size_t parser_get_vs(const Parser* parser);
size_t parser_get_ds(const Parser* parser);

// Cleanup
void parser_destroy(Parser* parser);

#ifdef __cplusplus
}
#endif