#include "parser.h"
#include "utils.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <ctype.h>

static int is_positive_number(const char* str) {
    if (!str || *str == '\0') return 0;
    while (*str) {
        if (*str < '0' || *str > '9') return 0;
        str++;
    }
    return 1;
}

static int parse_hosts_file(Parser* parser, const char* hosts_path) {
    FILE* file = fopen(hosts_path, "r");
    if (!file) {
        fprintf(stderr, "`%s` does not exist.\n", hosts_path);
        return -1;
    }
    
    Host* temp_hosts = NULL;
    size_t capacity = 10;
    size_t count = 0;
    
    temp_hosts = (Host*)malloc(sizeof(Host) * capacity);
    if (!temp_hosts) {
        fclose(file);
        return -1;
    }
    
    char line[256];
    int line_num = 0;
    
    while (fgets(line, sizeof(line), file)) {
        line_num++;
        
        // Trim whitespace
        char* start = line;
        while (*start && isspace(*start)) start++;
        if (*start == '\0') continue;
        
        uint32_t id;
        char ip_str[256];
        unsigned short port;
        
        if (sscanf(start, "%u %255s %hu", &id, ip_str, &port) != 3) {
            fprintf(stderr, "Parsing for `%s` failed at line %d\n", hosts_path, line_num);
            free(temp_hosts);
            fclose(file);
            return -1;
        }
        
        if (count >= capacity) {
            capacity *= 2;
            Host* new_hosts = (Host*)realloc(temp_hosts, sizeof(Host) * capacity);
            if (!new_hosts) {
                free(temp_hosts);
                fclose(file);
                return -1;
            }
            temp_hosts = new_hosts;
        }
        temp_hosts[count].id = id;
        temp_hosts[count].ip = inet_addr(ip_str);
        temp_hosts[count].port = htons(port);
        temp_hosts[count].ip_readable = my_strdup(ip_str);
        
        // Convert port to string
        char port_str[16];
        snprintf(port_str, sizeof(port_str), "%hu", port);
        temp_hosts[count].port_readable = my_strdup(port_str);
        
        count++;
    }
    
    fclose(file);
    
    if (count < 2) {
        fprintf(stderr, "`%s` must contain at least two hosts\n", hosts_path);
        free(temp_hosts);
        return -1;
    }
    
    // Validate IDs are from 1 to count
    for (size_t i = 0; i < count; i++) {
        if (temp_hosts[i].id < 1 || temp_hosts[i].id > (uint32_t)count) {
            fprintf(stderr, "In `%s` IDs of processes have to start from 1 and be compact\n", hosts_path);
            free(temp_hosts);
            return -1;
        }
    }
    
    parser->hosts = temp_hosts;
    parser->hosts_count = count;
    return 0;
}

Parser* parser_create(int argc, char** argv) {
    Parser* parser = (Parser*)malloc(sizeof(Parser));
    if (!parser) return NULL;
    
    parser->argc = argc;
    parser->argv = argv;
    parser->my_id = 0;
    parser->hosts = NULL;
    parser->hosts_count = 0;
    parser->output_path = NULL;
    parser->config_path = NULL;
    parser->num_messages = 0;
    parser->num_nodes = 0;
    parser->num_proposals = 0;
    parser->vs = 0;
    parser->ds = 0;
    
    return parser;
}

int parser_parse(Parser* parser) {
    if (!parser) return -1;
    
    // Parse --id
    if (parser->argc < 3 || strcmp(parser->argv[1], "--id") != 0) {
        fprintf(stderr, "Usage: %s --id ID --hosts HOSTS --output OUTPUT [CONFIG]\n", parser->argv[0]);
        return -1;
    }
    
    if (!is_positive_number(parser->argv[2])) {
        fprintf(stderr, "Invalid ID\n");
        return -1;
    }
    
    parser->my_id = (uint32_t)strtoul(parser->argv[2], NULL, 10);
    
    // Parse --hosts
    if (parser->argc < 5 || strcmp(parser->argv[3], "--hosts") != 0) {
        fprintf(stderr, "Usage: %s --id ID --hosts HOSTS --output OUTPUT [CONFIG]\n", parser->argv[0]);
        return -1;
    }
    
    const char* hosts_path = parser->argv[4];
    
    // Parse --output
    if (parser->argc < 7 || strcmp(parser->argv[5], "--output") != 0) {
        fprintf(stderr, "Usage: %s --id ID --hosts HOSTS --output OUTPUT [CONFIG]\n", parser->argv[0]);
        return -1;
    }
    
    parser->output_path = my_strdup(parser->argv[6]);
    if (!parser->output_path) return -1;
    
    // Parse config (optional)
    if (parser->argc >= 8) {
        parser->config_path = my_strdup(parser->argv[7]);
        if (!parser->config_path) return -1;
        
        // Parse config file
        FILE* config = fopen(parser->config_path, "r");
        if (config) {
            // Try to read three numbers first (Lattice Agreement format)
            int items_read = fscanf(config, "%zu %zu %zu", &parser->num_proposals, &parser->vs, &parser->ds);
            
            if (items_read == 3) {
                // Lattice Agreement format: num_proposals vs ds
                parser->num_messages = 0;
                parser->num_nodes = 0;
            } else {
                // Rewind and try Perfect Links format (2 numbers) or FIFO format (1 number)
                rewind(config);
                items_read = fscanf(config, "%zu %zu", &parser->num_messages, &parser->num_nodes);
                if (items_read < 1) {
                    fprintf(stderr, "Invalid config file format\n");
                    fclose(config);
                    return -1;
                }
                parser->num_proposals = 0;
                parser->vs = 0;
                parser->ds = 0;
            }
            fclose(config);
        }
    }
    
    // Parse hosts file
    if (parse_hosts_file(parser, hosts_path) != 0) {
        return -1;
    }
    
    return 0;
}

uint32_t parser_get_id(const Parser* parser) {
    return parser ? parser->my_id : 0;
}

const Host* parser_get_hosts(const Parser* parser, size_t* count) {
    if (!parser || !count) return NULL;
    *count = parser->hosts_count;
    return parser->hosts;
}

const char* parser_get_output_path(const Parser* parser) {
    return parser ? parser->output_path : NULL;
}

const char* parser_get_config_path(const Parser* parser) {
    return parser ? parser->config_path : NULL;
}

size_t parser_get_num_messages(const Parser* parser) {
    return parser ? parser->num_messages : 0;
}

size_t parser_get_num_proposals(const Parser* parser) {
    return parser ? parser->num_proposals : 0;
}

size_t parser_get_vs(const Parser* parser) {
    return parser ? parser->vs : 0;
}

size_t parser_get_ds(const Parser* parser) {
    return parser ? parser->ds : 0;
}

void parser_destroy(Parser* parser) {
    if (!parser) return;
    
    if (parser->hosts) {
        free(parser->hosts);
    }
    if (parser->output_path) {
        free(parser->output_path);
    }
    if (parser->config_path) {
        free(parser->config_path);
    }
    free(parser);
}