#define _POSIX_C_SOURCE      199309L
#include"logger.h"
#include"parser.h"
#include"udp.h"
#include"pflx.h"
#include"tests.h"
#include"node.h"
#include"utils.h"
#include"queue.h"
#include"bst_set.h"
#include"ba.h"
#include"fifo.h"
#include<stdlib.h>
#include<pthread.h>
#include<string.h>
#include<time.h>

// Forward declarations for thread functions
void* popper_thread(void* arg);
void* pusher_thread(void* arg);
void* broadcast_thread(void* arg);

// FIFO broadcast test thread data and function
typedef struct {
    fifo* f;
    size_t sender_id;
    size_t num_msgs;
} broadcast_data_t;

void* broadcast_thread(void* arg) {
    broadcast_data_t* bd = (broadcast_data_t*)arg;
    for (size_t i = 1; i <= bd->num_msgs; i++) {
        char message[64];
        snprintf(message, sizeof(message), "%zu %zu", bd->sender_id, i);
        fifo_send(bd->f, message, strlen(message)*sizeof(char) + 1, bd->sender_id);
    }
    return NULL;
}

// test for correctness of logger
void testLogger(char* res, Parser* parser){

    // Get output path from parser
    const char* output_path = parser_get_output_path(parser);
    if (!output_path) {
        strcpy(res, "fail");
        return;
    }
    
    // Initialize logger with output file
    FILE* output_file = fopen(output_path, "w");
    if (!output_file) {
        strcpy(res, "fail");
        return;
    }
    fclose(output_file);
    
    Logger* logger = logger_init(output_path, 0);
    if (!logger) {
        strcpy(res, "fail");
        return;
    }
    
    // Add 5 test logs
    logger_add(logger, "b 1");
    logger_add(logger, "b 2");
    logger_add(logger, "d 1 1");
    logger_add(logger, "d 2 2");
    logger_add(logger, "b 3");
    
    // Flush the logger
    logger_flush(logger);
    logger_destroy(logger);
    
    // Read back the output file and verify
    FILE* verify_file = fopen(output_path, "r");
    if (!verify_file) {
        strcpy(res, "fail");
        return;
    }
    
    const char* expected[] = {
        "b 1",
        "b 2",
        "d 1 1",
        "d 2 2",
        "b 3"
    };
    
    char line[256];
    int line_num = 0;
    int passed = 1;
    
    while (fgets(line, sizeof(line), verify_file) && line_num < 5) {
        // Remove newline
        line[strcspn(line, "\n")] = 0;
        
        if (strcmp(line, expected[line_num]) != 0) {
            passed = 0;
            break;
        }
        line_num++;
    }
    
    fclose(verify_file);
    
    // Check if we got exactly 5 lines
    if (line_num != 5) {
        passed = 0;
    }
    
    strcpy(res, passed ? "pass" : "fail");

    size_t hosts_count;
    const Host* hosts = parser_get_hosts(parser, &hosts_count);
    // Cleanup
    // cleanup output log files before final test
    char filename[256];
    for(size_t i = 1; i <= hosts_count; i++){
        snprintf(filename, sizeof(filename), "../example/output/%zu.output", i);
        FILE* f = fopen(filename, "w");
        if (f) fclose(f);
    }
    
    return;
}

void testUdp(char* res, Parser* parser){
    // Get host information from parser
    size_t hosts_count;
    const Host* hosts = parser_get_hosts(parser, &hosts_count);
    
    if (hosts_count < 2) {
        strcpy(res, "fail - need at least 2 hosts");
        return;
    }
    
    // Initialize two UDP sockets on different ports
    short unsigned int port_a = ntohs(hosts[0].port);
    short unsigned int port_b = ntohs(hosts[1].port);
    
    UDP* udp_a = udp_init(port_a);
    UDP* udp_b = udp_init(port_b);
    
    if (!udp_a || !udp_b) {
        strcpy(res, "fail - socket init");
        if (udp_a) udp_destroy(udp_a);
        if (udp_b) udp_destroy(udp_b);
        return;
    }
    
    // Test message from A to B
    const char* test_message = "Hello from A";
    char buffer_b[256];
    memset(buffer_b, 0, sizeof(buffer_b));
    
    // A sends to B
    udp_send(udp_a, hosts[1].ip_readable, port_b, test_message, sizeof(char)*strlen(test_message));
    
    // B receives from A
    ssize_t received_len = udp_recv(udp_b, buffer_b, sizeof(buffer_b));
    
    if (received_len < 0 || strcmp(buffer_b, test_message) != 0) {
        strcpy(res, "fail - A to B transmission");
        udp_destroy(udp_a);
        udp_destroy(udp_b);
        return;
    }
    
    // Test ACK from B to A
    const char* ack_message = "ACK from B";
    char buffer_a[256];
    memset(buffer_a, 0, sizeof(buffer_a));
    
    // B sends ACK to A
    udp_send(udp_b, hosts[0].ip_readable, port_a, ack_message, sizeof(char)*strlen(ack_message));
    
    // A receives ACK from B
    received_len = udp_recv(udp_a, buffer_a, sizeof(buffer_a));
    
    if (received_len < 0 || strcmp(buffer_a, ack_message) != 0) {
        strcpy(res, "fail - B to A ACK");
        udp_destroy(udp_a);
        udp_destroy(udp_b);
        return;
    }
    
    // Cleanup
    udp_destroy(udp_a);
    udp_destroy(udp_b);
    
    strcpy(res, "pass");
}

void testPflx(char* res, Parser* parser){
    // Get host information from parser
    size_t hosts_count;
    const Host* hosts = parser_get_hosts(parser, &hosts_count);
    if (hosts_count < 2) {
        strcpy(res, "fail - need at least 2 hosts");
        return;
    }

    const size_t NUM_MESSAGES = parser_get_num_messages(parser);
    if (NUM_MESSAGES == 0 || NUM_MESSAGES > 100000) {
        strcpy(res, "fail - invalid message count");
        return;
    }

    // Init one pflx per host
    pflx** nodes = calloc(hosts_count, sizeof(pflx*));
    if (!nodes) { strcpy(res, "fail - alloc nodes"); return; }

    int init_ok = 1;
    for (size_t i = 0; i < hosts_count; i++) {
        short unsigned int port = ntohs(hosts[i].port);
        nodes[i] = pflx_init(port, hosts, hosts_count);
        if (!nodes[i]) { init_ok = 0; break; }
    }
    if (!init_ok) {
        for (size_t i = 0; i < hosts_count; i++) if (nodes[i]) pflx_destroy(nodes[i]);
        free(nodes);
        strcpy(res, "fail - pflx init");
        return;
    }

    // Start all nodes
    for (size_t i = 0; i < hosts_count; i++) {
        if (pflx_start(nodes[i]) != 0) {
            for (size_t j = 0; j < hosts_count; j++) {
                if (nodes[j]) { pflx_stop(nodes[j]); pflx_destroy(nodes[j]); }
            }
            free(nodes);
            strcpy(res, "fail - pflx start");
            return;
        }
    }

    // Track sent per sender (1..NUM_MESSAGES)
    bst_set** sent_per_sender = calloc(hosts_count, sizeof(bst_set*));
    if (!sent_per_sender) {
        strcpy(res, "fail - alloc sent sets");
        goto cleanup_fail;
    }
    for (size_t s = 0; s < hosts_count; s++) {
        sent_per_sender[s] = bst_set_init();
        if (!sent_per_sender[s]) { strcpy(res, "fail - sent set init"); goto cleanup_fail; }
        for (size_t i = 1; i <= NUM_MESSAGES; i++) {
            bst_set_add(sent_per_sender[s], i, NULL, 0);
        }
    }

    // Track delivered per (receiver r, sender s)
    size_t N = hosts_count;
    bst_set** delivered = NULL;
    delivered = calloc(N * N, sizeof(bst_set*));
    if (!delivered) { strcpy(res, "fail - alloc delivered sets"); goto cleanup_fail; }
    for (size_t r = 0; r < N; r++) {
        for (size_t s = 0; s < N; s++) {
            if (r == s) continue;
            delivered[r*N + s] = bst_set_init();
            if (!delivered[r*N + s]) { strcpy(res, "fail - delivered set init"); goto cleanup_fail; }
        }
    }

    // Send: each sender to every other target
    for (size_t s = 0; s < N; s++) {
        for (size_t t = 0; t < N; t++) {
            if (t == s) continue;
            for (size_t i = 1; i <= NUM_MESSAGES; i++) {
                char msg[64];
                snprintf(msg, sizeof(msg), "%zu %zu", s + 1, i);
                if (pflx_send(nodes[s], msg, strlen(msg) * sizeof(char) + 1, s + 1, t + 1) != 0) {
                    strcpy(res, "fail - pflx send");
                    goto cleanup_fail;
                }
            }
        }
    }

    // Receive all expected deliveries
    size_t expected_total = (2 * (N-1) * NUM_MESSAGES) * N;
    size_t total_delivered = 0;
    size_t max_iterations = expected_total * 20 + 1000; // generous cap
    size_t iter = 0;

    while (total_delivered < expected_total && iter < max_iterations) {
        int progressed = 0;

        for (size_t r = 0; r < N; r++) {
            // Only try recv if there is something to avoid 1s timeout
            while (queue_size(nodes[r]->upQueue) > 0) {
                char buf[256] = {0};
                size_t len = 0;
                if (pflx_recv(nodes[r], buf, &len) != 0) {
                    break; // timed out or error; move to next node
                }
                size_t sender_parsed = 0, msg_id = 0;
                if (sscanf(buf, "%zu %zu", &sender_parsed, &msg_id) != 2) {
                    strcpy(res, "fail - malformed delivered payload");
                    goto cleanup_fail;
                }
                if (sender_parsed < 1 || sender_parsed > N || msg_id < 1 || msg_id > NUM_MESSAGES) {
                    strcpy(res, "fail - out of range payload");
                    goto cleanup_fail;
                }
                size_t s = sender_parsed - 1;
                // Must have been sent by that sender
                void* tmp = NULL; size_t tmpsz = 0;
                if (bst_set_lookup(sent_per_sender[s], msg_id, &tmp, &tmpsz) == 0) {
                    strcpy(res, "fail - delivered message never sent");
                    goto cleanup_fail;
                }
                // No duplicates per (receiver, sender)
                if (bst_set_lookup(delivered[r*N + s], msg_id, &tmp, &tmpsz) == 1) {
                    strcpy(res, "fail - duplicate delivery");
                    goto cleanup_fail;
                }
                bst_set_add(delivered[r*N + s], msg_id, NULL, 0);
                total_delivered++;
                progressed = 1;
            }
        }

        iter++;
        if (!progressed) {
            struct timespec ts_1ms = { .tv_sec = 0, .tv_nsec = 1000000 };
            nanosleep(&ts_1ms, NULL);
        }
    }

    // Verify counts
    if (total_delivered != expected_total) {
        sprintf(res, "fail - %zu messages were delivered, but expected %zu", total_delivered, expected_total);
        goto cleanup_fail;
    }
    for (size_t r = 0; r < N; r++) {
        for (size_t s = 0; s < N; s++) {
            if (r == s) continue;
            if (!delivered[r*N + s] || delivered[r*N + s]->size != NUM_MESSAGES) {
                strcpy(res, "fail - per-pair delivery count mismatch");
                goto cleanup_fail;
            }
        }
    }

    // Give some time for ACKs to settle
    {
        struct timespec ts_100ms = { .tv_sec = 0, .tv_nsec = 100000000 };
        nanosleep(&ts_100ms, NULL);
    }

    // Verify that nextId tbd is NUMMESSAGES + 1 except for self
    for (size_t i = 0; i < N; i++) {
        for (size_t j = 0; j < N; j++) {
            size_t id_next_tbd = nodes[i]->next_id_tbd[j].value;
            if (i == j){
                if (id_next_tbd != NUM_MESSAGES * (hosts_count - 1) + 1) {
                    sprintf(res, "fail - id_next_tbd of %zu for self wrong, holds value %zu ",  i+1, id_next_tbd);
                    goto cleanup_fail; 
                } else{
                    printf("PFLX TEST- id_next_tbd of %zu for self correct\n", i+1);
                    continue;
                }
            }

            if (id_next_tbd != NUM_MESSAGES + 1) {
                sprintf(res, "fail - id_next_tbd of %zu for %zu wrong, holds value %zu ",  i+1, j+1, id_next_tbd);
                goto cleanup_fail;
            } else{
                printf("PFLX TEST- id_next_tbd of %zu for %zu correct\n", i+1, j+1);
            }
        }
    }

    // Verify id_delivered empty on all nodes
    for (size_t i = 0; i < N; i++) {
        for (size_t j = 0; j < N; j++) {
            size_t id_del_len = nodes[i]->id_delivered[j]->size;
            if (id_del_len != 0) {
                sprintf(res, "fail - id_delivered not empty, has size %zu ", id_del_len);
                goto cleanup_fail;
            }
        }
    }

    // Success: cleanup and return pass
    for (size_t i = 0; i < hosts_count; i++) { pflx_stop(nodes[i]); }
    for (size_t i = 0; i < hosts_count; i++) { pflx_destroy(nodes[i]); }
    free(nodes);
    if (sent_per_sender) {
        for (size_t s = 0; s < hosts_count; s++) if (sent_per_sender[s]) bst_set_destroy(sent_per_sender[s]);
        free(sent_per_sender);
    }
    if (delivered) {
        for (size_t idx = 0; idx < N*N; idx++) if (delivered[idx]) bst_set_destroy(delivered[idx]);
        free(delivered);
    }
    strcpy(res, "pass");
    return;

cleanup_fail:
    if (nodes) {
        for (size_t i = 0; i < hosts_count; i++) {
            if (nodes[i]) { pflx_stop(nodes[i]); pflx_destroy(nodes[i]); }
        }
        free(nodes);
    }
    if (sent_per_sender) {
        for (size_t s = 0; s < hosts_count; s++) if (sent_per_sender[s]) bst_set_destroy(sent_per_sender[s]);
        free(sent_per_sender);
    }
    if (delivered) {
        //for (size_t idx = 0; idx < hosts_count*hosts_count; idx++) if (delivered[idx]) bst_set_destroy(delivered[idx]);
        //free(delivered);
    }
    // res already set above
}

void testNodeSeq(char* res, Parser* parser) {
    // Get host information from parser
    size_t hosts_count;
    const Host* hosts = parser_get_hosts(parser, &hosts_count);
    
    if (hosts_count != 2) {
        strcpy(res, "fail - need 2 hosts");
        return;
    }
    
    const size_t NUM_MESSAGES = parser_get_num_messages(parser);
    if (NUM_MESSAGES == 0) {
        strcpy(res, "fail - no messages in config");
        return;
    }
    
    const size_t nodeId = parser_get_id(parser);

    // Create temporary log files
    const char* node_log = parser_get_output_path(parser);
    printf("initializing node....\n");
    
    // Initialize nodes using node_init
    Node* node = node_init(nodeId, NUM_MESSAGES, hosts, hosts_count, node_log);
    if (!node) {
        strcpy(res, "fail - node init");
        return;
    }
    printf("node initialized!\nstarting loop\n");
    

    // this will block indefinetly, or until it crashes
    node_loop(node);
    
    printf("loop finished! now checking results....\n");
    
    // Verify sender output
    FILE* node_file = fopen(node_log, "r");
    if (!node_file) {
        strcpy(res, "fail - cannot open node log");
        return;
    }
    
    char line[256];
    if(nodeId < hosts_count){
        for (size_t i = 1; i <= NUM_MESSAGES; i++) {
            if (!fgets(line, sizeof(line), node_file)) {
                strcpy(res, "fail - sender missing lines");
                fclose(node_file);
                return;
            }
            line[strcspn(line, "\n")] = 0;
            
            char expected[256];
            snprintf(expected, sizeof(expected), "b %zu", i);
            if (strcmp(line, expected) != 0) {
                strcpy(res, "fail - sender wrong format");
                fclose(node_file);
                return;
            }
        }
    }
    else{
        for (size_t i = 1; i <= NUM_MESSAGES; i++) {
            if (!fgets(line, sizeof(line), node_file)) {
                strcpy(res, "fail - reciever missing lines");
                fclose(node_file);
                return;
            }
            line[strcspn(line, "\n")] = 0;
            
            char expected[256];
            snprintf(expected, sizeof(expected), "d 1 %zu", i);
            if (strcmp(line, expected) != 0) {
                strcpy(res, "fail - reciever wrong format");
                fclose(node_file);
                return;
            }
        }
    }

    // Cleanup
    fclose(node_file);
    remove(node_log);

    strcpy(res, "pass");
}

typedef struct {
    queue_t *q;
    unsigned int max_items;
} thread_data_t;

void* pusher_thread(void* arg) {
    thread_data_t* td = (thread_data_t*)arg;
    
    for (unsigned int i = 1; i <= td->max_items; i++) {
        unsigned int* value = malloc(sizeof(int));
        *value = i;
        queue_push(td->q, value, sizeof(int));
    }
    
    return NULL;
}

void* popper_thread(void* arg) {
    thread_data_t* td = (thread_data_t*)arg;
    int* results = malloc(td->max_items * sizeof(int));
    
    for (unsigned int i = 0; i < td->max_items; i++) {
        void* data;
        size_t dataSize;
        queue_pop(td->q, &data, &dataSize);
        results[i] = *(int*)data;
        free(data);
    }
    
    return results;
}

void testQueue(char* res, Parser* parser) {
    // Test 1: Sequential operations
    queue_t* q = queue_init();
    if (q == NULL) {
        strcpy(res, "fail - queue init");
        return;
    }
    
    // Test empty queue size
    if (queue_size(q) != 0) {
        strcpy(res, "fail - initial size not 0");
        queue_destroy(q);
        return;
    }
    
    // Test push and size
    int* val1 = malloc(sizeof(int));
    int* val2 = malloc(sizeof(int));
    int* val3 = malloc(sizeof(int));
    *val1 = 10;
    *val2 = 20;
    *val3 = 30;
    
    queue_push(q, val1, sizeof(int));
    if (queue_size(q) != 1) {
        strcpy(res, "fail - size after 1 push");
        queue_destroy(q);
        return;
    }
    
    queue_push(q, val2, sizeof(int));
    queue_push(q, val3, sizeof(int));
    if (queue_size(q) != 3) {
        strcpy(res, "fail - size after 3 pushes");
        queue_destroy(q);
        return;
    }
    
    // Test FIFO order (push at head, pop from tail)
    void* data;
    size_t dataSize;
    
    queue_pop(q, &data, &dataSize);
    if (*(int*)data != 10) {
        strcpy(res, "fail - wrong FIFO order (first)");
        free(data);
        queue_destroy(q);
        return;
    }
    free(data);
    
    queue_pop(q, &data, &dataSize);
    if (*(int*)data != 20) {
        strcpy(res, "fail - wrong FIFO order (second)");
        free(data);
        queue_destroy(q);
        return;
    }
    free(data);
    
    queue_pop(q, &data, &dataSize);
    if (*(int*)data != 30) {
        strcpy(res, "fail - wrong FIFO order (third)");
        free(data);
        queue_destroy(q);
        return;
    }
    free(data);
    
    if (queue_size(q) != 0) {
        strcpy(res, "fail - size after pops not 0");
        queue_destroy(q);
        return;
    }
    
    queue_destroy(q);
    
    // Test 2: Concurrent operations
    q = queue_init();
    if (q == NULL) {
        strcpy(res, "fail - queue init for concurrent test");
        return;
    }
    
    const int NUM_ITEMS = 10;
    thread_data_t td = {q, NUM_ITEMS};
    
    pthread_t pusher, popper;
    
    // Start both threads
    if (pthread_create(&popper, NULL, popper_thread, &td) != 0) {
        strcpy(res, "fail - create popper thread");
        queue_destroy(q);
        return;
    }
    
    if (pthread_create(&pusher, NULL, pusher_thread, &td) != 0) {
        strcpy(res, "fail - create pusher thread");
        queue_destroy(q);
        return;
    }
    
    // Wait for both threads
    pthread_join(pusher, NULL);
    
    void* popper_result;
    pthread_join(popper, &popper_result);
    
    // Verify results
    unsigned int* results = (unsigned int*)popper_result;
    for (unsigned int i = 0; i < NUM_ITEMS; i++) {
        if (results[i] != i + 1) {
            strcpy(res, "fail - concurrent order/duplication");
            free(results);
            queue_destroy(q);
            return;
        }
    }
    
    free(results);
    
    // Queue should be empty
    if (queue_size(q) != 0) {
        strcpy(res, "fail - queue not empty after concurrent test");
        queue_destroy(q);
        return;
    }
    
    queue_destroy(q);
    strcpy(res, "pass");
}

void* ba_add_thread(void* arg);
void* ba_rm_thread(void* arg);
void* ba_get_thread(void* arg);

typedef struct {
    ba* bit_array;
    size_t start_idx;
    size_t end_idx;
} ba_thread_data_t;

void* ba_add_thread(void* arg) {
    ba_thread_data_t* td = (ba_thread_data_t*)arg;
    for (size_t i = td->start_idx; i < td->end_idx; i++) {
        ba_add(td->bit_array, i);
    }
    return NULL;
}

void* ba_rm_thread(void* arg) {
    ba_thread_data_t* td = (ba_thread_data_t*)arg;
    for (size_t i = td->start_idx; i < td->end_idx; i++) {
        ba_rm(td->bit_array, i);
    }
    return NULL;
}

void* ba_get_thread(void* arg) {
    ba_thread_data_t* td = (ba_thread_data_t*)arg;
    int* results = malloc((td->end_idx - td->start_idx) * sizeof(int));
    for (size_t i = td->start_idx; i < td->end_idx; i++) {
        results[i - td->start_idx] = ba_get(td->bit_array, i);
    }
    return results;
}

void testBa(char* res, Parser* parser) {
    // Test 1: Initialization
    ba* bit_array = ba_init(100);
    if (bit_array == NULL) {
        strcpy(res, "fail - ba_init");
        return;
    }
    
    if (bit_array->length != 100) {
        strcpy(res, "fail - wrong length");
        ba_destroy(bit_array);
        return;
    }
    
    // Test 2: Initial state (all bits should be 0)
    if (ba_sum(bit_array) != 0) {
        strcpy(res, "fail - initial sum not 0");
        ba_destroy(bit_array);
        return;
    }
    
    for (size_t i = 0; i < 100; i++) {
        if (ba_get(bit_array, i) != 0) {
            strcpy(res, "fail - initial bit not 0");
            ba_destroy(bit_array);
            return;
        }
    }
    
    // Test 3: Add single bits
    if (ba_add(bit_array, 0) != 0) {
        strcpy(res, "fail - ba_add first bit");
        ba_destroy(bit_array);
        return;
    }
    
    if (ba_get(bit_array, 0) != 1) {
        strcpy(res, "fail - first bit not set");
        ba_destroy(bit_array);
        return;
    }
    
    if (ba_sum(bit_array) != 1) {
        strcpy(res, "fail - sum after first add");
        ba_destroy(bit_array);
        return;
    }
    
    // Test 4: Add multiple bits
    ba_add(bit_array, 10);
    ba_add(bit_array, 50);
    ba_add(bit_array, 99);
    
    if (ba_sum(bit_array) != 4) {
        strcpy(res, "fail - sum after multiple adds");
        ba_destroy(bit_array);
        return;
    }
    
    if (ba_get(bit_array, 10) != 1 || ba_get(bit_array, 50) != 1 || ba_get(bit_array, 99) != 1) {
        strcpy(res, "fail - multiple bits not set");
        ba_destroy(bit_array);
        return;
    }
    
    // Test 5: Remove bits
    if (ba_rm(bit_array, 10) != 0) {
        strcpy(res, "fail - ba_rm");
        ba_destroy(bit_array);
        return;
    }
    
    if (ba_get(bit_array, 10) != 0) {
        strcpy(res, "fail - bit not removed");
        ba_destroy(bit_array);
        return;
    }
    
    if (ba_sum(bit_array) != 3) {
        strcpy(res, "fail - sum after remove");
        ba_destroy(bit_array);
        return;
    }
    
    // Test 6: ba_where_1
    size_t* indexes_1 = NULL;
    size_t count_1 = 0;
    
    if (ba_where_1(bit_array, &indexes_1, &count_1) == 0 && count_1 == 0) {
        strcpy(res, "fail - ba_where_1");
        ba_destroy(bit_array);
        return;
    }
    
    if (count_1 != 3) {
        strcpy(res, "fail - ba_where_1 count");
        free(indexes_1);
        ba_destroy(bit_array);
        return;
    }
    
    // Verify indexes are correct (0, 50, 99)
    size_t expected_1[] = {0, 50, 99};
    for (size_t i = 0; i < count_1; i++) {
        if (indexes_1[i] != expected_1[i]) {
            strcpy(res, "fail - ba_where_1 indexes");
            free(indexes_1);
            ba_destroy(bit_array);
            return;
        }
    }
    free(indexes_1);
    
    // Test 7: ba_where_0
    size_t* indexes_0 = NULL;
    size_t count_0 = 0;
    
    if (ba_where_0(bit_array, &indexes_0, &count_0) == 0 && count_0 == 0) {
        strcpy(res, "fail - ba_where_0");
        ba_destroy(bit_array);
        return;
    }
    
    if (count_0 != 97) {
        strcpy(res, "fail - ba_where_0 count");
        free(indexes_0);
        ba_destroy(bit_array);
        return;
    }
    
    // Verify first few indexes are correct (skip 0, so 1, 2, 3...)
    for (size_t i = 1; i < 10 && i < count_0; i++) {
        if (indexes_0[i-1] != i) {
            strcpy(res, "fail - ba_where_0 indexes");
            free(indexes_0);
            ba_destroy(bit_array);
            return;
        }
    }
    free(indexes_0);
    
    // Test 8: Boundary tests - removed negative index test since size_t is unsigned
    if (ba_add(bit_array, 100) != -1) {
        strcpy(res, "fail - out of bounds not rejected");
        ba_destroy(bit_array);
        return;
    }
    
    if (ba_get(bit_array, 100) != -1) {
        strcpy(res, "fail - out of bounds get not rejected");
        ba_destroy(bit_array);
        return;
    }
    
    ba_destroy(bit_array);
    
    // Test 9: Test across block boundaries (64-bit blocks)
    bit_array = ba_init(200);
    if (bit_array == NULL) {
        strcpy(res, "fail - ba_init for boundary test");
        return;
    }
    
    // Set bits around block boundaries (63, 64, 127, 128)
    ba_add(bit_array, 63);
    ba_add(bit_array, 64);
    ba_add(bit_array, 127);
    ba_add(bit_array, 128);
    
    if (ba_sum(bit_array) != 4) {
        strcpy(res, "fail - boundary sum");
        ba_destroy(bit_array);
        return;
    }
    
    if (ba_get(bit_array, 63) != 1 || ba_get(bit_array, 64) != 1 ||
        ba_get(bit_array, 127) != 1 || ba_get(bit_array, 128) != 1) {
        strcpy(res, "fail - boundary bits");
        ba_destroy(bit_array);
        return;
    }
    
    ba_destroy(bit_array);
    
    // Test 10: Concurrent operations
    bit_array = ba_init(1000);
    if (bit_array == NULL) {
        strcpy(res, "fail - concurrent test init");
        return;
    }
    
    const int NUM_THREADS = 4;
    pthread_t threads[NUM_THREADS];
    ba_thread_data_t thread_data[NUM_THREADS];
    
    // Each thread adds bits to non-overlapping regions
    size_t chunk_size = 250;
    for (size_t i = 0; i < NUM_THREADS; i++) {
        thread_data[i].bit_array = bit_array;
        thread_data[i].start_idx = i * chunk_size;
        thread_data[i].end_idx = (i + 1) * chunk_size;
        pthread_create(&threads[i], NULL, ba_add_thread, &thread_data[i]);
    }
    
    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }
    
    // Verify all bits were set
    if (ba_sum(bit_array) != 1000) {
        strcpy(res, "fail - concurrent add sum");
        ba_destroy(bit_array);
        return;
    }
    
    for (size_t i = 0; i < 1000; i++) {
        if (ba_get(bit_array, i) != 1) {
            strcpy(res, "fail - concurrent add missing bit");
            ba_destroy(bit_array);
            return;
        }
    }
    
    // Test concurrent remove
    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_create(&threads[i], NULL, ba_rm_thread, &thread_data[i]);
    }
    
    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }
    
    if (ba_sum(bit_array) != 0) {
        strcpy(res, "fail - concurrent remove sum");
        ba_destroy(bit_array);
        return;
    }
    
    // Test concurrent read/write
    // Set first half of bits
    for (size_t i = 0; i < 500; i++) {
        ba_add(bit_array, i);
    }
    
    // Thread 1-2: read first half, Thread 3-4: write second half
    pthread_t read_threads[2];
    pthread_t write_threads[2];
    ba_thread_data_t read_data[2];
    ba_thread_data_t write_data[2];
    
    for (size_t i = 0; i < 2; i++) {
        read_data[i].bit_array = bit_array;
        read_data[i].start_idx = i * 250;
        read_data[i].end_idx = (i + 1) * 250;
        
        write_data[i].bit_array = bit_array;
        write_data[i].start_idx = 500 + i * 250;
        write_data[i].end_idx = 500 + (i + 1) * 250;
        
        pthread_create(&read_threads[i], NULL, ba_get_thread, &read_data[i]);
        pthread_create(&write_threads[i], NULL, ba_add_thread, &write_data[i]);
    }
    
    void* results[2];
    for (int i = 0; i < 2; i++) {
        pthread_join(read_threads[i], &results[i]);
        pthread_join(write_threads[i], NULL);
    }
    
    // Verify read results (first half should all be 1)
    for (int i = 0; i < 2; i++) {
        int* read_results = (int*)results[i];
        for (int j = 0; j < 250; j++) {
            if (read_results[j] != 1) {
                strcpy(res, "fail - concurrent read wrong value");
                free(read_results);
                ba_destroy(bit_array);
                return;
            }
        }
        free(read_results);
    }
    
    // Verify all bits are now set
    if (ba_sum(bit_array) != 1000) {
        strcpy(res, "fail - concurrent read/write sum");
        ba_destroy(bit_array);
        return;
    }
    
    ba_destroy(bit_array);
    strcpy(res, "pass");
}

void testFifo(char* res, Parser* parser) {
    strcpy(res, "pass");
    // Get host information from parser
    size_t hosts_count;
    const Host* hosts = parser_get_hosts(parser, &hosts_count);
    
    if (hosts_count < 2) {
        strcpy(res, "fail - need at least 2 hosts for FIFO test");
        return;
    }
    
    const size_t NUM_MESSAGES = parser_get_num_messages(parser);
    if (NUM_MESSAGES == 0 || NUM_MESSAGES > 100) {
        strcpy(res, "fail - invalid message count (1-100)");
        return;
    }
    
    // Dynamically allocate arrays for all hosts
    pflx** pflx_instances = calloc(hosts_count, sizeof(pflx*));
    fifo** fifo_instances = calloc(hosts_count, sizeof(fifo*));
    
    if (!pflx_instances || !fifo_instances) {
        strcpy(res, "fail - allocation");
        free(pflx_instances);
        free(fifo_instances);
        return;
    }
    
    // Test 1: Sequential test - single broadcaster
    // Initialize pflx instances for all processes
    for (size_t i = 0; i < hosts_count; i++) {
        short unsigned int port = ntohs(hosts[i].port);
        pflx_instances[i] = pflx_init(port, hosts, hosts_count);
        
        if (!pflx_instances[i]) {
            strcpy(res, "fail - pflx init");
            for (size_t j = 0; j < i; j++) pflx_destroy(pflx_instances[j]);
            free(pflx_instances);
            free(fifo_instances);
            return;
        }
    }

    // Initialize FIFO layers
    for (size_t i = 0; i < hosts_count; i++) {
        fifo_instances[i] = fifo_init(pflx_instances[i], i + 1);
        
        if (!fifo_instances[i]) {
            strcpy(res, "fail - fifo init");
            for (size_t j = 0; j < i; j++) fifo_destroy(fifo_instances[j]);
            for (size_t j = 0; j < hosts_count; j++) pflx_destroy(pflx_instances[j]);
            free(pflx_instances);
            free(fifo_instances);
            return;
        }
    }
    
    // Start all FIFO instances
    for (size_t i = 0; i < hosts_count; i++) {
        if (fifo_start(fifo_instances[i]) != 0) {
            strcpy(res, "fail - fifo start");
            for (size_t j = 0; j < hosts_count; j++) {
                fifo_stop(fifo_instances[j]);
                fifo_destroy(fifo_instances[j]);
            }
            free(pflx_instances);
            free(fifo_instances);
            return;
        }
    }
    
    // Process 0 (first process) broadcasts NUM_MESSAGES messages
    size_t sender_id = 1;
    for (size_t i = 1; i <= NUM_MESSAGES; i++) {
        char message[64];
        snprintf(message, sizeof(message), "%zu %zu", sender_id, i);
        
        if (fifo_send(fifo_instances[0], message, strlen(message)*sizeof(char) + 1, sender_id) != 0) {
            strcpy(res, "fail - fifo_send");
            for (size_t j = 0; j < hosts_count; j++) {
                fifo_stop(fifo_instances[j]);
                fifo_destroy(fifo_instances[j]);
            }
            free(pflx_instances);
            free(fifo_instances);
            return;
        }
    }
    
    // Collect deliveries at all other processes
    size_t** delivered = calloc(hosts_count, sizeof(size_t*));
    size_t* delivery_counts = calloc(hosts_count, sizeof(size_t));
    
    for (size_t i = 0; i < hosts_count; i++) {
        delivered[i] = calloc(NUM_MESSAGES, sizeof(size_t));
    }
    
    size_t max_iterations = NUM_MESSAGES * hosts_count * 2;
    size_t iterations = 0;
    size_t total_expected = NUM_MESSAGES * (hosts_count - 1);
    size_t total_delivered = 0;

    while (total_delivered < total_expected && iterations < max_iterations) {
        // Try to receive from all processes except sender
        for (size_t proc = 1; proc < hosts_count; proc++) {
            if (delivery_counts[proc] < NUM_MESSAGES) {
                char recv_buf[256] = {0};
                size_t recv_len = 0;
                
                if (fifo_recv(fifo_instances[proc], recv_buf, &recv_len) == 0) {
                    size_t recv_sender = 0, msg_id = 0;
                    if (sscanf(recv_buf, "%zu %zu", &recv_sender, &msg_id) == 2) {
                        delivered[proc][delivery_counts[proc]++] = msg_id;
                        total_delivered++;
                    }
                }
            }
        }
        
        iterations++;
        struct timespec ts_1ms = { .tv_sec = 0, .tv_nsec = 1000000 };
        nanosleep(&ts_1ms, NULL);
    }
    
    // Verify all messages delivered to all receivers
    for (size_t proc = 1; proc < hosts_count; proc++) {
        if (delivery_counts[proc] != NUM_MESSAGES) {
            strcpy(res, "fail - not all messages delivered");
            goto cleanup_seq;
        }
    }
    
    // Verify FIFO ordering - messages should be 1, 2, 3, ..., NUM_MESSAGES
    for (size_t proc = 1; proc < hosts_count; proc++) {
        for (size_t i = 0; i < NUM_MESSAGES; i++) {
            if (delivered[proc][i] != i + 1) {
                strcpy(res, "fail - FIFO ordering violated");
                goto cleanup_seq;
            }
        }
    }
    
    // Verify no duplicates
    for (size_t proc = 1; proc < hosts_count; proc++) {
        for (size_t i = 0; i < NUM_MESSAGES; i++) {
            for (size_t j = i + 1; j < NUM_MESSAGES; j++) {
                if (delivered[proc][i] == delivered[proc][j]) {
                    strcpy(res, "fail - duplicate delivery");
                    goto cleanup_seq;
                }
            }
        }
    }
    
cleanup_seq:
    for (size_t i = 0; i < hosts_count; i++) {
        fifo_stop(fifo_instances[i]);
        fifo_destroy(fifo_instances[i]);
        free(delivered[i]);
    }
    free(delivered);
    free(delivery_counts);
    
    if ((strcmp(res, "pass") != 0) && strlen(res) > 0) {
        free(pflx_instances);
        free(fifo_instances);
        printf("FIFO TEST: fail, %s\n", res);fflush(stdout);
        return;
    }
    
    // Test 2: Concurrent test - all processes broadcast
    for (size_t i = 0; i < hosts_count; i++) {
        short unsigned int port = ntohs(hosts[i].port);
        pflx_instances[i] = pflx_init(port, hosts, hosts_count);
        
        if (!pflx_instances[i]) {
            strcpy(res, "fail - pflx init (concurrent)");
            for (size_t j = 0; j < i; j++) pflx_destroy(pflx_instances[j]);
            free(pflx_instances);
            free(fifo_instances);
            printf("FIFO TEST: fail - pflx init (concurrent)\n");fflush(stdout);

            return;
        }
    }
    
    for (size_t i = 0; i < hosts_count; i++) {
        fifo_instances[i] = fifo_init(pflx_instances[i], i + 1);
        
        if (!fifo_instances[i]) {
            strcpy(res, "fail - fifo init (concurrent)");
            for (size_t j = 0; j < i; j++) fifo_destroy(fifo_instances[j]);
            for (size_t j = 0; j < hosts_count; j++) pflx_destroy(pflx_instances[j]);
            free(pflx_instances);
            free(fifo_instances);
            printf("FIFO TEST: fail - fifo init (concurrent)\n");fflush(stdout);
            
            return;
        }
    }
    
    for (size_t i = 0; i < hosts_count; i++) {
        fifo_start(fifo_instances[i]);
    }
    
    // All processes broadcast concurrently
    pthread_t* threads = calloc(hosts_count, sizeof(pthread_t));
    broadcast_data_t* broadcast_data = calloc(hosts_count, sizeof(broadcast_data_t));
    
    for (size_t i = 0; i < hosts_count; i++) {
        broadcast_data[i].f = fifo_instances[i];
        broadcast_data[i].sender_id = i + 1;
        broadcast_data[i].num_msgs = NUM_MESSAGES;
        pthread_create(&threads[i], NULL, broadcast_thread, &broadcast_data[i]);
    }
    
    for (size_t i = 0; i < hosts_count; i++) {
        pthread_join(threads[i], NULL);
    }
    
    free(threads);
    free(broadcast_data);
    
    // Collect all deliveries at each process
    // deliveries[process][sender][msg_idx]
    size_t*** deliveries = calloc(hosts_count, sizeof(size_t**));
    size_t** delivery_counts_2d = calloc(hosts_count, sizeof(size_t*));
    
    for (size_t i = 0; i < hosts_count; i++) {
        deliveries[i] = calloc(hosts_count, sizeof(size_t*));
        delivery_counts_2d[i] = calloc(hosts_count, sizeof(size_t));
        for (size_t j = 0; j < hosts_count; j++) {
            deliveries[i][j] = calloc(NUM_MESSAGES, sizeof(size_t));
        }
    }
    
    iterations = 0;
    max_iterations = NUM_MESSAGES * hosts_count * hosts_count * 2;
    total_expected = NUM_MESSAGES * hosts_count * hosts_count; // all senders * all receivers * NUM_MESSAGES
    total_delivered = 0;
    
    while (total_delivered < total_expected && iterations < max_iterations) {
        for (size_t proc = 0; proc < hosts_count; proc++) {
            char recv_buf[256] = {0};
            size_t recv_len = 0;
            
            if (fifo_recv(fifo_instances[proc], recv_buf, &recv_len) == 0) {
                size_t sender = 0, msg_id = 0;
                if (sscanf(recv_buf, "%zu %zu", &sender, &msg_id) == 2 && 
                    sender >= 1 && sender <= hosts_count && msg_id >= 1 && msg_id <= NUM_MESSAGES) {
                    
                    size_t sender_idx = sender - 1;
                    size_t count = delivery_counts_2d[proc][sender_idx];
                    
                    if (count < NUM_MESSAGES) {
                        deliveries[proc][sender_idx][count] = msg_id;
                        delivery_counts_2d[proc][sender_idx]++;
                        total_delivered++;
                    }
                }
            }
        }
        
        iterations++;
        struct timespec ts_1ms = { .tv_sec = 0, .tv_nsec = 1000000 };
        nanosleep(&ts_1ms, NULL);
    }
    
    // Verify all messages delivered to all processes
    for (size_t proc = 0; proc < hosts_count; proc++) {
        for (size_t sender = 0; sender < hosts_count; sender++) {
            if (delivery_counts_2d[proc][sender] != NUM_MESSAGES) {
                strcpy(res, "fail - concurrent: not all messages delivered");
                printf("FIFO TEST: concurrent failed\n"); fflush(stdout);
                goto cleanup_concurrent;
            }
        }
    }
    
    // Verify FIFO ordering per sender
    for (size_t proc = 0; proc < hosts_count; proc++) {
        for (size_t sender = 0; sender < hosts_count; sender++) {
            for (size_t i = 0; i < NUM_MESSAGES; i++) {
                if (deliveries[proc][sender][i] != i + 1) {
                    strcpy(res, "fail - concurrent: FIFO ordering violated");
                    printf("FIFO TEST: concurrent failed\n"); fflush(stdout);

                    goto cleanup_concurrent;
                }
            }
        }
    }
    
    strcpy(res, "pass");
    printf("FIFO TEST: passed\n"); fflush(stdout);
    
cleanup_concurrent:
    for (size_t i = 0; i < hosts_count; i++) {
        fifo_stop(fifo_instances[i]);
        fifo_destroy(fifo_instances[i]);
        for (size_t j = 0; j < hosts_count; j++) {
            free(deliveries[i][j]);
        }
        free(deliveries[i]);
        free(delivery_counts_2d[i]);
    }
    free(deliveries);
    free(delivery_counts_2d);
    free(pflx_instances);
    free(fifo_instances);
    printf("FIFO TEST: CLEANUP CONCURRENT FINISHED\n"); fflush(stdout);

    return;
}