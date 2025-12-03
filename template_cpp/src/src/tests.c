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
#include<stdlib.h>
#include<pthread.h>
#include<string.h>
#include<time.h>

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
        strcpy(res, "fail - not all messages delivered");
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

    // Verify that nextId tbd is NUMMESSAGES + 1
    for (size_t i = 0; i < N; i++) {
        for (size_t j = 0; j < N; j++) {
            size_t id_next_tbd = nodes[i]->next_id_tbd[j].value;
            if (id_next_tbd != (hosts_count - 1) * NUM_MESSAGES + 1) {
                sprintf(res, "fail - id_next_tbd wrong, holds value %zu ", id_next_tbd);
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

void* popper_thread(void* arg);
void* pusher_thread(void* arg);

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

void* lookup_thread_func(void* arg);
void* add_thread_func(void* arg);

void* lookup_thread_func(void* arg) {
        bst_set* s = (bst_set*)arg;
        for (size_t i = 1; i <= 100; i++) {
            if (bst_set_lookup(s, i, NULL, NULL) != 1) {
                return (void*)1; // Fail
            }
        }
        return (void*)0; // Success
    }

void* add_thread_func(void* arg){
        bst_set* s = (bst_set*)arg;
        for (size_t i = 101; i <= 200; i++) {
            bst_set_add(s, i, NULL, 0);
        }
        return NULL;
    }
void testBstSet(char* res, Parser* parser) {
    // Test 1: Initialization
    bst_set* set = bst_set_init();
    if (set == NULL) {
        strcpy(res, "fail - bst_set init");
        return;
    }
    
    if (set->size != 0) {
        strcpy(res, "fail - initial size not 0");
        bst_set_destroy(set);
        return;
    }
    
    // Test 2: Add elements
    if (bst_set_add(set, 50, NULL, 0) != 0) {
        strcpy(res, "fail - add first element");
        bst_set_destroy(set);
        return;
    }
    
    if (set->size != 1) {
        strcpy(res, "fail - size after first add");
        bst_set_destroy(set);
        return;
    }
    
    // Add more elements to test balancing
    size_t keys[] = {30, 70, 20, 40, 60, 80, 10, 25, 35};
    for (size_t i = 0; i < 9; i++) {
        if (bst_set_add(set, keys[i], NULL, 0) != 0) {
            strcpy(res, "fail - add element");
            bst_set_destroy(set);
            return;
        }
    }
    
    if (set->size != 10) {
        strcpy(res, "fail - size after multiple adds");
        bst_set_destroy(set);
        return;
    }
    
    // Test 3: Add duplicate (should return 1)
    if (bst_set_add(set, 50, NULL, 0) != 1) {
        strcpy(res, "fail - duplicate not detected");
        bst_set_destroy(set);
        return;
    }
    
    if (set->size != 10) {
        strcpy(res, "fail - size changed after duplicate");
        bst_set_destroy(set);
        return;
    }
    
    // Test 4: Lookup existing elements
    if (bst_set_lookup(set, 50, NULL, NULL) != 1) {
        strcpy(res, "fail - lookup existing element");
        bst_set_destroy(set);
        return;
    }
    
    if (bst_set_lookup(set, 10, NULL, NULL) != 1) {
        strcpy(res, "fail - lookup min element");
        bst_set_destroy(set);
        return;
    }
    
    if (bst_set_lookup(set, 80, NULL, NULL) != 1) {
        strcpy(res, "fail - lookup max element");
        bst_set_destroy(set);
        return;
    }
    
    // Test 5: Lookup non-existing elements
    if (bst_set_lookup(set, 100, NULL, NULL) != 0) {
        strcpy(res, "fail - lookup non-existing");
        bst_set_destroy(set);
        return;
    }
    
    if (bst_set_lookup(set, 5, NULL, NULL) != 0) {
        strcpy(res, "fail - lookup below min");
        bst_set_destroy(set);
        return;
    }
    
    // Test 6: Delete elements
    if (bst_set_delete(set, 20) != 0) {
        strcpy(res, "fail - delete existing element");
        bst_set_destroy(set);
        return;
    }
    
    if (set->size != 9) {
        strcpy(res, "fail - size after delete");
        bst_set_destroy(set);
        return;
    }
    
    if (bst_set_lookup(set, 20, NULL, NULL) != 0) {
        strcpy(res, "fail - deleted element still found");
        bst_set_destroy(set);
        return;
    }
    
    // Test 7: Delete non-existing element
    if (bst_set_delete(set, 999) != -1) {
        strcpy(res, "fail - delete non-existing");
        bst_set_destroy(set);
        return;
    }
    
    // Test 8: Delete root and verify balancing
    if (bst_set_delete(set, 50) != 0) {
        strcpy(res, "fail - delete root");
        bst_set_destroy(set);
        return;
    }
    
    if (bst_set_lookup(set, 30, NULL, NULL) != 1 || bst_set_lookup(set, 70, NULL, NULL) != 1) {
        strcpy(res, "fail - tree corrupted after root delete");
        bst_set_destroy(set);
        return;
    }
    
    bst_set_destroy(set);
    
    // Test 9: Concurrent operations
    set = bst_set_init();
    if (set == NULL) {
        strcpy(res, "fail - concurrent test init");
        return;
    }
    
    // Add initial elements
    for (size_t i = 1; i <= 100; i++) {
        bst_set_add(set, i, NULL, 0);
    }
    
    // Thread-safe lookup while another thread is modifying
    pthread_t thread1, thread2;
    
    pthread_create(&thread1, NULL, add_thread_func, set);
    pthread_create(&thread2, NULL, lookup_thread_func, set);
    
    void* lookup_result;
    pthread_join(thread1, NULL);
    pthread_join(thread2, &lookup_result);
    
    if (lookup_result != (void*)0) {
        strcpy(res, "fail - concurrent lookup failed");
        bst_set_destroy(set);
        return;
    }
    
    if (set->size != 200) {
        strcpy(res, "fail - concurrent final size");
        bst_set_destroy(set);
        return;
    }
    
    bst_set_destroy(set);
    strcpy(res, "pass");
}