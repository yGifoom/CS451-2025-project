// external
#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<pthread.h>
#include<unistd.h>
#include<time.h>

// internal 
#include"parser.h"
#include"la.h"
#include"pflx.h"

typedef struct {
    la* la_instance;
    int pid;
    char* config_path;
    int bootstrap_result;
    int started;
} bootstrap_thread_args;

static void* bootstrap_thread_func(void* arg) {
    bootstrap_thread_args* args = (bootstrap_thread_args*)arg;
    
    printf("Process %d: Starting LA layer\n", args->pid);
    fflush(stdout);
    
    int start_res = la_start(args->la_instance);
    if (start_res != 0) {
        printf("Process %d: Failed to start LA layer\n", args->pid);
        fflush(stdout);
        args->bootstrap_result = -1;
        return NULL;
    }
    args->started = 1;
    
    // Add random delay to simulate asynchronous starts
    usleep((rand() % 1000) * 1000); // 0-1 second delay
    
    printf("Process %d: Running bootstrap\n", args->pid);
    fflush(stdout);
    
    args->bootstrap_result = la_bootstrap_from_config(args->la_instance, args->config_path);
    
    printf("Process %d: Bootstrap completed with result %d\n", args->pid, args->bootstrap_result);
    fflush(stdout);
    
    return NULL;
}

static int verify_bootstrap_results(la** instances, int num_processes) {
    printf("\n=== Verifying Bootstrap Results ===\n");
    fflush(stdout);
    
    // Check all instances have the same ds and proposals_len
    int ds = instances[0]->ds;
    int proposals_len = instances[0]->proposals_len;
    int vs = instances[0]->vs;
    
    for (int i = 1; i < num_processes; i++) {
        if (instances[i]->ds != ds) {
            printf("ERROR: Process %d has ds=%d, expected %d\n", i+1, instances[i]->ds, ds);
            fflush(stdout);
            return -1;
        }
        if (instances[i]->proposals_len != proposals_len) {
            printf("ERROR: Process %d has proposals_len=%d, expected %d\n", 
                   i+1, instances[i]->proposals_len, proposals_len);
            fflush(stdout);
            return -1;
        }
        if (instances[i]->vs != vs) {
            printf("ERROR: Process %d has vs=%d, expected %d\n", i+1, instances[i]->vs, vs);
            fflush(stdout);
            return -1;
        }
    }
    
    printf("✓ All processes agree on ds=%d, vs=%d, proposals_len=%d\n", ds, vs, proposals_len);
    fflush(stdout);
    
    // Verify all processes have the same translation array
    for (int i = 1; i < num_processes; i++) {
        if (!instances[i]->index_to_proposal_int_translation || 
            !instances[0]->index_to_proposal_int_translation) {
            printf("ERROR: Process %d has NULL translation array\n", i+1);
            fflush(stdout);
            return -1;
        }
        
        for (int j = 0; j < ds; j++) {
            if (instances[i]->index_to_proposal_int_translation[j] != 
                instances[0]->index_to_proposal_int_translation[j]) {
                printf("ERROR: Process %d translation[%d]=%d, expected %d\n", 
                       i+1, j, instances[i]->index_to_proposal_int_translation[j],
                       instances[0]->index_to_proposal_int_translation[j]);
                fflush(stdout);
                return -1;
            }
        }
    }
    
    printf("✓ All processes have identical translation arrays\n");
    fflush(stdout);
    
    // Print translation array
    printf("Translation array (index -> proposal number):\n");
    for (int i = 0; i < ds; i++) {
        printf("  [%d] -> %d\n", i, instances[0]->index_to_proposal_int_translation[i]);
    }
    fflush(stdout);
    
    // Verify dictionary consistency
    printf("\n✓ Verifying dictionary consistency...\n");
    for (int proc = 0; proc < num_processes; proc++) {
        for (int i = 0; i < ds; i++) {
            char key[32];
            snprintf(key, sizeof(key), "%d", instances[proc]->index_to_proposal_int_translation[i]);
            
            if (!dic_find(instances[proc]->proposal_to_index_translation, key, strlen(key))) {
                printf("ERROR: Process %d cannot find key '%s' in dictionary\n", proc+1, key);
                fflush(stdout);
                return -1;
            }
            
            int index = *instances[proc]->proposal_to_index_translation->value;
            if (index != i) {
                printf("ERROR: Process %d key '%s' maps to index %d, expected %d\n", 
                       proc+1, key, index, i);
                fflush(stdout);
                return -1;
            }
        }
    }
    
    printf("✓ All dictionaries are consistent\n");
    fflush(stdout);
    
    return 0;
}

void testBootstrapLa(char* config_path, Parser* parser){
    printf("\n=== Starting LA Bootstrap Test ===\n");
    fflush(stdout);
    
    srand(time(NULL));
    
    size_t num_hosts;
    const Host* hosts = parser_get_hosts(parser, &num_hosts);
    int my_id = parser_get_id(parser);
    
    printf("Test configuration: %zu processes, my_id=%d\n", num_hosts, my_id);
    printf("Config file: %s\n", config_path);
    fflush(stdout);
    
    // Create LA instances for all processes
    la** instances = malloc(sizeof(la*) * num_hosts);
    bootstrap_thread_args* thread_args = malloc(sizeof(bootstrap_thread_args) * num_hosts);
    pthread_t* threads = malloc(sizeof(pthread_t) * num_hosts);
    
    // Initialize LA instances
    for (size_t i = 0; i < num_hosts; i++) {
        unsigned short base_port = 5000 + (i * 2);
        
        pflx* pflx_layer = pflx_init(base_port, hosts, num_hosts);
        pflx* beb_layer = pflx_init(base_port + 1, hosts, num_hosts);
        
        if (!pflx_layer || !beb_layer) {
            printf("ERROR: Failed to initialize pflx layers for process %zu\n", i+1);
            fflush(stdout);
            // Cleanup and exit
            for (size_t j = 0; j < i; j++) {
                la_destroy(instances[j]);
            }
            free(instances);
            free(thread_args);
            free(threads);
            return;
        }
        
        instances[i] = la_init(pflx_layer, beb_layer, config_path, i + 1);
        if (!instances[i]) {
            printf("ERROR: Failed to initialize LA for process %zu\n", i+1);
            fflush(stdout);
            return;
        }
        
        thread_args[i].la_instance = instances[i];
        thread_args[i].pid = i + 1;
        thread_args[i].config_path = config_path;
        thread_args[i].bootstrap_result = -99;
        thread_args[i].started = 0;
        
        printf("Process %zu: LA instance created\n", i+1);
        fflush(stdout);
    }
    
    // Start all bootstrap threads
    printf("\n=== Starting Bootstrap Threads ===\n");
    fflush(stdout);
    
    for (size_t i = 0; i < num_hosts; i++) {
        if (pthread_create(&threads[i], NULL, bootstrap_thread_func, &thread_args[i]) != 0) {
            printf("ERROR: Failed to create thread for process %zu\n", i+1);
            fflush(stdout);
            return;
        }
    }
    
    // Wait for all threads to complete
    printf("\n=== Waiting for Bootstrap to Complete ===\n");
    fflush(stdout);
    
    int all_success = 1;
    for (size_t i = 0; i < num_hosts; i++) {
        pthread_join(threads[i], NULL);
        
        if (thread_args[i].bootstrap_result != 0) {
            printf("Process %zu: Bootstrap FAILED with result %d\n", 
                   i+1, thread_args[i].bootstrap_result);
            fflush(stdout);
            all_success = 0;
        } else {
            printf("Process %zu: Bootstrap SUCCESS\n", i+1);
            fflush(stdout);
        }
    }
    
    if (!all_success) {
        printf("\n❌ TEST FAILED: Some processes failed bootstrap\n");
        fflush(stdout);
    } else {
        printf("\n✓ All processes completed bootstrap successfully\n");
        fflush(stdout);
        
        // Verify results
        if (verify_bootstrap_results(instances, num_hosts) == 0) {
            printf("\n✅ TEST PASSED: Bootstrap mechanism works correctly!\n");
            fflush(stdout);
        } else {
            printf("\n❌ TEST FAILED: Bootstrap verification failed\n");
            fflush(stdout);
        }
    }
    
    // Cleanup
    printf("\n=== Cleaning Up ===\n");
    fflush(stdout);
    
    for (size_t i = 0; i < num_hosts; i++) {
        if (thread_args[i].started) {
            la_stop(instances[i]);
        }
        la_destroy(instances[i]);
    }
    
    free(instances);
    free(thread_args);
    free(threads);
    
    printf("=== Test Complete ===\n\n");
    fflush(stdout);
}