#ifndef BST_SET_H
#define BST_SET_H

#include <stddef.h>
#include <pthread.h>

// struct designed with the help of LLM
typedef struct bst_node {
    size_t key;
    int height;
    struct bst_node* left;
    struct bst_node* right;

    void* data;
    size_t data_size;
} bst_node;

typedef struct {
    bst_node* root;
    size_t size;
    pthread_mutex_t mutex;
} bst_set;

// Initialize a new ordered set
bst_set* bst_set_init(void);

// Add an element with the given key
// if data and data_size are NULL, will act as set, otherwise as map
// Returns 0 on success, -1 on error, 1 if key already exists
int bst_set_add(bst_set* set, size_t key, void* data, size_t data_size);

// Delete an element with the given key
// Returns 0 on success, -1 if key not found
int bst_set_delete(bst_set* set, size_t key);

// Lookup if a key is present in the set 
// updates data to the memory contents key was pointing to. Can be NULL pointer.
// Returns 1 if present, 0 if not present
int bst_set_lookup(bst_set* set, size_t key, void** data, size_t* data_size);

// Destroy the set and free all memory
void bst_set_destroy(bst_set* set);

// Compact consecutive keys starting from (*lastCon + 1):
// Repeatedly removes the minimum node if it equals (*lastCon + 1),
// increments the accumulator, and updates *lastCon accordingly.
// if condition is not NULL node will be aggregated only if condition returns 1 (true)
// Returns the number of removed nodes, or -1 on error (e.g., NULL args).
size_t bst_set_compact_consequent(bst_set* set, size_t lastCon, int (*condition)(void* data, size_t data_size));

#endif // BST_SET_H
