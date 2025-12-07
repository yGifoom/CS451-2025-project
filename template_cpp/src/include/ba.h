#ifndef BA_H
#define BA_H

#include <stdatomic.h>
#include <stdint.h>
#include <stddef.h>

typedef struct {
    atomic_uint_fast64_t* bits;  // Array of atomic 64-bit integers
    size_t length;                // Total number of bits
    size_t num_blocks;            // Number of 64-bit blocks
} ba;

// Initialize a bit array with given length
ba* ba_init(size_t len);

// Destroy a bit array and free memory
int ba_destroy(ba* ba);

// Set bit at index i to 1 (returns 0 on success, -1 on error)
int ba_add(ba* ba, size_t i);

// Set bit at index i to 0 (returns 0 on success, -1 on error)
int ba_rm(ba* ba, size_t i);

// Check if bit at index i is set (returns 1 if set, 0 if not, -1 on error)
int ba_get(ba* ba, size_t i);

// Count total number of bits set to 1
size_t ba_sum(ba* ba);

// Get indexes where bits are 1
// Returns number of 1s found, fills indexes_1s array (caller must free)
size_t ba_where_1(ba* ba, size_t** indexes_1s, size_t* count_1s);

// Get indexes where bits are 0
// Returns number of 0s found, fills indexes_0s array (caller must free)
size_t ba_where_0(ba* ba, size_t** indexes_0s, size_t* count_0s);

// unsafe copy bit array to a buffer
void* ba_unsafe_copy(ba* ba);


// construct a bit array starting from a 
// buffer of 1s & 0s
ba* ba_construct(void* bits, size_t bitsLen);

void ba_merge(ba* dest, ba* add);

#endif

