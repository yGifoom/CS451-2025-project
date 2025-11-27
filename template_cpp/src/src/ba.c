#include "ba.h"
#include <stdlib.h>
#include <string.h>

#define BITS_PER_BLOCK 64

ba* ba_init(size_t len) {
    if (len == 0) {
        return NULL;
    }
    
    ba* bit_array = (ba*)malloc(sizeof(ba));
    if (!bit_array) {
        return NULL;
    }
    
    bit_array->length = len;
    bit_array->num_blocks = (len + BITS_PER_BLOCK - 1) / BITS_PER_BLOCK;
    
    bit_array->bits = (atomic_uint_fast64_t*)calloc(bit_array->num_blocks, sizeof(atomic_uint_fast64_t));
    if (!bit_array->bits) {
        free(bit_array);
        return NULL;
    }
    
    // Initialize all atomic blocks to 0
    for (size_t i = 0; i < bit_array->num_blocks; i++) {
        atomic_init(&bit_array->bits[i], 0);
    }
    
    return bit_array;
}

int ba_destroy(ba* bit_array) {
    if (!bit_array) {
        return -1;
    }
    
    if (bit_array->bits) {
        free(bit_array->bits);
    }
    free(bit_array);
    
    return 0;
}

int ba_add(ba* bit_array, size_t i) {
    if (!bit_array || i >= bit_array->length) {
        return -1;
    }
    
    size_t block_idx = i / BITS_PER_BLOCK;
    size_t bit_idx = i % BITS_PER_BLOCK;
    uint64_t mask = 1ULL << bit_idx;
    
    // Atomically set the bit using fetch_or
    atomic_fetch_or_explicit(&bit_array->bits[block_idx], mask, memory_order_relaxed);
    
    return 0;
}

int ba_rm(ba* bit_array, size_t i) {
    if (!bit_array || i >= bit_array->length) {
        return -1;
    }
    
    size_t block_idx = i / BITS_PER_BLOCK;
    size_t bit_idx = i % BITS_PER_BLOCK;
    uint64_t mask = ~(1ULL << bit_idx);
    
    // Atomically clear the bit using fetch_and
    atomic_fetch_and_explicit(&bit_array->bits[block_idx], mask, memory_order_relaxed);
    
    return 0;
}

int ba_get(ba* bit_array, size_t i) {
    if (!bit_array || i >= bit_array->length) {
        return -1;
    }
    
    size_t block_idx = i / BITS_PER_BLOCK;
    size_t bit_idx = i % BITS_PER_BLOCK;
    
    // Atomically load the block
    uint64_t block = atomic_load_explicit(&bit_array->bits[block_idx], memory_order_relaxed);
    
    return (block & (1ULL << bit_idx)) ? 1 : 0;
}

size_t ba_sum(ba* bit_array) {
    if (!bit_array) {
        return 0;
    }
    
    size_t count = 0;
    
    for (size_t block_idx = 0; block_idx < bit_array->num_blocks; block_idx++) {
        uint64_t block = atomic_load_explicit(&bit_array->bits[block_idx], memory_order_relaxed);
        
        // Count bits using Brian Kernighan's algorithm
        while (block) {
            block &= (block - 1);
            count++;
        }
    }
    
    return count;
}

size_t ba_where_1(ba* bit_array, size_t** indexes_1s, size_t* count_1s) {
    if (!bit_array) {
        return 0;
    }
    
    // First pass: count the number of 1s
    size_t count = ba_sum(bit_array);

    // return if caller doesn't specify return arraypointer
    if (!indexes_1s || !count_1s){
        return count;
    }
    
    *count_1s = count;
    
    if (count == 0) {
        *indexes_1s = NULL;
        return 0;
    }
    
    // Allocate array for indexes
    *indexes_1s = (size_t*)malloc(count * sizeof(size_t));
    if (!*indexes_1s) {
        return 0;
    }
    
    // Second pass: collect indexes
    size_t idx = 0;
    for (size_t i = 0; i < bit_array->length && idx < count; i++) {
        if (ba_get(bit_array, i) == 1) {
            (*indexes_1s)[idx++] = i;
        }
    }
    
    return count;
}

size_t ba_where_0(ba* bit_array, size_t** indexes_0s, size_t* count_0s) {
    if (!bit_array || !indexes_0s || !count_0s) {
        return 0;
    }
    
    // Count zeros
    size_t count_ones = ba_sum(bit_array);
    size_t count = bit_array->length - count_ones;
    *count_0s = count;
    
    if (count == 0) {
        *indexes_0s = NULL;
        return 0;
    }
    
    // Allocate array for indexes
    *indexes_0s = (size_t*)malloc(count * sizeof(size_t));
    if (!*indexes_0s) {
        return 0;
    }
    
    // Collect indexes where bits are 0
    size_t idx = 0;
    for (size_t i = 0; i < bit_array->length && idx < count; i++) {
        if (ba_get(bit_array, i) == 0) {
            (*indexes_0s)[idx++] = i;
        }
    }
    
    return count;
}

void* ba_unsafe_copy(ba* ba){
    if (ba == NULL){
        return NULL;
    }

    size_t block_size = BITS_PER_BLOCK / 8;
    void* bits = malloc(ba->num_blocks * block_size);
    if (!bits) {
        return NULL;
    }

    unsigned char* dest = (unsigned char*)bits;
    
    for(size_t i = 0; i < ba->num_blocks; i++){
        uint64_t value = atomic_load_explicit(&ba->bits[i], memory_order_relaxed);
        memcpy(&dest[i * block_size], &value, block_size);
    }

    return bits;
}

ba* ba_construct(void* bits, size_t bitsLen){
    return NULL;
}

void ba_merge(ba* dest, ba* add){
    for(size_t i = 0; i < dest->num_blocks; i++){
        // Load value from 'add' array
        uint64_t value = atomic_load_explicit(&add->bits[i], memory_order_relaxed);
        
        // Atomically OR it into dest
        atomic_fetch_or_explicit(&dest->bits[i], value, memory_order_relaxed);
    }
    
    return;
}

