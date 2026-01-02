#include "ba.h"
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

// utils 

static inline void _lock(ba* ba){
    int zero = 0; int val = 0;
    do{
        val = atomic_compare_exchange_weak(&ba->lock, &zero, 1);
    }
    while (val == 0);

    return;
}

static inline void _unlock(ba* ba){
    atomic_store(&ba->lock, 0);
    return;
}


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
    
    bit_array->bits = (size_t*)calloc(bit_array->num_blocks, sizeof(size_t));
    if (!bit_array->bits) {
        free(bit_array);
        return NULL;
    }
    
    // Initialize all blocks to 0
    for (size_t i = 0; i < bit_array->num_blocks; i++) {
        bit_array->bits[i] = 0;
    }

    bit_array->lock = 0;
    
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
    if (!bit_array){
        return -1;
    }

    _lock(bit_array);
    if (i >= bit_array->length) {
        _unlock(bit_array);
        return -1;
    }
    
    size_t block_idx = i / BITS_PER_BLOCK;
    size_t bit_idx = i % BITS_PER_BLOCK;
    uint64_t mask = 1ULL << bit_idx;
    
    // Atomically set the bit using fetch_or
    bit_array->bits[block_idx] |= mask;
    
    _unlock(bit_array);
    return 0;
}

int ba_rm(ba* bit_array, size_t i) {
    if (!bit_array){
        return -1;
    }

    _lock(bit_array);
    if (i >= bit_array->length) {
        _unlock(bit_array);
        return -1;
    }
    
    size_t block_idx = i / BITS_PER_BLOCK;
    size_t bit_idx = i % BITS_PER_BLOCK;
    uint64_t mask = ~(1ULL << bit_idx);
    
    // Atomically clear the bit using fetch_and
    bit_array->bits[block_idx] &= mask;
    
    return 0;
}

int ba_get(ba* bit_array, size_t i) {
    if (!bit_array){
        return -1;
    }

    _lock(bit_array);
    if (i >= bit_array->length) {
        return -1;
        _unlock(bit_array);
    }
    
    size_t block_idx = i / BITS_PER_BLOCK;
    size_t bit_idx = i % BITS_PER_BLOCK;
    
    // Atomically load the block
    uint64_t block = bit_array->bits[block_idx];
    
    return (block & (1ULL << bit_idx)) ? 1 : 0;
}

size_t ba_sum(ba* bit_array) {
    if (!bit_array) {
        return 0;
    }

    _lock(bit_array);
    
    size_t count = 0;
    
    for (size_t block_idx = 0; block_idx < bit_array->num_blocks; block_idx++) {
        uint64_t block = bit_array->bits[block_idx];
        
        // Count bits using Brian Kernighan's algorithm
        while (block) {
            block &= (block - 1);
            count++;
        }
    }
    _unlock(bit_array);
    
    return count;
}

size_t ba_where_1(ba* bit_array, size_t** indexes_1s, size_t* count_1s) {
    if (!bit_array) {
        return 0;
    }

    _lock(bit_array);
    
    // First pass: count the number of 1s
    size_t count = ba_sum(bit_array);

    // return if caller doesn't specify return arraypointer
    if (!indexes_1s || !count_1s){
        _unlock(bit_array);
        return count;
    }
    
    *count_1s = count;
    
    if (count == 0) {
        *indexes_1s = NULL;
        _unlock(bit_array);
        return 0;
    }
    
    // Allocate array for indexes
    *indexes_1s = (size_t*)malloc(count * sizeof(size_t));
    if (!*indexes_1s) {
        _unlock(bit_array);
        return 0;
    }
    
    // Second pass: collect indexes
    size_t idx = 0;
    for (size_t i = 0; i < bit_array->length && idx < count; i++) {
        if (ba_get(bit_array, i) == 1) {
            (*indexes_1s)[idx++] = i;
        }
    }
    
    _unlock(bit_array);
    return count;
}

size_t ba_where_0(ba* bit_array, size_t** indexes_0s, size_t* count_0s) {
    if (!bit_array || !indexes_0s || !count_0s) {
        return 0;
    }

    _lock(bit_array);
    
    // Count zeros
    size_t count_ones = ba_sum(bit_array);
    size_t count = bit_array->length - count_ones;
    *count_0s = count;
    
    if (count == 0) {
        *indexes_0s = NULL;
        _unlock(bit_array);
        return 0;
    }
    
    // Allocate array for indexes
    *indexes_0s = (size_t*)malloc(count * sizeof(size_t));
    if (!*indexes_0s) {
        _unlock(bit_array);
        return 0;
    }
    
    // Collect indexes where bits are 0
    size_t idx = 0;
    for (size_t i = 0; i < bit_array->length && idx < count; i++) {
        if (ba_get(bit_array, i) == 0) {
            (*indexes_0s)[idx++] = i;
        }
    }
    
    _unlock(bit_array);
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
        uint64_t value = ba->bits[i];
        memcpy(&dest[i * block_size], &value, block_size);
    }

    return bits;
}

ba* ba_construct(void* bits, size_t bitsLen){
    if (!bits || bitsLen == 0) {
        return NULL;
    }
    
    // Allocate the ba structure
    ba* bit_array = (ba*)malloc(sizeof(ba));
    if (!bit_array) {
        return NULL;
    }
    
    bit_array->length = bitsLen;
    bit_array->num_blocks = (bitsLen + BITS_PER_BLOCK - 1) / BITS_PER_BLOCK;
    
    // Allocate memory for blocks
    bit_array->bits = (size_t*)calloc(bit_array->num_blocks, sizeof(size_t));
    if (!bit_array->bits) {
        free(bit_array);
        return NULL;
    }
    
    // Copy the raw bits into the blocks
    uint8_t block_size = BITS_PER_BLOCK / 8; // 8 bytes per block
    unsigned char* src = (unsigned char*)bits;
    
    for (uint8_t i = 0; i < bit_array->num_blocks; i++) {
        uint64_t value;
        memcpy(&value, &src[i * block_size], block_size);
        bit_array->bits[i] = value;
    }
    
    return bit_array;
}

// locking only the destination to avoid deadlocks and because
// that is the only bit_array that actually gets modified 
void ba_merge(ba* dest, ba* add){
    _lock(dest);
    for(size_t i = 0; i < dest->num_blocks; i++){
        // Load value from 'add' array
        uint64_t value = add->bits[i];
        
        // Atomically OR it into dest
        dest->bits[i] |= value;
    }
    _unlock(dest);
    return;
}


// locks only subset, ba's have to be same length
bool ba_subset(ba* superset, ba* subset){
    if (!superset || !subset){
        return false;
    }

    _lock(subset);
    if (superset->length != subset->length){
        _unlock(subset);
        return false;
    }


    for(size_t i = 0; i < superset->length; i++){
        size_t part_res = superset->bits[i] & subset->bits[i];
        if(part_res != subset->bits[i]){
            _unlock(subset);
            return false;
        }
    }
    _unlock(subset);
    return true;
}
