// external
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <stdio.h>

// internal
#include "utils.h"
#include "la.h"


char* my_strdup(const char* s) {
    if (!s) return NULL;
    size_t len = strlen(s) + 1;
    char* dup = (char*)malloc(len);
    if (dup) {
        memcpy(dup, s, len);
    }
    return dup;
}

// Comparison function for qsort
static int compare_ints(const void* a, const void* b) {
    int arg1 = *(const int*)a;
    int arg2 = *(const int*)b;
    
    if (arg1 < arg2) return -1;
    if (arg1 > arg2) return 1;
    return 0;
}

void union_arrays(int* dest, int* size_dest, int* from, int size_from){
    if(!dest || !from || !size_dest || size_from < 0){
        return;
    }
    int added = 0;
    
    for(int i = 0; i < size_from; i++){
        bool present = false;
        for(int j = 0; j < *size_dest; j++){
            if(from[i] == dest[j]){
                present = true;break;
            }
        }
        if(!present){
            dest[*size_dest + added] = from[i];
            added++;
        }
    }
    
    *size_dest += added;
    return;
}

bool is_subset(int* superset, int size_superset, int* subset, int size_subset){
    if(!superset || !subset || !size_superset){
        return false;
    }

    for(int i = 0; i < size_subset; i++){
        bool found = false;
        for(int j = 0; j < size_superset; j++){
            if(subset[i] == superset[j]){
                found = true;break;
            }
        }
        if(!found)return false;
    }

    return true;
}

int translate_index_buffer(la* la_layer, int ID, int buffersize){
    int x = (ID - 1) % buffersize;
    
    int exp_round = ((ID - 1) / buffersize) + 1;

    if (la_layer->round < exp_round){
        return -1; // is a message which we will have in the buffer in the future, just wait
    }else if(la_layer->round > exp_round){
        return -2; // is a message which we got in the past and now got rid of
    }

    return x; // because we want an index
}

char* array_to_string(int* arr, int len) {
    if (!arr || len <= 0) {
        char* empty = (char*)malloc(3);
        if (empty) strcpy(empty, "[]");
        return empty;
    }
    
    // Estimate buffer size: ~12 chars per int + separators + brackets
    size_t buf_size = (size_t)len * 14 + 3;
    char* result = (char*)malloc(buf_size);
    if (!result) return NULL;
    
    char* ptr = result;
    ptr += sprintf(ptr, "[");
    
    for (int i = 0; i < len; i++) {
        if (i > 0) ptr += sprintf(ptr, ", ");
        ptr += sprintf(ptr, "%d", arr[i]);
    }
    
    sprintf(ptr, "]");
    return result;
}