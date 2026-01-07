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
    if(!dest || !from || !size_dest || !size_from){
        return;
    }
    int added = 0;

outer:  for(int i = 0; i < size_from; i++){
            for(int j = 0; j < *size_dest; j++){
                if(from[i] == dest[j]){
                    goto outer;
                }
            }
            dest[*size_dest + added] = from[i];
            added++;
        }
    
    *size_dest += added;
    return;
}

int* intersection_arrays(int* set1, int set1_len, int* set2, int set2_len, int* res_len){
    if(!set1 || !set2 || !set1_len || !set2_len || !res_len){
        return NULL;
    }
    *res_len = 0;

    int res_starting_size = set1_len > set2_len ? set1_len : set2_len;
    int* result = malloc(sizeof(int) * (unsigned int)res_starting_size);
    if(result == NULL){
        return NULL; 
    }


set2_for:   for(int i = 0; i < set2_len; i++){
                for(int j = 0; j < set1_len; j++){
                    if(set2[i] == set1[j]){
                        result[*res_len++] = set2[i];
                        goto set2_for;
                    }
                }
            }
    
    
    void* realloc_res = realloc(result, sizeof(int) * (unsigned int)(*res_len));

    free(result);
    return realloc_res;
}

int* exclusion_arrays(int* set1, int set1_len, int* set2, int set2_len, int* res_len){
    if(!set1 || !set2 || !set1_len || !set2_len || !res_len){
        *res_len = 0;
        return NULL;
    }
    *res_len = 0;

    int* result = malloc(sizeof(int) * (unsigned int)set1_len);
    if(result == NULL){
        return NULL; 
    }

set1_for:   for(int i = 0; i < set1_len; i++){
                for(int j = 0; j < set2_len; j++){
                    if(set1[i] == set2[j]){
                        goto set1_for;
                    }
                }
                result[(*res_len)++] = set1[i];
            }
    
    void* realloc_res = realloc(result, sizeof(int) * (unsigned int)(*res_len));

    free(result);
    return realloc_res;
}

bool is_subset(int* superset, int size_superset, int* subset, int size_subset){
    if(!superset || !subset || !size_superset){
        return false;
    }

superset:   for(int i = 0; i < size_subset; i++){
                for(int j = 0; j < size_superset; j++){
                    if(subset[i] == superset[j]){
                        goto superset;
                    }
                }
                return false;
            }

    return true;
}

int translate_index_buffer(la* la_layer, int ID, int buffersize){
    int x = ID % buffersize;
    int exp_round = ID / buffersize;

    if (la_layer->round != exp_round){
        return -1;
    }

    return x - 1; // because we want an index
}