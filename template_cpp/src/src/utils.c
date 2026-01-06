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

void union_arrays(int* dest, int size_dest, int* from, int size_from){
    if(!dest || !from || !size_dest || !size_from){
        return;
    }
    int added = 0;

outer:  for(int i = 0; i < size_from; i++){
            for(int j = 0; j < size_dest; j++){
                if(from[i] == dest[j]){
                    goto outer;
                }
            }
            added++;
            dest[size_dest + added] = from[i];
        }

    return;
}

bool is_subset(int* superset, int size_superset, int* subset, int size_subset){
    if(!superset || !subset || !size_superset || !size_subset){
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