// external
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

// internal
#include "utils.h"
#include "dict.h"
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

void _bootstrap_merge_incoming_proposals(int* own_frame, int* incoming_frame, 
    struct dictionary* own_unique_proposals, int* num_own_unique_proposals, int ds){

        int own_unique_prop_n = *num_own_unique_proposals;
        char s[16];

        for(int i = 0; i < incoming_frame[1] || own_unique_prop_n >= ds; i++){
            int prop = incoming_frame[i+2];
            sprintf( s, "%d", prop);

            if(!dic_add(own_unique_proposals, s, (int)strlen(s))){
                own_frame[own_unique_prop_n + 2] = prop;
                own_unique_prop_n++;
            }
        }

        *num_own_unique_proposals = own_unique_prop_n;
        return;
}

// Comparison function for qsort
static int compare_ints(const void* a, const void* b) {
    int arg1 = *(const int*)a;
    int arg2 = *(const int*)b;
    
    if (arg1 < arg2) return -1;
    if (arg1 > arg2) return 1;
    return 0;
}