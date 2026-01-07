#ifndef UTILS_H
#define UTILS_H

// external
#include <stdbool.h>

// internal
#include "la.h"

// copy a string and return the pointer
char* my_strdup(const char* s);
void _bootstrap_merge_incoming_proposals(int* own_frame, int* incoming_frame, 
struct dictionary* own_unique_proposals, int* num_own_unique_proposals, int ds);
static int compare_ints(const void* a, const void* b);

void union_arrays(int* dest, int* size_dest, int* from, int size_from);
int* intersection_arrays(int* set1, int set1_len, int* set2, int set2_len, int* res_len);
int* exclusion_arrays(int* set1, int set1_len, int* set2, int set2_len, int* res_len);
bool is_subset(int* superset, int size_superset, int* subset, int size_subset);
int translate_index_buffer(la* la_layer, int index, int buffersize);

#endif