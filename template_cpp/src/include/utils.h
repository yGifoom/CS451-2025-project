#ifndef UTILS_H
#define UTILS_H

#include "dict.h"

// copy a string and return the pointer
char* my_strdup(const char* s);
void _bootstrap_merge_incoming_proposals(int* own_frame, int* incoming_frame, 
struct dictionary* own_unique_proposals, int* num_own_unique_proposals, int ds);
static int compare_ints(const void* a, const void* b);



#endif