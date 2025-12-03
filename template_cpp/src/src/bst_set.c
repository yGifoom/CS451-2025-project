#include "bst_set.h"
#include <string.h>
#include <stdlib.h>
#include <pthread.h>

// Helper functions
static int max(int a, int b) {
    return (a > b) ? a : b;
}

static int height(bst_node* node) {
    return node ? node->height : 0;
}

static int get_balance(bst_node* node) {
    if (!node) return 0;
    
    // Cast to long to avoid overflow warnings
    long left_h = height(node->left);
    long right_h = height(node->right);
    
    return (int)(left_h - right_h);
}

static bst_node* create_node(size_t key) {
    bst_node* node = malloc(sizeof(bst_node));
    if (!node) return NULL;
    
    node->key = key;
    node->height = 1;
    node->left = NULL;
    node->right = NULL;
    return node;
}

static bst_node* rotate_right(bst_node* y) {
    bst_node* x = y->left;
    bst_node* T2 = x->right;
    
    x->right = y;
    y->left = T2;
    
    y->height = max(height(y->left), height(y->right)) + 1;
    x->height = max(height(x->left), height(x->right)) + 1;
    
    return x;
}

static bst_node* rotate_left(bst_node* x) {
    bst_node* y = x->right;
    bst_node* T2 = y->left;
    
    y->left = x;
    x->right = T2;
    
    x->height = max(height(x->left), height(x->right)) + 1;
    y->height = max(height(y->left), height(y->right)) + 1;
    
    return y;
}

static bst_node* find_min(bst_node* node) {
    while (node->left) {
        node = node->left;
    }
    return node;
}

static bst_node* insert_helper(bst_node* node, size_t key, int* status, void* data, size_t data_size) {
    if (!node) {
        *status = 0;
        bst_node* new_node = create_node(key);
        if (new_node && data && data_size > 0) {
            if ( data_size != 0 || new_node->data != NULL) {
                new_node->data = malloc(data_size);
                memcpy(new_node->data, data, data_size);
            } else{
                new_node->data = NULL; 
            }
            new_node->data_size = data_size;
        } else if (new_node) {
            new_node->data = NULL;
            new_node->data_size = 0;
        }
        return new_node;
    }
    
    if (key < node->key) {
        node->left = insert_helper(node->left, key, status, data, data_size);
    } else if (key > node->key) {
        node->right = insert_helper(node->right, key, status, data, data_size);
    } else {
        *status = 1; // Key already exists
        return node;
    }
    
    node->height = 1 + max(height(node->left), height(node->right));
    
    int balance = get_balance(node);
    
    // Left Left Case
    if (balance > 1 && node->left && key < node->left->key) {
        return rotate_right(node);
    }
    
    // Right Right Case
    if (balance < -1 && node->right && key > node->right->key) {
        return rotate_left(node);
    }
    
    // Left Right Case
    if (balance > 1 && node->left && key > node->left->key) {
        node->left = rotate_left(node->left);
        return rotate_right(node);
    }
    
    // Right Left Case
    if (balance < -1 && node->right && key < node->right->key) {
        node->right = rotate_right(node->right);
        return rotate_left(node);
    }
    
    return node;
}

static bst_node* delete_helper(bst_node* node, size_t key, int* status) {
    if (!node) {
        *status = -1; // Key not found
        return NULL;
    }
    
    if (key < node->key) {
        node->left = delete_helper(node->left, key, status);
    } else if (key > node->key) {
        node->right = delete_helper(node->right, key, status);
    } else {
        *status = 0; // Key found
        
        if (!node->left || !node->right) {
            bst_node* temp = node->left ? node->left : node->right;
            
            // Free the data of the node being deleted
            if (node->data != NULL){
                free(node->data);
            }
            
            // If no children, just free the node
            if (!temp) {
                free(node);
                return NULL;
            } else {
                // One child: copy child's content and free the child
                *node = *temp;
                free(temp);
                return node;
            }
        } else {
            // Two children: find inorder successor
            bst_node* temp = find_min(node->right);
            node->key = temp->key;
            
            // Free old data and copy new data from successor
            if (node->data != NULL) {
                free(node->data);
            }
            if (temp->data == NULL) {
                node->data = NULL;
                node->data_size = 0;
            } else {
                node->data = malloc(temp->data_size);
                if (node->data) {
                    memcpy(node->data, temp->data, temp->data_size);
                    node->data_size = temp->data_size;
                }
            }
            node->right = delete_helper(node->right, temp->key, status);
        }
    }
    
    if (!node) return NULL;
    
    node->height = 1 + max(height(node->left), height(node->right));
    
    int balance = get_balance(node);
    
    // Left Left Case
    if (balance > 1 && get_balance(node->left) >= 0) {
        return rotate_right(node);
    }
    
    // Left Right Case
    if (balance > 1 && get_balance(node->left) < 0) {
        node->left = rotate_left(node->left);
        return rotate_right(node);
    }
    
    // Right Right Case
    if (balance < -1 && get_balance(node->right) <= 0) {
        return rotate_left(node);
    }
    
    // Right Left Case
    if (balance < -1 && get_balance(node->right) > 0) {
        node->right = rotate_right(node->right);
        return rotate_left(node);
    }
    
    return node;
}

static int lookup_helper(bst_node* node, size_t key, void** data, size_t* data_size) {
    if (!node) return 0;
    
    if (key == node->key){
        if (data != NULL)*data = node->data;
        if(data_size != NULL)*data_size = node->data_size;
        return 1;
    } 
    if (key < node->key) return lookup_helper(node->left, key, data, data_size);
    return lookup_helper(node->right, key, data, data_size);
}

static void destroy_helper(bst_node* node) {
    if (!node) return;
    
    destroy_helper(node->left);
    destroy_helper(node->right);
    free(node->data);
    free(node);
}

// Public interface
bst_set* bst_set_init(void) {
    bst_set* set = malloc(sizeof(bst_set));
    if (!set) return NULL;
    
    set->root = NULL;
    set->size = 0;
    
    if (pthread_mutex_init(&set->mutex, NULL) != 0) {
        free(set);
        return NULL;
    }
    
    return set;
}

int bst_set_add(bst_set* set, size_t key, void* data, size_t data_size) {
    if (!set) return -1;
    
    pthread_mutex_lock(&set->mutex);
    
    int status = 0;
    set->root = insert_helper(set->root, key, &status, data, data_size);
    
    if (status == 0) {
        set->size++;
    }
    
    pthread_mutex_unlock(&set->mutex);
    
    return status;
}

int bst_set_delete(bst_set* set, size_t key) {
    if (!set) return -1;
    
    pthread_mutex_lock(&set->mutex);
    
    int status = 0;
    set->root = delete_helper(set->root, key, &status);
    
    if (status == 0) {
        set->size--;
    }
    
    pthread_mutex_unlock(&set->mutex);
    
    return status;
}

int bst_set_lookup(bst_set* set, size_t key, void** data, size_t* data_size) {
    if (!set) return 0;
    
    pthread_mutex_lock(&set->mutex);
    int result = lookup_helper(set->root, key, data, data_size);
    pthread_mutex_unlock(&set->mutex);
    
    return result;
}

size_t bst_set_compact_consequent(bst_set* set, size_t lastCon, int (*condition)(void* data, size_t data_size)) {
    if (!set) return 0;

    pthread_mutex_lock(&set->mutex);

    size_t removed = 0;

    while (set->root) {
        // Avoid overflow on (lastCon + 1)
        size_t next = lastCon + 1;
        if (next <= lastCon) {
            // Overflow detected; cannot progress further.
            break;
        }

        bst_node* min_node = find_min(set->root);
        if (!min_node) break;

        if (min_node->key == next && (condition == NULL || condition(min_node->data, min_node->data_size) == 1)) {
            int status = 0;
            set->root = delete_helper(set->root, min_node->key, &status);
            if (status == 0) {
                set->size--;
                removed++;
                lastCon = next;
            } else {
                // Unexpected: min not found; stop to avoid infinite loop
                break;
            }
        } else {
            // Minimum key is not the next consecutive value; stop
            break;
        }
    }

    pthread_mutex_unlock(&set->mutex);
    return removed;
}

void bst_set_destroy(bst_set* set) {
    if (!set) return;
    
    pthread_mutex_lock(&set->mutex);
    destroy_helper(set->root);
    pthread_mutex_unlock(&set->mutex);
    
    pthread_mutex_destroy(&set->mutex);
    free(set);
}
