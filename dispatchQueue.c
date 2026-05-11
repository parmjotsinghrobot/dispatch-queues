#include <stdlib.h>
#include "dispatchQueue.h"

// Creates a dispatch queue, probably setting up any associated threads and a linked list to be used by the added tasks. The queueType is either CONCURRENT or SERIAL
dispatch_queue_t *dispatch_queue_create(queue_type_t queueType) {
    dispatch_queue_t *queue = malloc(sizeof(dispatch_queue_t));
    if (queue == NULL) {
        error_exit("malloc failed");
    }
    queue->queue_type = queueType;
    return queue;
}

// Destroys the dispatch queue queue. All allocated memory and resources such as semaphores are released and returned.
void dispatch_queue_destroy(dispatch_queue_t *queue) {
    free(queue);
}
