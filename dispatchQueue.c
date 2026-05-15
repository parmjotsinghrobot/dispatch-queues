#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "dispatchQueue.h"

typedef struct task_node_t {
    task_t *task;
    struct task_node_t *next;
} task_node_t;

static void task_enqueue(dispatch_queue_t *queue, task_t *task) {
    task_node_t *node = malloc(sizeof(task_node_t));
    if (node == NULL) error_exit("malloc failed");
    node->task = task;
    node->next = NULL;
    if (queue->task_tail) {
        queue->task_tail->next = node;
    } else {
        queue->task_list = node;
    }
    queue->task_tail = node;
}

static task_t *task_dequeue(dispatch_queue_t *queue) {
    task_node_t *node = queue->task_list;
    if (node == NULL) return NULL;
    queue->task_list = node->next;
    if (queue->task_list == NULL) queue->task_tail = NULL;
    task_t *task = node->task;
    free(node);
    return task;
}

static void task_clear_all(dispatch_queue_t *queue) {
    task_node_t *node = queue->task_list;
    while (node) {
        task_node_t *next = node->next;
        task_destroy(node->task);
        free(node);
        node = next;
    }
    queue->task_list = NULL;
    queue->task_tail = NULL;
}

static void *worker_main(void *arg) {
    dispatch_queue_thread_t *t = (dispatch_queue_thread_t *) arg;
    dispatch_queue_t *q = t->queue;

    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);

    for (;;) {
        sem_wait(&q->task_semaphore);
        pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
        if (pthread_mutex_lock(&q->queue_mutex) != 0) error_exit("pthread_mutex_lock failed");
        if (q->shutdown) {
            if (pthread_mutex_unlock(&q->queue_mutex) != 0) error_exit("pthread_mutex_unlock failed");
            pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
            break;
        }

        task_t *task = task_dequeue(q);
        if (task == NULL) {
            if (pthread_mutex_unlock(&q->queue_mutex) != 0) error_exit("pthread_mutex_unlock failed");
            continue;
        }
        q->active_count++;
        if (pthread_mutex_unlock(&q->queue_mutex) != 0) error_exit("pthread_mutex_unlock failed");
        pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
        task->work(task->params);
        task_destroy(task);
        pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
        if (pthread_mutex_lock(&q->queue_mutex) != 0) error_exit("pthread_mutex_lock failed");
        q->active_count--;
        if (q->active_count == 0 && q->task_list == NULL) {
            pthread_cond_signal(&q->queue_cond);
        }
        if (pthread_mutex_unlock(&q->queue_mutex) != 0) error_exit("pthread_mutex_unlock failed");
        pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    }

    return NULL;
}

dispatch_queue_t *dispatch_queue_create(queue_type_t queueType) {
    dispatch_queue_t *queue = malloc(sizeof(dispatch_queue_t));
    if (queue == NULL) {
        error_exit("malloc failed");
    }
    queue->queue_type = queueType;
    queue->task_list = NULL;
    queue->task_tail = NULL;
    queue->threads = NULL;
    queue->num_threads = 0;
    queue->active_count = 0;
    queue->shutdown = 0;

    if (pthread_mutex_init(&queue->queue_mutex, NULL) != 0) {
        free(queue);
        error_exit("pthread_mutex_init failed");
    }
    if (pthread_cond_init(&queue->queue_cond, NULL) != 0) {
        pthread_mutex_destroy(&queue->queue_mutex);
        free(queue);
        error_exit("pthread_cond_init failed");
    }
    if (sem_init(&queue->task_semaphore, 0, 0) != 0) {
        pthread_cond_destroy(&queue->queue_cond);
        pthread_mutex_destroy(&queue->queue_mutex);
        free(queue);
        error_exit("sem_init failed");
    }

    int threads = 1;
    if (queueType == CONCURRENT) {
        long cores = sysconf(_SC_NPROCESSORS_ONLN);
        threads = (cores > 0) ? (int) cores : 1;
    }

    queue->threads = calloc((size_t) threads, sizeof(dispatch_queue_thread_t));
    if (queue->threads == NULL) {
        sem_destroy(&queue->task_semaphore);
        pthread_cond_destroy(&queue->queue_cond);
        pthread_mutex_destroy(&queue->queue_mutex);
        free(queue);
        error_exit("calloc failed");
    }
    queue->num_threads = threads;
    for (int i = 0; i < threads; i++) {
        queue->threads[i].queue = queue;
        queue->threads[i].task = NULL;
        if (pthread_create(&queue->threads[i].thread, NULL, worker_main, &queue->threads[i]) != 0) {
            error_exit("pthread_create failed");
        }
    }
    return queue;
}

void dispatch_queue_destroy(dispatch_queue_t *queue) {
    if (queue == NULL) return;
    if (pthread_mutex_lock(&queue->queue_mutex) != 0) error_exit("pthread_mutex_lock failed");
    queue->shutdown = 1;
    task_clear_all(queue);
    if (pthread_mutex_unlock(&queue->queue_mutex) != 0) error_exit("pthread_mutex_unlock failed");

    for (int i = 0; i < queue->num_threads; i++) {
        pthread_cancel(queue->threads[i].thread);
        sem_post(&queue->task_semaphore);
    }
    for (int i = 0; i < queue->num_threads; i++) {
        pthread_join(queue->threads[i].thread, NULL);
    }

    sem_destroy(&queue->task_semaphore);
    pthread_cond_destroy(&queue->queue_cond);
    pthread_mutex_destroy(&queue->queue_mutex);
    free(queue->threads);
    free(queue);
}

void dispatch_sync(dispatch_queue_t *queue, task_t *task) {
    // add the task to the queue's linked list of tasks
    // signal a thread to execute the task
    // wait for the task to be completed before returning
    if (task == NULL) return;
    task->work(task->params);
    task_destroy(task);
}

task_t *task_create(void (* work)(void *), void *param, char* name) {
    task_t *task = malloc(sizeof(task_t));
    if (task == NULL) {
        error_exit("malloc failed");
    }
    task->work = work;
    task->params = param;
    strncpy(task->name, name, 63);
    task->name[63] = '\0'; // Ensure null termination
    return task;
}

void task_destroy(task_t *task) {
    if (task == NULL) return;
    free(task);
}

void dispatch_async(dispatch_queue_t *queue, task_t *task) {
    if (queue == NULL || task == NULL) return;

    if (pthread_mutex_lock(&queue->queue_mutex) != 0) error_exit("pthread_mutex_lock failed");
    if (queue->shutdown) {
        pthread_mutex_unlock(&queue->queue_mutex);
        task_destroy(task);
        return;
    }
    task_enqueue(queue, task);
    if (pthread_mutex_unlock(&queue->queue_mutex) != 0) error_exit("pthread_mutex_unlock failed");

    sem_post(&queue->task_semaphore);
}

void dispatch_queue_wait(dispatch_queue_t *queue) {
    if (queue == NULL) return;
    if (pthread_mutex_lock(&queue->queue_mutex) != 0) error_exit("pthread_mutex_lock failed");
    while (queue->active_count > 0 || queue->task_list != NULL) {
        pthread_cond_wait(&queue->queue_cond, &queue->queue_mutex);
    }
    if (pthread_mutex_unlock(&queue->queue_mutex) != 0) error_exit("pthread_mutex_unlock failed");
}
