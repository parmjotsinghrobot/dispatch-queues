/* 
 * File:   dispatchQueue.h
 * Author: shyamli
 *
 * Modified by: your UPI
 */

#ifndef DISPATCHQUEUE_H
    #define	DISPATCHQUEUE_H

    #include <stdio.h>
    #include <pthread.h>
    #include <semaphore.h>

    #define error_exit(MESSAGE)     perror(MESSAGE), exit(EXIT_FAILURE)

    typedef enum { // whether dispatching a task synchronously or asynchronously
        ASYNC, SYNC
    } task_dispatch_type_t;
    
    typedef enum { // The type of dispatch queue.
        CONCURRENT, SERIAL
    } queue_type_t;

    typedef struct task {
        char name[64];              // to identify it when debugging
        void (*work)(void *);       // the function to perform
        void *params;               // parameters to pass to the function
        task_dispatch_type_t type;  // asynchronous or synchronous
    } task_t;
    
    typedef struct dispatch_queue_t dispatch_queue_t; // the dispatch queue type
    typedef struct dispatch_queue_thread_t dispatch_queue_thread_t; // the dispatch queue thread type
    typedef struct task_node_t task_node_t; // internal task queue node

    struct dispatch_queue_thread_t {
        dispatch_queue_t *queue;// the queue this thread is associated with
        pthread_t thread;       // the thread which runs the task
        sem_t thread_semaphore; // the semaphore the thread waits on until a task is allocated
        task_t *task;           // the current task for this tread
    };

    struct dispatch_queue_t {
        queue_type_t queue_type;            // the type of queue - serial or concurrent
        task_node_t *task_list;             // head of linked list of tasks in the queue
        task_node_t *task_tail;             // tail of linked list of tasks in the queue
        dispatch_queue_thread_t *threads;   // the threads associated with this queue
        int num_threads;                    // the number of threads in the queue
        int active_count;                   // number of active tasks (running or queued)
        pthread_cond_t queue_cond;          // condition variable to wait for tasks to finish
        int shutdown;                       // shutdown flag used to stop worker threads
        pthread_mutex_t queue_mutex;        // the mutex to protect the queue data structure
        sem_t task_semaphore;               // semaphore used to wake worker threads
    };
    
    task_t *task_create(void (*)(void *), void *, char*);
    
    void task_destroy(task_t *);

    dispatch_queue_t *dispatch_queue_create(queue_type_t);
    
    void dispatch_queue_destroy(dispatch_queue_t *);
    
    void dispatch_async(dispatch_queue_t *, task_t *);
    
    void dispatch_sync(dispatch_queue_t *, task_t *);
        
    void dispatch_queue_wait(dispatch_queue_t *);

#endif	/* DISPATCHQUEUE_H */
