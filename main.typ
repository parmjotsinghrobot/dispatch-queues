#let conf(doc) = {
  set text(lang: "en", region: "gb", font: "New Computer Modern")
  set block(above: 1em)
  set page(margin: 30pt)
  set par(linebreaks: "optimized", justify: true)
  // set enum(numbering: "a)")

  set quote(block: true)
  show quote: set pad(top: -1em, left: 2.5em, right: 2.5em)

  show heading.where(level: 1): set text(size: 1.4em)
  show heading.where(level: 2): set text(size: 1.2em)
  show heading.where(level: 3): set text(size: 1.1em, weight: "regular")
  show heading.where(level: 4): set text(weight: "regular")

  // Set numbering to `1.` but ignore the first level
  set heading(numbering: (first, ..nums) => {
      let pos = nums.pos()
      let n = if pos.len() == 1 {
        numbering("(1)", pos.at(0))
      } else {
        numbering("(1a. i)", first, ..nums)
      }
      text(weight: "bold", n)
    }
  )
  // Remove the space in front of headings(level: 1)
  show heading.where(level: 1): set heading(numbering: none)

  set table.vline(stroke: 0.75pt + black)
  set table.hline(stroke: 0.75pt + black)
  set table(stroke: (x, y) => (
    left: if x > 0 { 0.5pt + rgb(100, 100, 100)},
    top: if y > 0 { 0.5pt + rgb(100, 100, 100)},
  ))
  show table.cell.where(y: 0): set text(weight: 700)

  // set math.equation(numbering: "(1)")

  // this is to ensure that the document is shown at all.
  doc
}
#show: conf
#align(center)[
  = SOFTENG 370 Assignment 2
  Dispatch Queues and Thread Pools \
  Parmjot Singh - `psin539`
]

#counter(heading).update((0, 7))

== How does your impl. dispatch tasks from the queues?

The dispatch queue uses a thread pool plus a linked-list task queue.

```h
// dispatchQueue.h
struct dispatch_queue_t {
    ...
    task_node_t *task_list;             // head of linked list of tasks in the queue
    task_node_t *task_tail;             // tail of linked list of tasks in the queue
    dispatch_queue_thread_t *threads;   // the threads associated with this queue
    int num_threads;                    // the number of threads in the queue
    ...
};
```

This queue helps to store each task in the order they were enqueued. Each worker thread waits on a semaphore that signals when a new task is available. When a task is enqueued, the semaphore is posted to wake up a worker thread.

```h
// dispatchQueue.h
struct dispatch_queue_thread_t {
    ...
    sem_t thread_semaphore; // the semaphore the thread waits on until a task is allocated
    ...
};
```

Calls to `dispatch_async` enqueue work and signal a semaphore so an idle worker wakes up. These `async` calls do not block the caller, so they can be used to enqueue multiple tasks quickly, which is useful for a concurrent queue.

```c
// dispatchQueue.c
void dispatch_async(dispatch_queue_t *queue, task_t *task) {
    ... // error checks
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
```

As shown above, we use a helper function, `task_enqueue`, to add the task to the end of the linked list.

```c
// dispatchQueue.c
static void task_enqueue(dispatch_queue_t *queue, task_t *task) {
    task_node_t *node = malloc(sizeof(task_node_t));
    if (node == NULL) error_exit("malloc failed");
    node->task = task;
    node->next = NULL;
    if (queue->task_tail) { // if there is something in the queue, append to the end
        queue->task_tail->next = node;
    } else {
        queue->task_list = node;
    }
    queue->task_tail = node;
}
```

Each worker waits on the semaphore, dequeues a task under a mutex, executes it, and then updates the active count and condition variable so `dispatch_queue_wait` can detect completion.

```c
// dispatchQueue.c
static void *worker_main(void *arg) {
  dispatch_queue_thread_t *t = (dispatch_queue_thread_t *) arg;
  dispatch_queue_t *q = t->queue;

  for (;;) {
    sem_wait(&q->task_semaphore);
    pthread_mutex_lock(&q->queue_mutex);
    if (q->shutdown) { pthread_mutex_unlock(&q->queue_mutex); break; }

    task_t *task = task_dequeue(q);
    if (task == NULL) { pthread_mutex_unlock(&q->queue_mutex); continue; }
    q->active_count++;
    pthread_mutex_unlock(&q->queue_mutex);

    task->work(task->params);
    task_destroy(task); // helper function

    pthread_mutex_lock(&q->queue_mutex);
    q->active_count--;
    if (q->active_count == 0 && q->task_list == NULL) {
      pthread_cond_signal(&q->queue_cond);
    }
    pthread_mutex_unlock(&q->queue_mutex);
  }
  return NULL;
}
```

Finally, `dispatch_queue_wait` blocks until both the queue is empty and no workers are active.

```c
// dispatchQueue.c
void dispatch_queue_wait(dispatch_queue_t *queue) {
  pthread_mutex_lock(&queue->queue_mutex);
  while (queue->active_count > 0 || queue->task_list != NULL) {
    pthread_cond_wait(&queue->queue_cond, &queue->queue_mutex);
  }
  pthread_mutex_unlock(&queue->queue_mutex);
}
```

For serial queues, we can simply execute the task directly in `dispatch_sync` without using the worker threads or task queue, since there is only one thread and tasks are executed in order.

```c
// dispatchQueue.c
void dispatch_sync(dispatch_queue_t *queue, task_t *task) {
    // add the task to the queue's linked list of tasks
    // signal a thread to execute the task
    // wait for the task to be completed before returning
    if (task == NULL) return;
    task->work(task->params);
    task_destroy(task);
}
```

As shown above, we execute the task directly in `dispatch_sync` for serial queues, which ensures that tasks are executed in the order they were enqueued without needing to manage worker threads or a task queue. 

For cases where `dispatch_async` is called on a serial queue, we can still enqueue the task and signal the worker thread, but since there is only one worker thread for a serial queue, it will execute tasks in the order they were enqueued.

```c
// dispatchQueue.c
dispatch_queue_t *dispatch_queue_create(queue_type_t queueType) {
    ...
    int threads = 1; // SERIAL queues have only one thread
    if (queueType == CONCURRENT) {
        long cores = sysconf(_SC_NPROCESSORS_ONLN);
        threads = (cores > 0) ? (int) cores : 1;
    }
    ...
```
