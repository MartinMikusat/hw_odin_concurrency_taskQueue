# hw_odin_concurrency_taskQueue

An in-memory task queue for Odin applications that need bounded background
work, priorities, cooperative cancellation, timeouts, and rate limits.

## AI-assisted development disclosure

Models used:

- **gpt-5.6-sol**

## Behavior

The queue owns a fixed set of worker threads. `add` appends a task and returns
a stable identifier. The scheduler starts the highest-priority eligible task
when a worker slot and a rate-limit slot are available.

The queue supports these operations:

- Set the concurrency while the queue runs.
- Pause and resume new task starts.
- Change a waiting task priority.
- Cancel one task or clear all waiting tasks.
- Apply a default timeout or a per-task timeout.
- Apply fixed-window or strict rolling rate limits.
- Poll transition events.
- Wait for one task, an empty waiting list, no running tasks, or complete idle.
- Install a custom selection policy for resource classes and barriers.

Cancellation and timeout requests are cooperative. A running task keeps its
worker slot until its procedure returns. The optional cancellation procedure
can terminate an owned subprocess or signal other task-specific work.

The queue retains terminal task records until `release` or `queue_destroy`.
The caller owns each task data pointer through that interval. The queue clones
task labels, but it does not allocate or free task data.

Keep the `Queue` at one stable memory address until `queue_destroy` returns.
The custom policy procedures execute while the queue mutex is locked. They
must not block or call queue operations.

## Example

```odin
package example

import task_queue "task_queue:."

download :: proc(task_context: ^task_queue.Task_Context) -> task_queue.Task_Outcome {
	request := (^Download_Request)(task_context.data)
	return run_download(request)
}

main :: proc() {
	queue: task_queue.Queue
	if task_queue.queue_init(&queue, {concurrency = 2}) != .None {
		return
	}
	defer task_queue.queue_destroy(&queue)

	request := Download_Request{}
	id, add_error := task_queue.add(&queue, {
		procedure = download,
		data = &request,
		label = "Download source",
	})
	if add_error != .None {
		return
	}
	result, completed := task_queue.wait_task(&queue, id)
	if completed {
		task_queue.release(&queue, id)
	}
}
```

## Verification

Run:

```sh
./test.sh
```

The tests cover concurrency, priority, pause and resume, cancellation, timeout,
strict rate limiting, events, record release, and a custom resource policy.

## License

MIT. See [LICENSE](LICENSE).
