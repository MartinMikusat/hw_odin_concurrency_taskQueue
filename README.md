# hw_odin_concurrency_taskQueue

An in-memory task queue for Odin applications that need concurrent background
work, priorities, cooperative cancellation, timeouts, and rate limits.

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
`cancel_with_state` returns whether the task was waiting or running when the
queue accepted the request. This lets an owner finalize task data that never
entered a worker procedure.

The optional finalization procedure runs after an entered task procedure
returns and before the queue publishes its terminal state or releases its
worker slot. Use it to free task data after a background result has been
committed. A task cancelled while waiting does not run its finalization
procedure because its task procedure never acquired ownership.

The queue retains terminal task records until `release` or `queue_destroy`.
Set `release_on_finish` when the caller does not need `wait_task` or
`task_info` after completion. The caller owns each task data pointer until the
terminal record is released. The queue clones task labels, but it does not
allocate or free task data.

Keep the `Queue` at one stable memory address until `queue_destroy` returns.
The custom policy procedures execute while the queue mutex is locked. They
must not block or call queue operations.

Waiting tasks, retained records and events grow with submissions. Callers bound
submission bursts, drain events and release terminal records. `snapshot` reports
`dropped_events`, a saturating lifetime count of event allocation failures.
Tasks continue when events are lost; use task state and wait operations for
completion guarantees. Submission allocation failure returns `.Allocation_Failed`
before publishing the task. `running_tasks` returns false on output allocation
failure and preserves its destination. Labels returned by `task_info`,
`wait_task` and `running_tasks` borrow their records until `release`, automatic
release or queue destruction; do not retain them across concurrent release.

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
strict rate limiting, events, finalization, record release, and a custom
resource policy.

## License

MIT. See [LICENSE](LICENSE).
