package task_queue

import "base:intrinsics"
import "core:sync"
import "core:testing"
import "core:thread"
import "core:time"

Test_State :: struct {
	mutex:       sync.Mutex,
	started:     [dynamic]int,
	finished:    [dynamic]int,
	active:      int,
	max_active:  int,
	cancelled:   bool,
	finalized:   bool,
	release:     bool,
}

Test_Data :: struct {
	state: ^Test_State,
	value: int,
	delay: time.Duration,
	fail:  bool,
}

recording_task :: proc(task_context: ^Task_Context) -> Task_Outcome {
	data := (^Test_Data)(task_context.data)
	sync.mutex_lock(&data.state.mutex)
	append(&data.state.started, data.value)
	data.state.active += 1
	data.state.max_active = max(data.state.max_active, data.state.active)
	sync.mutex_unlock(&data.state.mutex)
	if data.delay > 0 {
		time.sleep(data.delay)
	}
	sync.mutex_lock(&data.state.mutex)
	data.state.active -= 1
	append(&data.state.finished, data.value)
	sync.mutex_unlock(&data.state.mutex)
	return {failed = data.fail, code = data.value}
}

blocking_task :: proc(task_context: ^Task_Context) -> Task_Outcome {
	data := (^Test_Data)(task_context.data)
	sync.mutex_lock(&data.state.mutex)
	append(&data.state.started, data.value)
	sync.mutex_unlock(&data.state.mutex)
	for {
		sync.mutex_lock(&data.state.mutex)
		release := data.state.release
		sync.mutex_unlock(&data.state.mutex)
		if release || cancel_requested(task_context) {
			break
		}
		time.sleep(time.Millisecond)
	}
	return {code = data.value}
}

cancel_blocking :: proc(pointer: rawptr) {
	data := (^Test_Data)(pointer)
	sync.mutex_lock(&data.state.mutex)
	data.state.cancelled = true
	sync.mutex_unlock(&data.state.mutex)
}

finalize_recording :: proc(pointer: rawptr) {
	data := (^Test_Data)(pointer)
	sync.mutex_lock(&data.state.mutex)
	data.state.finalized = true
	sync.mutex_unlock(&data.state.mutex)
}

state_init :: proc(state: ^Test_State) {
	state.started = make([dynamic]int)
	state.finished = make([dynamic]int)
}

state_destroy :: proc(state: ^Test_State) {
	delete(state.started)
	delete(state.finished)
}

@(test)
queue_respects_concurrency_and_priority_test :: proc(t: ^testing.T) {
	state: Test_State
	state_init(&state)
	defer state_destroy(&state)
	queue: Queue
	testing.expect_value(t, queue_init(&queue, {concurrency = 2, start_paused = true}), Init_Error.None)
	defer queue_destroy(&queue)
	data := [4]Test_Data{
		{state = &state, value = 1, delay = 20 * time.Millisecond},
		{state = &state, value = 2, delay = 20 * time.Millisecond},
		{state = &state, value = 3, delay = 20 * time.Millisecond},
		{state = &state, value = 4, delay = 20 * time.Millisecond},
	}
	priorities := [4]int{0, 10, 5, 0}
	for datum, index in data {
		_, add_error := add(&queue, {
			procedure = recording_task,
			data = &data[index],
			priority = priorities[index],
		})
		testing.expect_value(t, add_error, Add_Error.None)
	}
	start(&queue)
	testing.expect(t, wait_until(&queue, .Idle, timeout = time.Second))
	testing.expect(t, state.max_active <= 2)
	testing.expect_value(t, state.started[0], 2)
	testing.expect_value(t, state.started[1], 3)
}

@(test)
pause_and_runtime_concurrency_test :: proc(t: ^testing.T) {
	state: Test_State
	state_init(&state)
	defer state_destroy(&state)
	queue: Queue
	testing.expect_value(t, queue_init(&queue, {concurrency = 1, start_paused = true}), Init_Error.None)
	defer queue_destroy(&queue)
	data := [2]Test_Data{
		{state = &state, value = 1, delay = 30 * time.Millisecond},
		{state = &state, value = 2, delay = 30 * time.Millisecond},
	}
	for &datum in data {
		_, _ = add(&queue, {procedure = recording_task, data = &datum})
	}
	time.sleep(10 * time.Millisecond)
	testing.expect_value(t, len(state.started), 0)
	testing.expect(t, set_concurrency(&queue, 2))
	start(&queue)
	testing.expect(t, wait_until(&queue, .Idle, timeout = time.Second))
	testing.expect_value(t, state.max_active, 2)
}

@(test)
waiting_and_running_cancellation_test :: proc(t: ^testing.T) {
	state: Test_State
	state_init(&state)
	defer state_destroy(&state)
	queue: Queue
	testing.expect_value(t, queue_init(&queue, {concurrency = 1}), Init_Error.None)
	defer queue_destroy(&queue)
	first := Test_Data{state = &state, value = 1}
	second := Test_Data{state = &state, value = 2}
	first_id, _ := add(&queue, {
		procedure = blocking_task,
		data = &first,
		cancel_procedure = cancel_blocking,
	})
	second_id, _ := add(&queue, {procedure = recording_task, data = &second})
	for snapshot(&queue).running == 0 {
		thread.yield()
	}
	testing.expect(t, cancel(&queue, second_id))
	testing.expect(t, cancel(&queue, first_id))
	first_info, first_ok := wait_task(&queue, first_id, time.Second)
	second_info, second_ok := wait_task(&queue, second_id, time.Second)
	testing.expect(t, first_ok && second_ok)
	testing.expect_value(t, first_info.state, Task_State.Cancelled)
	testing.expect_value(t, second_info.state, Task_State.Cancelled)
	testing.expect(t, state.cancelled)
}

@(test)
timeout_requests_cooperative_cancellation_test :: proc(t: ^testing.T) {
	state: Test_State
	state_init(&state)
	defer state_destroy(&state)
	queue: Queue
	testing.expect_value(t, queue_init(&queue, {
		concurrency = 1,
		default_timeout = 20 * time.Millisecond,
	}), Init_Error.None)
	defer queue_destroy(&queue)
	data := Test_Data{state = &state, value = 7}
	id, _ := add(&queue, {
		procedure = blocking_task,
		data = &data,
		cancel_procedure = cancel_blocking,
	})
	info, ok := wait_task(&queue, id, time.Second)
	testing.expect(t, ok)
	testing.expect_value(t, info.state, Task_State.Timed_Out)
	testing.expect(t, info.timeout_requested)
	testing.expect(t, state.cancelled)
}

@(test)
strict_rate_limit_spaces_starts_test :: proc(t: ^testing.T) {
	state: Test_State
	state_init(&state)
	defer state_destroy(&state)
	queue: Queue
	testing.expect_value(t, queue_init(&queue, {
		concurrency = 2,
		interval_cap = 1,
		interval = 30 * time.Millisecond,
		strict_rate_limit = true,
	}), Init_Error.None)
	defer queue_destroy(&queue)
	data := [2]Test_Data{
		{state = &state, value = 1},
		{state = &state, value = 2},
	}
	started := time.tick_now()
	for &datum in data {
		_, _ = add(&queue, {procedure = recording_task, data = &datum})
	}
	testing.expect(t, wait_until(&queue, .Idle, timeout = time.Second))
	elapsed := time.tick_diff(started, time.tick_now())
	testing.expect(t, elapsed >= 25 * time.Millisecond)
}

@(test)
events_and_release_test :: proc(t: ^testing.T) {
	state: Test_State
	state_init(&state)
	defer state_destroy(&state)
	queue: Queue
	testing.expect_value(t, queue_init(&queue, {concurrency = 1}), Init_Error.None)
	defer queue_destroy(&queue)
	data := Test_Data{state = &state, value = 9}
	id, _ := add(&queue, {procedure = recording_task, data = &data})
	info, ok := wait_task(&queue, id, time.Second)
	testing.expect(t, ok)
	testing.expect_value(t, info.outcome.code, 9)
	kinds: bit_set[Event_Kind]
	for {
		event, available := poll_event(&queue)
		if !available {
			break
		}
		kinds += {event.kind}
	}
	testing.expect(t, .Added in kinds)
	testing.expect(t, .Active in kinds)
	testing.expect(t, .Completed in kinds)
	testing.expect(t, .Idle in kinds)
	testing.expect(t, release(&queue, id))
	_, exists := task_info(&queue, id)
	testing.expect(t, !exists)
}

@(test)
waiting_auto_release_removes_cancelled_record_test :: proc(t: ^testing.T) {
	queue: Queue
	testing.expect_value(
		t,
		queue_init(&queue, {concurrency = 1, start_paused = true}),
		Init_Error.None,
	)
	defer queue_destroy(&queue)
	id, add_error := add(&queue, {
		procedure = recording_task,
		release_on_finish = true,
	})
	testing.expect_value(t, add_error, Add_Error.None)
	previous_state, cancelled := cancel_with_state(&queue, id)
	testing.expect(t, cancelled)
	testing.expect_value(t, previous_state, Task_State.Waiting)
	_, exists := task_info(&queue, id)
	testing.expect(t, !exists)
}

@(test)
clear_auto_releases_waiting_records_test :: proc(t: ^testing.T) {
	queue: Queue
	testing.expect_value(
		t,
		queue_init(&queue, {concurrency = 1, start_paused = true}),
		Init_Error.None,
	)
	defer queue_destroy(&queue)
	id, add_error := add(&queue, {
		procedure = recording_task,
		release_on_finish = true,
	})
	testing.expect_value(t, add_error, Add_Error.None)
	testing.expect_value(t, clear(&queue), 1)
	_, exists := task_info(&queue, id)
	testing.expect(t, !exists)
}

@(test)
finalizer_runs_before_idle_and_auto_release_test :: proc(t: ^testing.T) {
	state: Test_State
	state_init(&state)
	defer state_destroy(&state)
	queue: Queue
	testing.expect_value(
		t,
		queue_init(&queue, {concurrency = 1}),
		Init_Error.None,
	)
	defer queue_destroy(&queue)
	data := Test_Data{state = &state}
	id, add_error := add(&queue, {
		procedure = recording_task,
		data = &data,
		finalize_procedure = finalize_recording,
		release_on_finish = true,
	})
	testing.expect_value(t, add_error, Add_Error.None)
	testing.expect(t, wait_until(&queue, .Idle, timeout = time.Second))
	testing.expect(t, state.finalized)
	_, exists := task_info(&queue, id)
	testing.expect(t, !exists)
}

Queue_Class :: enum {
	Download,
	Export,
}

Policy_State :: struct {
	downloads: int,
	exports:   int,
}

class_from_pointer :: proc(pointer: rawptr) -> Queue_Class {
	return Queue_Class(uintptr(pointer))
}

select_with_class_limits :: proc(
	items: []Policy_Item,
	snapshot: Queue_Snapshot,
	pointer: rawptr,
) -> int {
	state := (^Policy_State)(pointer)
	selected := -1
	for item, index in items {
		class := class_from_pointer(item.policy_data)
		if class == .Download && state.downloads >= 1 ||
		   class == .Export && state.exports >= 1 {
			continue
		}
		if selected < 0 || item.priority > items[selected].priority {
			selected = index
		}
	}
	return selected
}

notify_class_limits :: proc(
	transition: Policy_Transition,
	item: Policy_Item,
	pointer: rawptr,
) {
	state := (^Policy_State)(pointer)
	change := 1
	if transition == .Finished {
		change = -1
	}
	if class_from_pointer(item.policy_data) == .Download {
		state.downloads += change
	} else {
		state.exports += change
	}
}

@(test)
custom_policy_selects_eligible_tasks_test :: proc(t: ^testing.T) {
	state: Test_State
	state_init(&state)
	defer state_destroy(&state)
	policy_state: Policy_State
	queue: Queue
	testing.expect_value(t, queue_init(&queue, {
		concurrency = 2,
		start_paused = true,
		policy = {
			select_procedure = select_with_class_limits,
			notify_procedure = notify_class_limits,
			data = &policy_state,
		},
	}), Init_Error.None)
	defer queue_destroy(&queue)
	data := [3]Test_Data{
		{state = &state, value = 1, delay = 30 * time.Millisecond},
		{state = &state, value = 2, delay = 30 * time.Millisecond},
		{state = &state, value = 3, delay = 30 * time.Millisecond},
	}
	classes := [3]Queue_Class{.Download, .Download, .Export}
	for datum, index in data {
		_, _ = add(&queue, {
			procedure = recording_task,
			data = &data[index],
			policy_data = rawptr(uintptr(classes[index])),
		})
	}
	start(&queue)
	testing.expect(t, wait_until(&queue, .Idle, timeout = time.Second))
	testing.expect_value(t, state.max_active, 2)
	testing.expect_value(t, state.started[0], 1)
	testing.expect_value(t, state.started[1], 3)
}

_ :: intrinsics
