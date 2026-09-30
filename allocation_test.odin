package task_queue

import "core:mem"
import "core:testing"
import "core:time"
import "base:runtime"

Fault_Allocator :: struct {
	backing: mem.Allocator,
	attempts, fail_at, live: int,
	triggered: bool,
}

fault_allocator_proc :: proc(data: rawptr, mode: mem.Allocator_Mode, size, alignment: int,
	old_memory: rawptr, old_size: int, location: runtime.Source_Code_Location = #caller_location,
) -> ([]byte, mem.Allocator_Error) {
	state := cast(^Fault_Allocator)data
	allocating := mode in bit_set[mem.Allocator_Mode]{.Alloc, .Alloc_Non_Zeroed, .Resize, .Resize_Non_Zeroed} && size > 0
	if allocating {
		state.attempts += 1
		if state.fail_at == state.attempts {
			state.triggered = true
			return nil, .Out_Of_Memory
		}
	}
	result, error := state.backing.procedure(state.backing.data, mode, size, alignment, old_memory, old_size, location)
	if error == nil {
		if allocating && old_memory == nil && raw_data(result) != nil {state.live += 1}
		if old_memory != nil && (mode == .Free || (mode in bit_set[mem.Allocator_Mode]{.Resize, .Resize_Non_Zeroed} && size == 0)) {state.live -= 1}
	}
	return result, error
}

@(test)
allocation_failures_preserve_submission_and_report_event_loss :: proc(t: ^testing.T) {
	for fail_phase in 1 ..= 5 {
		fault := Fault_Allocator{backing = context.allocator}
		allocator := mem.Allocator{procedure = fault_allocator_proc, data = &fault}
		queue: Queue
		testing.expect_value(t, queue_init(&queue, {concurrency = 1, start_paused = true, allocator = allocator}), Init_Error.None)
		fault.fail_at = fault.attempts + fail_phase
		id, error := add(&queue, {procedure = recording_task, label = "owned label"})
		testing.expect(t, fault.triggered)
		if fail_phase < 5 {
			testing.expect_value(t, error, Add_Error.Allocation_Failed)
			testing.expect_value(t, id, Task_ID(0))
			testing.expect_value(t, snapshot(&queue).waiting, 0)
			fault.fail_at = 0
			id, error = add(&queue, {procedure = recording_task, label = "retry"})
			testing.expect_value(t, error, Add_Error.None)
			testing.expect_value(t, id, Task_ID(1))
		} else {
			testing.expect_value(t, error, Add_Error.None)
			testing.expect_value(t, snapshot(&queue).dropped_events, u64(1))
		}
		queue_destroy(&queue)
		testing.expect_value(t, fault.live, 0)
	}
	for fail_phase in 1 ..= 2 {
		fault := Fault_Allocator{backing = context.allocator, fail_at = fail_phase}
		allocator := mem.Allocator{procedure = fault_allocator_proc, data = &fault}
		queue: Queue
		testing.expect_value(t, queue_init(&queue, {concurrency = 1, strict_rate_limit = true, interval_cap = 2, interval = time.Second, allocator = allocator}), Init_Error.Allocation_Failed)
		testing.expect_value(t, fault.live, 0)
		queue_destroy(&queue)
	}
}

@(test)
event_loss_does_not_stop_execution :: proc(t: ^testing.T) {
	state: Test_State
	state_init(&state)
	defer state_destroy(&state)
	fault := Fault_Allocator{backing = context.allocator}
	queue: Queue
	testing.expect_value(t, queue_init(&queue, {concurrency = 1, start_paused = true, allocator = {procedure = fault_allocator_proc, data = &fault}}), Init_Error.None)
	defer queue_destroy(&queue)
	data := Test_Data{state = &state, value = 7}
	fault.fail_at = fault.attempts + 4
	id, error := add(&queue, {procedure = recording_task, data = &data})
	testing.expect_value(t, error, Add_Error.None)
	testing.expect_value(t, snapshot(&queue).dropped_events, u64(1))
	start(&queue)
	info, completed := wait_task(&queue, id, time.Second)
	testing.expect(t, completed)
	testing.expect_value(t, info.state, Task_State.Completed)
	testing.expect_value(t, info.outcome.code, 7)
}
