package task_queue

import "core:mem"
import "core:strings"
import "core:sync"
import "core:thread"
import "core:time"

Task_ID :: distinct u64

Task_State :: enum {
	Unknown,
	Waiting,
	Running,
	Completed,
	Failed,
	Cancelled,
	Timed_Out,
}

Task_Outcome :: struct {
	failed: bool,
	code:   int,
}

Task_Proc :: #type proc(task_context: ^Task_Context) -> Task_Outcome
Cancel_Proc :: #type proc(data: rawptr)
Finalize_Proc :: #type proc(data: rawptr)
Clock_Proc :: #type proc(data: rawptr) -> time.Tick

Task :: struct {
	procedure:        Task_Proc,
	data:             rawptr,
	cancel_procedure: Cancel_Proc,
	finalize_procedure: Finalize_Proc,
	priority:         int,
	policy_data:      rawptr,
	timeout:          time.Duration,
	override_timeout: bool,
	release_on_finish: bool,
	label:            string,
}

Task_Context :: struct {
	id:          Task_ID,
	data:        rawptr,
	policy_data: rawptr,
	_queue:      ^Queue,
	_record:     ^Task_Record,
}

Task_Info :: struct {
	id:               Task_ID,
	state:            Task_State,
	priority:         int,
	outcome:          Task_Outcome,
	cancel_requested: bool,
	timeout_requested: bool,
	label:            string,
}

Queue_Snapshot :: struct {
	waiting:     int,
	running:     int,
	terminal:    int,
	concurrency: int,
	paused:      bool,
}

Policy_Item :: struct {
	id:          Task_ID,
	priority:    int,
	sequence:    u64,
	policy_data: rawptr,
	label:       string,
	state:       Task_State,
}

Policy_Transition :: enum {
	Activated,
	Finished,
}

Policy_Select_Proc :: #type proc(
	items: []Policy_Item,
	snapshot: Queue_Snapshot,
	data: rawptr,
) -> int

Policy_Notify_Proc :: #type proc(
	transition: Policy_Transition,
	item: Policy_Item,
	data: rawptr,
)

Policy :: struct {
	select_procedure: Policy_Select_Proc,
	notify_procedure: Policy_Notify_Proc,
	data:             rawptr,
}

Queue_Options :: struct {
	concurrency:             int,
	start_paused:            bool,
	default_timeout:         time.Duration,
	interval_cap:            int,
	interval:                time.Duration,
	carryover_interval_count: bool,
	strict_rate_limit:       bool,
	allocator:                mem.Allocator,
	clock_procedure:          Clock_Proc,
	clock_data:               rawptr,
	policy:                   Policy,
}

Init_Error :: enum {
	None,
	Invalid_Concurrency,
	Invalid_Rate_Limit,
	Thread_Creation_Failed,
}

Add_Error :: enum {
	None,
	Invalid_Task,
	Queue_Stopping,
	Allocation_Failed,
}

Event_Kind :: enum {
	Added,
	Active,
	Completed,
	Failed,
	Cancel_Requested,
	Cancelled,
	Timeout_Requested,
	Timed_Out,
	Empty,
	Pending_Zero,
	Idle,
	Next,
	Rate_Limited,
	Cleared,
}

Task_Event :: struct {
	kind:    Event_Kind,
	task_id: Task_ID,
	state:   Task_State,
	code:    int,
}

Wait_Condition :: enum {
	Empty,
	Pending_Zero,
	Idle,
	Size_At_Most,
}

Shutdown_Mode :: enum {
	Drain,
	Cancel_Waiting,
	Cancel_All,
}

@(private)
Task_Record :: struct {
	id:                      Task_ID,
	sequence:                u64,
	task:                    Task,
	state:                   Task_State,
	outcome:                 Task_Outcome,
	cancel_requested:        bool,
	timeout_requested:       bool,
	cancel_callback_invoked: bool,
	cancel_callback_pending: bool,
	finishing:                bool,
	started_at:              time.Tick,
	deadline:                time.Tick,
}

@(private)
Cancel_Call :: struct {
	procedure: Cancel_Proc,
	data:      rawptr,
	record:    ^Task_Record,
}

Queue :: struct {
	allocator:                 mem.Allocator,
	mutex:                     sync.Mutex,
	condition:                 sync.Cond,
	records:                   [dynamic]^Task_Record,
	waiting:                   [dynamic]^Task_Record,
	events:                    [dynamic]Task_Event,
	event_offset:              int,
	workers:                   [dynamic]^thread.Thread,
	watchdog:                  ^thread.Thread,
	next_id:                   u64,
	next_sequence:             u64,
	running_count:             int,
	concurrency:               int,
	paused:                    bool,
	stopping:                  bool,
	shutdown_mode:             Shutdown_Mode,
	default_timeout:           time.Duration,
	interval_cap:              int,
	interval:                  time.Duration,
	carryover_interval_count:  bool,
	strict_rate_limit:         bool,
	rate_window_started:       time.Tick,
	rate_window_count:         int,
	strict_start_ticks:        [dynamic]time.Tick,
	rate_limited_event_active: bool,
	clock_procedure:           Clock_Proc,
	clock_data:                rawptr,
	policy:                    Policy,
}

@(private)
default_clock :: proc(data: rawptr) -> time.Tick {
	return time.tick_now()
}

@(private)
now_locked :: proc(queue: ^Queue) -> time.Tick {
	return queue.clock_procedure(queue.clock_data)
}

@(private)
find_record_locked :: proc(queue: ^Queue, id: Task_ID) -> ^Task_Record {
	for record in queue.records {
		if record.id == id {
			return record
		}
	}
	return nil
}

@(private)
snapshot_locked :: proc(queue: ^Queue) -> Queue_Snapshot {
	terminal := 0
	for record in queue.records {
		#partial switch record.state {
		case .Completed, .Failed, .Cancelled, .Timed_Out:
			terminal += 1
		case:
		}
	}
	return {
		waiting = len(queue.waiting),
		running = queue.running_count,
		terminal = terminal,
		concurrency = queue.concurrency,
		paused = queue.paused,
	}
}

@(private)
policy_item :: proc(record: ^Task_Record) -> Policy_Item {
	return {
		id = record.id,
		priority = record.task.priority,
		sequence = record.sequence,
		policy_data = record.task.policy_data,
		label = record.task.label,
		state = record.state,
	}
}

@(private)
emit_locked :: proc(
	queue: ^Queue,
	kind: Event_Kind,
	record: ^Task_Record = nil,
) {
	event := Task_Event{kind = kind}
	if record != nil {
		event.task_id = record.id
		event.state = record.state
		event.code = record.outcome.code
	}
	append(&queue.events, event)
}

@(private)
emit_count_transitions_locked :: proc(
	queue: ^Queue,
	previous_waiting, previous_running: int,
) {
	current_waiting := len(queue.waiting)
	current_running := queue.running_count
	if previous_waiting > 0 && current_waiting == 0 {
		emit_locked(queue, .Empty)
	}
	if previous_running > 0 && current_running == 0 {
		emit_locked(queue, .Pending_Zero)
	}
	if previous_waiting + previous_running > 0 &&
	   current_waiting + current_running == 0 {
		emit_locked(queue, .Idle)
	}
}

@(private)
terminal_state :: proc(state: Task_State) -> bool {
	#partial switch state {
	case .Completed, .Failed, .Cancelled, .Timed_Out:
		return true
	case:
		return false
	}
}

@(private)
release_record_locked :: proc(
	queue: ^Queue,
	record: ^Task_Record,
) -> bool {
	for candidate, index in queue.records {
		if candidate != record {
			continue
		}
		delete(record.task.label, queue.allocator)
		free(record, queue.allocator)
		ordered_remove(&queue.records, index)
		return true
	}
	return false
}

@(private)
remove_waiting_locked :: proc(queue: ^Queue, record: ^Task_Record) -> bool {
	for queued, index in queue.waiting {
		if queued == record {
			ordered_remove(&queue.waiting, index)
			return true
		}
	}
	return false
}

@(private)
compact_events_locked :: proc(queue: ^Queue) {
	if queue.event_offset == 0 {
		return
	}
	if queue.event_offset < 128 && queue.event_offset * 2 < len(queue.events) {
		return
	}
	remaining := len(queue.events) - queue.event_offset
	if remaining > 0 {
		copy(queue.events[:remaining], queue.events[queue.event_offset:])
	}
	resize(&queue.events, remaining)
	queue.event_offset = 0
}

@(private)
reset_fixed_window_locked :: proc(queue: ^Queue, now: time.Tick) {
	if queue.rate_window_started._nsec == 0 {
		queue.rate_window_started = now
		return
	}
	if time.tick_diff(queue.rate_window_started, now) < queue.interval {
		return
	}
	queue.rate_window_started = now
	queue.rate_window_count = 0
	if queue.carryover_interval_count {
		queue.rate_window_count = queue.running_count
	}
	queue.rate_limited_event_active = false
}

@(private)
prune_strict_window_locked :: proc(queue: ^Queue, now: time.Tick) {
	remove_count := 0
	for started in queue.strict_start_ticks {
		if time.tick_diff(started, now) < queue.interval {
			break
		}
		remove_count += 1
	}
	if remove_count > 0 {
		copy(
			queue.strict_start_ticks[:len(queue.strict_start_ticks) - remove_count],
			queue.strict_start_ticks[remove_count:],
		)
		resize(&queue.strict_start_ticks, len(queue.strict_start_ticks) - remove_count)
		queue.rate_limited_event_active = false
	}
}

@(private)
rate_slot_available_locked :: proc(queue: ^Queue, now: time.Tick) -> bool {
	if queue.interval_cap <= 0 {
		return true
	}
	if queue.strict_rate_limit {
		prune_strict_window_locked(queue, now)
		return len(queue.strict_start_ticks) < queue.interval_cap
	}
	reset_fixed_window_locked(queue, now)
	return queue.rate_window_count < queue.interval_cap
}

@(private)
rate_wait_locked :: proc(queue: ^Queue, now: time.Tick) -> time.Duration {
	if queue.interval_cap <= 0 {
		return 0
	}
	if queue.strict_rate_limit {
		prune_strict_window_locked(queue, now)
		if len(queue.strict_start_ticks) < queue.interval_cap {
			return 0
		}
		elapsed := time.tick_diff(queue.strict_start_ticks[0], now)
		return max(time.Nanosecond, queue.interval - elapsed)
	}
	reset_fixed_window_locked(queue, now)
	if queue.rate_window_count < queue.interval_cap {
		return 0
	}
	elapsed := time.tick_diff(queue.rate_window_started, now)
	return max(time.Nanosecond, queue.interval - elapsed)
}

@(private)
record_rate_start_locked :: proc(queue: ^Queue, now: time.Tick) {
	if queue.interval_cap <= 0 {
		return
	}
	if queue.strict_rate_limit {
		append(&queue.strict_start_ticks, now)
	} else {
		reset_fixed_window_locked(queue, now)
		queue.rate_window_count += 1
	}
	queue.rate_limited_event_active = false
}

@(private)
default_waiting_index_locked :: proc(queue: ^Queue) -> int {
	selected := -1
	for record, index in queue.waiting {
		if selected < 0 ||
		   record.task.priority > queue.waiting[selected].task.priority ||
		   record.task.priority == queue.waiting[selected].task.priority &&
		   record.sequence < queue.waiting[selected].sequence {
			selected = index
		}
	}
	return selected
}

@(private)
select_waiting_index_locked :: proc(queue: ^Queue) -> int {
	if queue.policy.select_procedure == nil {
		return default_waiting_index_locked(queue)
	}
	items := make([]Policy_Item, len(queue.waiting), context.temp_allocator)
	defer delete(items, context.temp_allocator)
	for record, index in queue.waiting {
		items[index] = policy_item(record)
	}
	selected := queue.policy.select_procedure(
		items,
		snapshot_locked(queue),
		queue.policy.data,
	)
	if selected < 0 || selected >= len(queue.waiting) {
		return -1
	}
	return selected
}

@(private)
request_cancel_locked :: proc(
	queue: ^Queue,
	record: ^Task_Record,
	timeout: bool,
) -> (Cancel_Call, bool) {
	if terminal_state(record.state) {
		return {}, false
	}
	if record.finishing {
		return {}, false
	}
	if record.state == .Waiting {
		previous_waiting := len(queue.waiting)
		previous_running := queue.running_count
		_ = remove_waiting_locked(queue, record)
		record.cancel_requested = true
		record.timeout_requested = timeout
		if timeout {
			record.state = .Timed_Out
			emit_locked(queue, .Timeout_Requested, record)
			emit_locked(queue, .Timed_Out, record)
		} else {
			record.state = .Cancelled
			emit_locked(queue, .Cancel_Requested, record)
			emit_locked(queue, .Cancelled, record)
		}
		emit_count_transitions_locked(queue, previous_waiting, previous_running)
		sync.cond_broadcast(&queue.condition)
		if record.task.release_on_finish {
			_ = release_record_locked(queue, record)
		}
		return {}, true
	}
	if record.state != .Running {
		return {}, false
	}
	if timeout {
		if record.timeout_requested {
			return {}, false
		}
		record.timeout_requested = true
		record.cancel_requested = true
		emit_locked(queue, .Timeout_Requested, record)
	} else {
		if record.cancel_requested {
			return {}, false
		}
		record.cancel_requested = true
		emit_locked(queue, .Cancel_Requested, record)
	}
	call: Cancel_Call
	if !record.cancel_callback_invoked && record.task.cancel_procedure != nil {
		record.cancel_callback_invoked = true
		record.cancel_callback_pending = true
		call = {
			procedure = record.task.cancel_procedure,
			data = record.task.data,
			record = record,
		}
	}
	sync.cond_broadcast(&queue.condition)
	return call, true
}

@(private)
invoke_cancel_call :: proc(queue: ^Queue, call: Cancel_Call) {
	if call.procedure == nil {
		return
	}
	call.procedure(call.data)
	sync.mutex_lock(&queue.mutex)
	call.record.cancel_callback_pending = false
	sync.cond_broadcast(&queue.condition)
	sync.mutex_unlock(&queue.mutex)
}

@(private)
finish_record_locked :: proc(
	queue: ^Queue,
	record: ^Task_Record,
	outcome: Task_Outcome,
) {
	previous_waiting := len(queue.waiting)
	previous_running := queue.running_count
	record.outcome = outcome
	if record.timeout_requested {
		record.state = .Timed_Out
	} else if record.cancel_requested {
		record.state = .Cancelled
	} else if outcome.failed {
		record.state = .Failed
	} else {
		record.state = .Completed
	}
	queue.running_count -= 1
	if queue.policy.notify_procedure != nil {
		queue.policy.notify_procedure(
			.Finished,
			policy_item(record),
			queue.policy.data,
		)
	}
	#partial switch record.state {
	case .Completed:
		emit_locked(queue, .Completed, record)
	case .Failed:
		emit_locked(queue, .Failed, record)
	case .Cancelled:
		emit_locked(queue, .Cancelled, record)
	case .Timed_Out:
		emit_locked(queue, .Timed_Out, record)
	case:
	}
	if len(queue.waiting) > 0 {
		emit_locked(queue, .Next)
	}
	emit_count_transitions_locked(queue, previous_waiting, previous_running)
	sync.cond_broadcast(&queue.condition)
	if record.task.release_on_finish {
		_ = release_record_locked(queue, record)
	}
}

@(private)
worker_main :: proc(queue: ^Queue) {
	for {
		sync.mutex_lock(&queue.mutex)
		record: ^Task_Record
		for record == nil {
			if queue.stopping {
				if queue.shutdown_mode != .Drain ||
				   len(queue.waiting) == 0 && queue.running_count == 0 {
					sync.mutex_unlock(&queue.mutex)
					return
				}
			}
			if queue.paused || len(queue.waiting) == 0 ||
			   queue.running_count >= queue.concurrency {
				sync.cond_wait(&queue.condition, &queue.mutex)
				continue
			}
			now := now_locked(queue)
			if !rate_slot_available_locked(queue, now) {
				if !queue.rate_limited_event_active {
					emit_locked(queue, .Rate_Limited)
					queue.rate_limited_event_active = true
				}
				delay := rate_wait_locked(queue, now)
				_ = sync.cond_wait_with_timeout(&queue.condition, &queue.mutex, delay)
				continue
			}
			index := select_waiting_index_locked(queue)
			if index < 0 {
				sync.cond_wait(&queue.condition, &queue.mutex)
				continue
			}
			previous_waiting := len(queue.waiting)
			previous_running := queue.running_count
			record = queue.waiting[index]
			ordered_remove(&queue.waiting, index)
			record.state = .Running
			record.started_at = now
			timeout := queue.default_timeout
			if record.task.override_timeout || record.task.timeout > 0 {
				timeout = record.task.timeout
			}
			if timeout > 0 {
				record.deadline = time.tick_add(now, timeout)
			}
			queue.running_count += 1
			record_rate_start_locked(queue, now)
			if queue.policy.notify_procedure != nil {
				queue.policy.notify_procedure(
					.Activated,
					policy_item(record),
					queue.policy.data,
				)
			}
			emit_locked(queue, .Active, record)
			emit_count_transitions_locked(queue, previous_waiting, previous_running)
			sync.cond_broadcast(&queue.condition)
		}
		sync.mutex_unlock(&queue.mutex)

		task_context := Task_Context{
			id = record.id,
			data = record.task.data,
			policy_data = record.task.policy_data,
			_queue = queue,
			_record = record,
		}
		outcome := record.task.procedure(&task_context)

		sync.mutex_lock(&queue.mutex)
		for record.cancel_callback_pending {
			sync.cond_wait(&queue.condition, &queue.mutex)
		}
		record.finishing = true
		finalize_procedure := record.task.finalize_procedure
		finalize_data := record.task.data
		sync.mutex_unlock(&queue.mutex)
		if finalize_procedure != nil {
			finalize_procedure(finalize_data)
		}
		sync.mutex_lock(&queue.mutex)
		finish_record_locked(queue, record, outcome)
		sync.mutex_unlock(&queue.mutex)
	}
}

@(private)
watchdog_main :: proc(queue: ^Queue) {
	cancel_calls := make([dynamic]Cancel_Call, context.temp_allocator)
	defer delete(cancel_calls)
	for {
		resize(&cancel_calls, 0)
		sync.mutex_lock(&queue.mutex)
		if queue.stopping && queue.running_count == 0 {
			sync.mutex_unlock(&queue.mutex)
			return
		}
		now := now_locked(queue)
		has_deadline := false
		for record in queue.records {
			if record.state != .Running ||
			   record.deadline._nsec == 0 ||
			   record.timeout_requested {
				continue
			}
			has_deadline = true
			if time.tick_diff(record.deadline, now) >= 0 {
				if call, changed := request_cancel_locked(queue, record, true); changed &&
				   call.procedure != nil {
					append(&cancel_calls, call)
				}
			}
		}
		// Untimed work and an idle queue need no watchdog ticks. Task starts,
		// completions and shutdown all signal this condition.
		if len(cancel_calls) == 0 {
			if has_deadline {
				_ = sync.cond_wait_with_timeout(&queue.condition, &queue.mutex, 10 * time.Millisecond)
			} else {
				sync.cond_wait(&queue.condition, &queue.mutex)
			}
		}
		sync.mutex_unlock(&queue.mutex)
		for call in cancel_calls {
			invoke_cancel_call(queue, call)
		}
	}
}

@(private)
spawn_worker_locked :: proc(queue: ^Queue) -> bool {
	worker := thread.create_and_start_with_poly_data(queue, worker_main)
	if worker == nil {
		return false
	}
	append(&queue.workers, worker)
	return true
}

queue_init :: proc(queue: ^Queue, options: Queue_Options) -> Init_Error {
	assert(queue != nil)
	if options.concurrency <= 0 {
		return .Invalid_Concurrency
	}
	if options.interval_cap < 0 ||
	   options.interval_cap > 0 && options.interval <= 0 {
		return .Invalid_Rate_Limit
	}
	allocator := options.allocator
	if allocator.procedure == nil {
		allocator = context.allocator
	}
	clock := options.clock_procedure
	if clock == nil {
		clock = default_clock
	}
	queue^ = Queue{
		allocator = allocator,
		next_id = 1,
		next_sequence = 1,
		concurrency = options.concurrency,
		paused = options.start_paused,
		default_timeout = options.default_timeout,
		interval_cap = options.interval_cap,
		interval = options.interval,
		carryover_interval_count = options.carryover_interval_count,
		strict_rate_limit = options.strict_rate_limit,
		clock_procedure = clock,
		clock_data = options.clock_data,
		policy = options.policy,
	}
	queue.records = make([dynamic]^Task_Record, allocator)
	queue.waiting = make([dynamic]^Task_Record, allocator)
	queue.events = make([dynamic]Task_Event, allocator)
	queue.workers = make([dynamic]^thread.Thread, allocator)
	queue.strict_start_ticks = make([dynamic]time.Tick, allocator)
	for _ in 0 ..< options.concurrency {
		if !spawn_worker_locked(queue) {
			queue.stopping = true
			queue.shutdown_mode = .Cancel_All
			sync.cond_broadcast(&queue.condition)
			for worker in queue.workers {
				thread.destroy(worker)
			}
			delete(queue.records)
			delete(queue.waiting)
			delete(queue.events)
			delete(queue.workers)
			delete(queue.strict_start_ticks)
			queue^ = {}
			return .Thread_Creation_Failed
		}
	}
	queue.watchdog = thread.create_and_start_with_poly_data(queue, watchdog_main)
	if queue.watchdog == nil {
		queue.stopping = true
		queue.shutdown_mode = .Cancel_All
		sync.cond_broadcast(&queue.condition)
		for worker in queue.workers {
			thread.destroy(worker)
		}
		delete(queue.records)
		delete(queue.waiting)
		delete(queue.events)
		delete(queue.workers)
		delete(queue.strict_start_ticks)
		queue^ = {}
		return .Thread_Creation_Failed
	}
	return .None
}

add :: proc(queue: ^Queue, task: Task) -> (Task_ID, Add_Error) {
	if queue == nil || task.procedure == nil {
		return 0, .Invalid_Task
	}
	sync.mutex_lock(&queue.mutex)
	defer sync.mutex_unlock(&queue.mutex)
	if queue.stopping {
		return 0, .Queue_Stopping
	}
	record, allocation_error := new(Task_Record, queue.allocator)
	if allocation_error != nil {
		return 0, .Allocation_Failed
	}
	record^ = {
		id = Task_ID(queue.next_id),
		sequence = queue.next_sequence,
		task = task,
		state = .Waiting,
	}
	if len(task.label) > 0 {
		record.task.label = strings.clone(task.label, queue.allocator)
	}
	queue.next_id += 1
	queue.next_sequence += 1
	append(&queue.records, record)
	append(&queue.waiting, record)
	emit_locked(queue, .Added, record)
	queue.rate_limited_event_active = false
	sync.cond_broadcast(&queue.condition)
	return record.id, .None
}

add_all :: proc(
	queue: ^Queue,
	tasks: []Task,
	ids: ^[dynamic]Task_ID,
) -> Add_Error {
	for task in tasks {
		id, add_error := add(queue, task)
		if add_error != .None {
			return add_error
		}
		if ids != nil {
			append(ids, id)
		}
	}
	return .None
}

pause :: proc(queue: ^Queue) {
	if queue == nil {
		return
	}
	sync.mutex_lock(&queue.mutex)
	queue.paused = true
	sync.mutex_unlock(&queue.mutex)
}

start :: proc(queue: ^Queue) {
	if queue == nil {
		return
	}
	sync.mutex_lock(&queue.mutex)
	queue.paused = false
	sync.cond_broadcast(&queue.condition)
	sync.mutex_unlock(&queue.mutex)
}

wake :: proc(queue: ^Queue) {
	if queue == nil {
		return
	}
	sync.mutex_lock(&queue.mutex)
	queue.rate_limited_event_active = false
	sync.cond_broadcast(&queue.condition)
	sync.mutex_unlock(&queue.mutex)
}

cancel_with_state :: proc(
	queue: ^Queue,
	id: Task_ID,
) -> (Task_State, bool) {
	if queue == nil {
		return .Unknown, false
	}
	call: Cancel_Call
	changed := false
	previous_state := Task_State.Unknown
	sync.mutex_lock(&queue.mutex)
	if record := find_record_locked(queue, id); record != nil {
		previous_state = record.state
		call, changed = request_cancel_locked(queue, record, false)
	}
	sync.mutex_unlock(&queue.mutex)
	invoke_cancel_call(queue, call)
	return previous_state, changed
}

cancel :: proc(queue: ^Queue, id: Task_ID) -> bool {
	_, changed := cancel_with_state(queue, id)
	return changed
}

clear :: proc(queue: ^Queue) -> int {
	if queue == nil {
		return 0
	}
	release_records := make(
		[dynamic]^Task_Record,
		context.temp_allocator,
	)
	defer delete(release_records)
	sync.mutex_lock(&queue.mutex)
	defer sync.mutex_unlock(&queue.mutex)
	previous_waiting := len(queue.waiting)
	previous_running := queue.running_count
	for record in queue.waiting {
		record.cancel_requested = true
		record.state = .Cancelled
		emit_locked(queue, .Cancel_Requested, record)
		emit_locked(queue, .Cancelled, record)
		if record.task.release_on_finish {
			append(&release_records, record)
		}
	}
	resize(&queue.waiting, 0)
	for record in release_records {
		_ = release_record_locked(queue, record)
	}
	if previous_waiting > 0 {
		emit_locked(queue, .Cleared)
	}
	emit_count_transitions_locked(queue, previous_waiting, previous_running)
	sync.cond_broadcast(&queue.condition)
	return previous_waiting
}

set_priority :: proc(queue: ^Queue, id: Task_ID, priority: int) -> bool {
	if queue == nil {
		return false
	}
	sync.mutex_lock(&queue.mutex)
	defer sync.mutex_unlock(&queue.mutex)
	record := find_record_locked(queue, id)
	if record == nil || record.state != .Waiting {
		return false
	}
	record.task.priority = priority
	sync.cond_broadcast(&queue.condition)
	return true
}

set_concurrency :: proc(queue: ^Queue, concurrency: int) -> bool {
	if queue == nil || concurrency <= 0 {
		return false
	}
	sync.mutex_lock(&queue.mutex)
	defer sync.mutex_unlock(&queue.mutex)
	if queue.stopping {
		return false
	}
	for len(queue.workers) < concurrency {
		if !spawn_worker_locked(queue) {
			return false
		}
	}
	queue.concurrency = concurrency
	sync.cond_broadcast(&queue.condition)
	return true
}

set_default_timeout :: proc(queue: ^Queue, timeout: time.Duration) {
	if queue == nil {
		return
	}
	sync.mutex_lock(&queue.mutex)
	queue.default_timeout = max(time.Duration(0), timeout)
	sync.mutex_unlock(&queue.mutex)
}

snapshot :: proc(queue: ^Queue) -> Queue_Snapshot {
	if queue == nil {
		return {}
	}
	sync.mutex_lock(&queue.mutex)
	defer sync.mutex_unlock(&queue.mutex)
	return snapshot_locked(queue)
}

task_info :: proc(queue: ^Queue, id: Task_ID) -> (Task_Info, bool) {
	if queue == nil {
		return {}, false
	}
	sync.mutex_lock(&queue.mutex)
	defer sync.mutex_unlock(&queue.mutex)
	record := find_record_locked(queue, id)
	if record == nil {
		return {}, false
	}
	return {
		id = record.id,
		state = record.state,
		priority = record.task.priority,
		outcome = record.outcome,
		cancel_requested = record.cancel_requested,
		timeout_requested = record.timeout_requested,
		label = record.task.label,
	}, true
}

running_tasks :: proc(
	queue: ^Queue,
	destination: ^[dynamic]Task_Info,
) {
	if queue == nil || destination == nil {
		return
	}
	sync.mutex_lock(&queue.mutex)
	defer sync.mutex_unlock(&queue.mutex)
	resize(destination, 0)
	for record in queue.records {
		if record.state != .Running {
			continue
		}
		append(destination, Task_Info{
			id = record.id,
			state = record.state,
			priority = record.task.priority,
			outcome = record.outcome,
			cancel_requested = record.cancel_requested,
			timeout_requested = record.timeout_requested,
			label = record.task.label,
		})
	}
}

poll_event :: proc(queue: ^Queue) -> (Task_Event, bool) {
	if queue == nil {
		return {}, false
	}
	sync.mutex_lock(&queue.mutex)
	defer sync.mutex_unlock(&queue.mutex)
	if queue.event_offset >= len(queue.events) {
		compact_events_locked(queue)
		return {}, false
	}
	event := queue.events[queue.event_offset]
	queue.event_offset += 1
	compact_events_locked(queue)
	return event, true
}

cancel_requested :: proc(task_context: ^Task_Context) -> bool {
	if task_context == nil || task_context._queue == nil ||
	   task_context._record == nil {
		return false
	}
	queue := task_context._queue
	sync.mutex_lock(&queue.mutex)
	defer sync.mutex_unlock(&queue.mutex)
	return task_context._record.cancel_requested
}

timeout_requested :: proc(task_context: ^Task_Context) -> bool {
	if task_context == nil || task_context._queue == nil ||
	   task_context._record == nil {
		return false
	}
	queue := task_context._queue
	sync.mutex_lock(&queue.mutex)
	defer sync.mutex_unlock(&queue.mutex)
	return task_context._record.timeout_requested
}

wait_task :: proc(
	queue: ^Queue,
	id: Task_ID,
	timeout: time.Duration = 0,
) -> (Task_Info, bool) {
	if queue == nil {
		return {}, false
	}
	started := time.tick_now()
	sync.mutex_lock(&queue.mutex)
	defer sync.mutex_unlock(&queue.mutex)
	for {
		record := find_record_locked(queue, id)
		if record == nil {
			return {}, false
		}
		if terminal_state(record.state) {
			return Task_Info{
				id = record.id,
				state = record.state,
				priority = record.task.priority,
				outcome = record.outcome,
				cancel_requested = record.cancel_requested,
				timeout_requested = record.timeout_requested,
				label = record.task.label,
			}, true
		}
		if timeout <= 0 {
			sync.cond_wait(&queue.condition, &queue.mutex)
			continue
		}
		remaining := timeout - time.tick_diff(started, time.tick_now())
		if remaining <= 0 ||
		   !sync.cond_wait_with_timeout(&queue.condition, &queue.mutex, remaining) {
			return {}, false
		}
	}
}

@(private)
condition_satisfied_locked :: proc(
	queue: ^Queue,
	condition: Wait_Condition,
	size_limit: int,
) -> bool {
	switch condition {
	case .Empty:
		return len(queue.waiting) == 0
	case .Pending_Zero:
		return queue.running_count == 0
	case .Idle:
		return len(queue.waiting) == 0 && queue.running_count == 0
	case .Size_At_Most:
		return len(queue.waiting) <= size_limit
	}
	return false
}

wait_until :: proc(
	queue: ^Queue,
	condition: Wait_Condition,
	size_limit: int = 0,
	timeout: time.Duration = 0,
) -> bool {
	if queue == nil || condition == .Size_At_Most && size_limit < 0 {
		return false
	}
	started := time.tick_now()
	sync.mutex_lock(&queue.mutex)
	defer sync.mutex_unlock(&queue.mutex)
	for !condition_satisfied_locked(queue, condition, size_limit) {
		if timeout <= 0 {
			sync.cond_wait(&queue.condition, &queue.mutex)
			continue
		}
		remaining := timeout - time.tick_diff(started, time.tick_now())
		if remaining <= 0 ||
		   !sync.cond_wait_with_timeout(&queue.condition, &queue.mutex, remaining) {
			return false
		}
	}
	return true
}

release :: proc(queue: ^Queue, id: Task_ID) -> bool {
	if queue == nil {
		return false
	}
	sync.mutex_lock(&queue.mutex)
	defer sync.mutex_unlock(&queue.mutex)
	for record in queue.records {
		if record.id != id || !terminal_state(record.state) {
			continue
		}
		return release_record_locked(queue, record)
	}
	return false
}

queue_destroy :: proc(
	queue: ^Queue,
	mode: Shutdown_Mode = .Cancel_All,
) {
	if queue == nil || queue.allocator.procedure == nil {
		return
	}
	cancel_calls := make([dynamic]Cancel_Call, context.temp_allocator)
	defer delete(cancel_calls)
	sync.mutex_lock(&queue.mutex)
	queue.stopping = true
	queue.paused = false
	queue.shutdown_mode = mode
	if mode != .Drain {
		previous_waiting := len(queue.waiting)
		previous_running := queue.running_count
		for record in queue.waiting {
			record.cancel_requested = true
			record.state = .Cancelled
			emit_locked(queue, .Cancel_Requested, record)
			emit_locked(queue, .Cancelled, record)
		}
		resize(&queue.waiting, 0)
		emit_count_transitions_locked(queue, previous_waiting, previous_running)
	}
	if mode == .Cancel_All {
		for record in queue.records {
			if record.state != .Running {
				continue
			}
			if call, changed := request_cancel_locked(queue, record, false); changed &&
			   call.procedure != nil {
				append(&cancel_calls, call)
			}
		}
	}
	sync.cond_broadcast(&queue.condition)
	sync.mutex_unlock(&queue.mutex)
	for call in cancel_calls {
		invoke_cancel_call(queue, call)
	}
	for worker in queue.workers {
		thread.destroy(worker)
	}
	if queue.watchdog != nil {
		thread.destroy(queue.watchdog)
	}
	for record in queue.records {
		delete(record.task.label, queue.allocator)
		free(record, queue.allocator)
	}
	delete(queue.records)
	delete(queue.waiting)
	delete(queue.events)
	delete(queue.workers)
	delete(queue.strict_start_ticks)
	queue^ = {}
}
