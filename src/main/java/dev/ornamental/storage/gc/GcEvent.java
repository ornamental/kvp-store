package dev.ornamental.storage.gc;

/**
 * Enumeration of various events produced by a GC scheduler in debug mode
 */
public enum GcEvent {

	QUEUE_WAIT_START,

	QUEUE_WAIT_END,

	ENQUEUE,

	COLLECT_ONE_START,

	COLLECT_ONE_END,

	COLLECT_RESCHEDULE,

	COLLECT_NO_RESCHEDULE,

	COLLECT_ERROR,

	AGENT_REGISTERED,

	AGENT_DEREGISTERED,

	AGENT_STOPPED, CANCEL_SCHEDULED;
}
