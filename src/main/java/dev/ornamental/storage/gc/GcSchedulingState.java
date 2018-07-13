package dev.ornamental.storage.gc;

/**
 * GC scheduling status for a storage.
 */
public enum GcSchedulingState {

	/**
	 * No GC is pending for the storage.
	 */
	NOT_PENDING,

	/**
	 * A GC is deferred until later.
	 */
	DEFERRED,

	/**
	 * A GC is pending.
	 */
	ENQUEUED;

	public static GcSchedulingState merge(GcSchedulingState a, GcSchedulingState b) {
		return a.compareTo(b) > 0 ? a : b;
	}
}
