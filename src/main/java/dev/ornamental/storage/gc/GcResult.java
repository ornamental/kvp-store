package dev.ornamental.storage.gc;

/**
 * Enumeration of possible results of a GC session on a storage
 */
public enum GcResult {

	/**
	 * The session is interrupted as the GC agent is deregistered or the GC is stopped altogether.
	 */
	INTERRUPTED,

	/**
	 * The session could not continue for the agent and must be rescheduled for later.
	 */
	RECORDS_REMAIN,

	/**
	 * GC has freed at least all the memory that could be reclaimed by the time
	 * the session has begun.
	 */
	DONE;

	/**
	 * Returns the least successful session issue of the two.
	 * @param a a session issue
	 * @param b another session issue
	 * @return the least successful session issue of the two
	 */
	public static GcResult merge(GcResult a, GcResult b) {
		return a.compareTo(b) < 0 ? a : b;
	}
}
