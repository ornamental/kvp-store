package dev.ornamental.storage.gc;

/**
 * This class represents a GC process state for a specific storage.
 */
public final class GcState {

	/**
	 * The cancellation flag for GC on the storage
	 */
	private volatile boolean cancelled = false;

	private final Object lock = new Object();

	private boolean stopped = false; // synchronized on lock

	private boolean isCollecting = false; // synchronized on lock

	/**
	 * Indicates if garbage collections on the storage is cancelled.
	 * @return the cancellation flag
	 */
	public boolean isCancelled() {
		return cancelled;
	}

	/**
	 * Marks the garbage collection process as cancelled.
	 */
	public void cancel() {
		cancelled = true;
		synchronized (lock) {
			if (!isCollecting) {
				stopped = true;
				lock.notifyAll();
			}
		}
	}

	/**
	 * Marks the garbage collection session as started, unless GCs has been cancelled on the storage.
	 * @return <code>true</code> if and only if the GCs has not been cancelled on the storage
	 */
	public boolean gcStart() {
		synchronized (lock) {
			if (cancelled) {
				return false;
			}
			isCollecting = true;
		}

		return true;
	}

	/**
	 * Marks the garbage collection session as ended. If the GCs has been cancelled on the storage,
	 * passes to the stopped state and notifies the listeners awaiting the termination.
	 */
	public void gcEnd() {
		synchronized (lock) {
			isCollecting = false;
			if (cancelled) {
				stopped = true;
				lock.notifyAll();
			}
		}
	}

	/**
	 * Awaits the garbage collection termination. After this method returns, no GC session
	 * is running, and none will be started afterwards.
	 */
	public void awaitStop() throws InterruptedException {
		synchronized (lock) {
			while (!stopped) {
				lock.wait();
			}
		}
	}
}
