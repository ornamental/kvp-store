package dev.ornamental.storage.gc;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import dev.ornamental.util.debug.EventLog;
import dev.ornamental.util.threading.DaemonThreadFactory;

/**
 * Generic single-threaded garbage collection scheduler. A storage meant to take advantage of
 * this scheduler should register a GC agent and report the events which may result in GC becoming
 * necessary.
 */
public final class GcScheduler implements AutoCloseable {

	// private static final Logger log = LoggerFactory.getLogger(GcScheduler.class);

	private static final long INTERVAL = 10_000L; // milliseconds

	private final ScheduledExecutorService timer;

	private final Thread gcThread;

	private final Map<GcAgent, GcState> states = new HashMap<>(); // synchronized on self

	private final Map<GcAgent, GcSchedulingState> flags = new HashMap<>(); // synchronized on self

	private final LinkedList<GcAgent> queue = new LinkedList<>(); // synchronized on flags

	private final Map<GcAgent, ScheduledFuture<?>> scheduledTasks = new HashMap<>(); // synchronized on flags

	private final EventLog<GcEvent> eventLog;

	/**
	 * Creates a new scheduler. At least one daemon thread is created.<br>
	 * If a system property <code>repo.gc.eventLog</code> is set (to any <code>long</code> value),
	 * then a debug event log will be maintained, containing no more than the specified number of entries.
	 */
	public GcScheduler() {

		String logCapacity = System.getProperty("repo.gc.eventLog");
		this.eventLog = logCapacity == null ? null : new EventLog<>(Long.parseLong(logCapacity));

		this.timer = Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory());
		this.gcThread = new Thread(this::gc, "file-repo-gc");
		this.gcThread.setDaemon(true);
		this.gcThread.start();
	}

	/**
	 * Returns the debug event log of the scheduler.
	 * @return the debug event log (null if there
	 */
	public List<EventLog.EventEntry<GcEvent>> getEventLog() {
		return eventLog == null ? null : eventLog.get();
	}

	private void gc() {
		// log.info("Starting the file storage GC thread.");
		try {
			while (!Thread.currentThread().isInterrupted()) {
				GcAgent currentAgent;
				GcState state;
				synchronized (flags) {
					while (queue.isEmpty()) {
						EventLog.log(eventLog, GcEvent.QUEUE_WAIT_START);
						flags.wait();
						EventLog.log(eventLog, GcEvent.QUEUE_WAIT_END);
					}

					currentAgent = queue.poll();
					flags.put(currentAgent, GcSchedulingState.NOT_PENDING);
				}
				synchronized (states) {
					state = states.get(currentAgent);
				}
				if (state == null) { // deregistered
					continue;
				}

				GcResult gcResult = collectOne(currentAgent, state);
				if (gcResult == GcResult.RECORDS_REMAIN) {
					synchronized (flags) {
						boolean schedule = flags.replace(
							currentAgent, GcSchedulingState.NOT_PENDING, GcSchedulingState.DEFERRED);
						if (schedule) {
							ScheduledFuture<?> task = timer.schedule(
								() -> enqueueGc(currentAgent),
								INTERVAL, TimeUnit.MILLISECONDS);
							EventLog.log(
								eventLog, GcEvent.COLLECT_RESCHEDULE, currentAgent, task);
							scheduledTasks.put(currentAgent, task);
						} else {
							EventLog.log(eventLog, GcEvent.COLLECT_NO_RESCHEDULE);
						}
					}
				}
			}
		} catch (InterruptedException e) {
			// expected as a signal to stop the GC thread,no actions necessary
		} catch (Throwable t) {
			// log.error("An unexpected error has occurred in the file storage GC thread.", t);
			throw t;
		} finally {
			// log.info("The file storage GC thread ends.");
		}
	}

	private GcResult collectOne(GcAgent gcAgent, GcState state)
		throws InterruptedException {

		BooleanSupplier interruptionFlag =
			() -> Thread.currentThread().isInterrupted() || state.isCancelled();
		GcResult result;
		if (state.gcStart()) {
			try {
				EventLog.log(eventLog, GcEvent.COLLECT_ONE_START, gcAgent);
				result = gcAgent.gc(interruptionFlag);
				EventLog.log(eventLog, GcEvent.COLLECT_ONE_END, gcAgent, result);
			} catch (Throwable e) {
				// log.error("An error has occurred in one of file repositories' GCs.", e);
				EventLog.log(eventLog, GcEvent.COLLECT_ERROR, e);
				deregister(gcAgent);
				result = GcResult.DONE;
			}
			state.gcEnd();
		} else {
			result = GcResult.INTERRUPTED;
		}

		return result;
	}

	/**
	 * The same agent must not be registered more than once. If a storage
	 * wants to register an agent, it must be a new instance each time.
	 */
	public void register(GcAgent gcAgent) {
		boolean added;
		synchronized (states) {
			added = states.putIfAbsent(gcAgent, new GcState()) == null;
		}

		if (added) {
			synchronized (flags) {
				flags.put(gcAgent, GcSchedulingState.NOT_PENDING);
			}
			EventLog.log(eventLog, GcEvent.AGENT_REGISTERED, gcAgent);
			enqueueGc(gcAgent);
		}
	}

	public void deregister(GcAgent gcAgent) throws InterruptedException {
		GcState gcState;
		synchronized (states) {
			gcState = states.remove(gcAgent);
		}
		ScheduledFuture<?> task;
		synchronized (flags) {
			if (flags.remove(gcAgent) == GcSchedulingState.ENQUEUED) {
				queue.remove(gcAgent);
			}
			task = scheduledTasks.remove(gcAgent);
		}
		if (task != null) {
			task.cancel(false);
			EventLog.log(eventLog, GcEvent.CANCEL_SCHEDULED, gcAgent, task);
		}

		if (gcState != null) {
			EventLog.log(eventLog, GcEvent.AGENT_DEREGISTERED, gcAgent);
			gcState.cancel();
			gcState.awaitStop();
			EventLog.log(eventLog, GcEvent.AGENT_STOPPED, gcAgent);
		}
	}

	/**
	 * Informs the scheduler that an agent needs a garbage collection to be enqueued.
	 * @param gcAgent the notifying garbage collection agent
	 */
	public void enqueueGc(GcAgent gcAgent) {
		synchronized (flags) {
			GcSchedulingState previous = flags.replace(gcAgent, GcSchedulingState.ENQUEUED);
			if (previous != null // not deregistered
				&& previous != GcSchedulingState.ENQUEUED) {

				EventLog.log(eventLog, GcEvent.ENQUEUE, gcAgent);
				queue.add(gcAgent);
				ScheduledFuture<?> future = scheduledTasks.remove(gcAgent);
				if (future != null) {
					future.cancel(false);
					EventLog.log(eventLog, GcEvent.CANCEL_SCHEDULED, gcAgent, future);
				}

				flags.notifyAll();
			}
		}
	}

	@Override
	public void close() throws InterruptedException {
		gcThread.interrupt();
		gcThread.join();
		timer.shutdownNow();
		timer.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
	}
}
