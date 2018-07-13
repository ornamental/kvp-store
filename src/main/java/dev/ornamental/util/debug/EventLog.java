package dev.ornamental.util.debug;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class represents an in-memory log with concurrent access. Each event
 * is a tuple of event type (element of an enumeration), timestamp, and event arguments.<br>
 * The log is meant to be used for debugging and is able to store only the last <code>n</code> entries.
 * @param <T> the enumeration type whose elements stand for the supported event types
 */
public class EventLog<T extends Enum<T>> {

	/**
	 * This class represents a logged event.
	 * @param <Q> the enumeration type whose elements stand for the supported event types
	 */
	public static class EventEntry<Q extends Enum<Q>> {

		private final Q event;

		private final long timestamp;

		private final Object[] args;

		/**
		 * Creates a new log entry.
		 * @param event the event type
		 * @param args the event data (arguments)
		 */
		public EventEntry(Q event, Object[] args) {
			this.event = event;
			this.args = args;
			this.timestamp = System.currentTimeMillis(); // nanoTime() is too slow
		}

		/**
		 * Returns the event type.
		 * @return the event type
		 */
		public Q getEvent() {
			return event;
		}

		/**
		 * Returns the timestamp of the event.
		 * @return the timestamp of the event (milliseconds since UNIX epoch)
		 */
		public long getTimeMillis() {
			return timestamp;
		}

		/**
		 * Returns the event data.
		 * @return the array containing the event arguments
		 */
		public Object[] getArgs() {
			return args;
		}
	}

	private final long maxEvents;

	private final AtomicLong lastEvent = new AtomicLong();

	private final ConcurrentNavigableMap<Long, EventEntry<T>> log = new ConcurrentSkipListMap<>();

	/**
	 * Creates a log instance with unlimited entry count.
	 */
	public EventLog() {
		this.maxEvents = Long.MAX_VALUE;
	}

	/**
	 * Creates a log instance with a specific capacity.
	 * @param maxEvents the maximum number of events to preserve in the log
	 */
	public EventLog(long maxEvents) {
		if (maxEvents < 1) {
			throw new IllegalArgumentException("The event limit must be set.");
		}
		this.maxEvents = maxEvents;
	}

	private void log(EventEntry<T> event) {
		long id = lastEvent.getAndIncrement();
		log.put(id, event);
		if (id >= maxEvents) {
			log.remove(id - maxEvents);
		}
	}

	/**
	 * Copies the accumulated log entries to a list, the oldest entries first.
	 * @return the copy of the list of logged entries
	 */
	public List<EventEntry<T>> get() {
		return new ArrayList<>(log.values());
	}

	/**
	 * This is a utility method for conditional logging. It adds an entry to the specified log
	 * unless the first argument is <code>null</code>, in which case this call is a no-op.
	 * @param log the target log (may be <code>null</code>
	 * @param event the event type
	 * @param args the event data (arguments)
	 * @param <Q> the enumeration type whose elements stand for the supported event types
	 */
	public static <Q extends Enum<Q>> void log(EventLog<Q> log, Q event, Object... args) {
		if (log != null) {
			log.log(new EventEntry<Q>(event, args));
		}
	}
}
