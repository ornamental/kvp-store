package dev.ornamental.util.threading;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class is used for value-based synchronization. In order to use it
 * with objects of type T, the type must implement equals() and hashCode() in
 * a meaningful way.
 * Note that this class may be useful only if
 * <ul>
 *     <li>the operations to run under locks are long;</li>
 *     <li>the expected contention on specific objects is low.</li>
 * </ul>
 * In case of short operations, contention on a common object may be a better
 * option as inflicting less overhead.
 * An option making it possible to atomically lock on all the values of the type
 * is also available upon instance creation.
 */
public class LockOnValue<T> {

	private static class WrappedLock<T> {

		private final T key;

		private final ReentrantLock lock = new ReentrantLock();

		private AtomicLong usages = new AtomicLong(0L);

		public WrappedLock(T t) {
			this.key = t;
		}

		public void increment() {
			usages.incrementAndGet();
		}

		public long decrement() {
			return usages.decrementAndGet();
		}

		public void lock() {
			lock.lock();
		}

		public void unlock() {
			lock.unlock();
		}

		public T getKey() {
			return key;
		}

		public boolean isUsed() {
			return usages.get() > 0;
		}

		public boolean isHeldByCurrentThread() {
			return lock.isHeldByCurrentThread();
		}
	}

	private final Map<T, WrappedLock<T>> locks = new HashMap<>();

	private final ReadWriteLock masterLock;

	private ThreadLocal<Map<T, WrappedLock<T>>> heldLocks = ThreadLocal.withInitial(HashMap::new);

	/**
	 * Creates a value-based lock instance.
	 * @param allowLockAll flag to be set if the global lock oparation must be available; unset the flag if
	 * such an operation is not needed
	 */
	public LockOnValue(boolean allowLockAll) {
		masterLock = allowLockAll ? new ReentrantReadWriteLock() : null;
	}

	/**
	 * Shows if this instance supports global locking (lockAll() and unlockAll() calls).
	 * @return true if and only if the lockAll() and unlockAll() calls are supported by this instance
	 */
	public boolean allowsLockAll() {
		return masterLock != null;
	}

	/**
	 * Performs locking on a concrete non-null value of the type.
	 * @param t the value to lock on
	 */
	public void lock(T t) {
		WrappedLock<T> lock = heldLocks.get().get(t);
		if (lock != null) {
			lock.increment();
			if (masterLock != null) {
				masterLock.readLock().lock();
			}
			lock.lock();
		} else {
			synchronized (locks) {
				lock = locks.computeIfAbsent(t, WrappedLock::new);
				lock.increment();
			}
			if (masterLock != null) {
				masterLock.readLock().lock();
			}
			lock.lock();
			heldLocks.get().put(t, lock);
		}
	}

	/**
	 * Removes the lock on a concrete value. In order for the operation to succeed,
	 * the thread must own a lock on this value.
	 * @param t the value to unlock
	 */
	public void unlock(T t) {
		WrappedLock<T> lock = heldLocks.get().get(t);
		if (lock == null || !t.equals(lock.getKey())) {
			throw new IllegalMonitorStateException("The thread does not hold a lock on this object.");
		}

		lock.unlock();
		if (masterLock != null) {
			masterLock.readLock().unlock();
		}
		if (!lock.isHeldByCurrentThread()) {
			heldLocks.get().remove(t);
		}

		if (lock.usages.get() == 1) {
			synchronized (locks) {
				if (lock.decrement() == 0) {
					locks.remove(t);
				}
			}
		}
	}

	/**
	 * Performs global locking (on all the values at once). While this lock is held, no other
	 * thread is able to get a lock on either a concrete value nor on all the values at once.
	 * In order to use this function, the lock must be created with allowLockAll option turned on.
	 * While a global lock may be taken recursively, no thread, including the owning thread, may
	 * acquire locks on concrete values.
	 */
	public void lockAll() {
		if (masterLock == null) {
			throw new UnsupportedOperationException("This lock was created as not allowing global locking.");
		}
		masterLock.writeLock().lock();
	}

	/**
	 * Removes the global lock (lock on all the values). To succeed, the calling thread must own
	 * a global lock. In order to use this function, the lock must be created with allowLockAll option turned on.
	 * This operation cannot be used to release the locks on concrete values held by the invoking thread.
	 */
	public void unlockAll() {
		if (masterLock == null) {
			throw new UnsupportedOperationException("This lock was created as not allowing global locking.");
		}
		masterLock.writeLock().unlock();
	}
}
