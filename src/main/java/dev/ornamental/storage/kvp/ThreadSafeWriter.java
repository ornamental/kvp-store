package dev.ornamental.storage.kvp;

import java.util.Optional;

import dev.ornamental.storage.StorageException;
import dev.ornamental.storage.kvp.value.Value;
import dev.ornamental.util.threading.LockOnValue;

/**
 * Note that this wrapper class might only be used if the key type's
 * equality notion (defined by {@link #equals(Object)} and {@link #hashCode()})
 * is compatible with the storage key resolution policy (i. e. writes to non-equal
 * keys cannot cause writing to the same location).
 */
public class ThreadSafeWriter implements Writer {

	private final Writer write;

	private final Object transactionStateLock = new Object();

	private final LockOnValue<Key> keyLock = new LockOnValue<Key>(true);

	public ThreadSafeWriter(Writer write) {
		this.write = write;
	}

	public Writer getUnderlyingWriter() {
		return write;
	}

	@Override
	public long getVersion() {
		synchronized (transactionStateLock) {
			return write.getVersion();
		}
	}

	@Override
	public void put(Key key, Value value) throws StorageException {
		keyLock.lock(key);
		try {
			write.put(key, value);
		} finally {
			keyLock.unlock(key);
		}
	}

	@Override
	public boolean remove(Key key) throws StorageException {
		keyLock.lock(key);
		try {
			return write.remove(key);
		} finally {
			keyLock.unlock(key);
		}
	}

	@Override
	public boolean exists(Key key) throws StorageException {
		// TODO [LOW] one writer and multiple readers may be implemented using RW-lock on value
		keyLock.lock(key);
		try {
			return write.exists(key);
		} finally {
			keyLock.unlock(key);
		}
	}

	@Override
	public Optional<Value> get(Key key) throws StorageException {
		keyLock.lock(key);
		try {
			return write.get(key);
		} finally {
			keyLock.unlock(key);
		}
	}

	@Override
	public void commit() throws StorageException {
		synchronized (transactionStateLock) {
			keyLock.lockAll();
			try {
				write.commit();
			} finally {
				keyLock.unlockAll();
			}
		}
	}

	@Override
	public void rollback() throws StorageException {
		synchronized (transactionStateLock) {
			keyLock.lockAll();
			try {
				write.rollback();
			} finally {
				keyLock.unlockAll();
			}
		}
	}

	@Override
	public void close() throws StorageException {
		synchronized (transactionStateLock) {
			keyLock.lockAll();
			try {
				write.close();
			} finally {
				keyLock.unlockAll();
			}
		}
	}
}
