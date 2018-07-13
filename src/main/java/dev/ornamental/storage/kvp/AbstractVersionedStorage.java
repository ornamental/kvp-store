package dev.ornamental.storage.kvp;

import java.util.Optional;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import dev.ornamental.storage.StorageException;
import dev.ornamental.storage.kvp.value.Value;

/**
 * This is a skeleton implementation of an MVCC storage allowing any number of
 * concurrent readers and, at the same time, no more than one writer. Maintains the
 * set of storage versions for which a reader transaction exists.
 */
public abstract class AbstractVersionedStorage implements VersionedStorage {

	/**
	 * The descendants of this class must preserve thread safety.
	 */
	protected abstract class AbstractReader implements Reader {

		protected final long version;

		protected final AtomicInteger closed = new AtomicInteger();

		public AbstractReader(long version) {
			this.version = version;
		}

		@Override
		public long getVersion() {
			return version;
		}

		@Override
		public abstract boolean exists(Key key) throws StorageException;

		@Override
		public abstract Optional<Value> get(Key key) throws StorageException;

		@Override
		public void close() {
			if (closed.compareAndSet(0, 1)) {
				readerVersions.compute(version, (v, c) -> c == 1 ? null : c - 1);
			}
		}
	}

	protected abstract class AbstractWriter implements Writer {

		protected Reader read;

		protected boolean hasChanges;

		public AbstractWriter(long version) {
			this.read = getReader(version);
			this.hasChanges = false;
		}

		@Override
		public long getVersion() {
			return read.getVersion();
		}

		@Override
		public boolean exists(Key key) throws StorageException {
			return read.exists(key);
		}

		@Override
		public Optional<Value> get(Key key) throws StorageException {
			return read.get(key);
		}

		@Override
		public abstract void put(Key key, Value value) throws StorageException;

		@Override
		public abstract boolean remove(Key key) throws StorageException;

		@Override
		public void commit() throws StorageException {
			long newVersion = commitImpl();
			if (newVersion != read.getVersion()) {
				afterCommit(newVersion);
			}
		}

		@Override
		public abstract void rollback() throws StorageException;

		@Override
		public void close() throws StorageException {
			read.close();
			rollback();
			synchronized (writerLock) {
				writerExists = false;
				writerLock.notifyAll();
			}
		}

		/**
		 * Performs the commit of changes made by the writer transaction since the previous call
		 * to this method or to {@link #rollback()}.
		 * @return the storage version after the commit (may be the current version if there is
		 * nothing to commit, otherwise must be greater than the current version)
		 */
		protected abstract long commitImpl() throws StorageException;

		protected void afterCommit(long newVersion) throws StorageException {
			if (this.read != null) {
				try {
					this.read.close();
				} catch (StorageException e) {
					throw e;
				} catch (Exception e) {
					throw new StorageException("Could not close the underlying reader.", e);
				}
			}
			this.read = getReader(newVersion);
		}
	}

	protected final Object versionLock = new Object();

	protected long currentVersion;

	protected final ConcurrentNavigableMap<Long, Integer> readerVersions;

	protected final Object writerLock = new Object();

	protected boolean writerExists = false;

	/**
	 * It is the duty of descendant classes to initialize the current version.
	 */
	protected AbstractVersionedStorage() {
		readerVersions = new ConcurrentSkipListMap<>();
	}

	@Override
	public Reader getReader() {
		synchronized (versionLock) {
			return getReader(currentVersion);
		}
	}

	@Override
	public Writer getWriter() throws InterruptedException, StorageException {
		synchronized (writerLock) {
			while (writerExists) {
				writerLock.wait();
			}
			writerExists = true;
		}

		synchronized (versionLock) {
			return produceWriter();
		}
	}

	protected Reader getReader(long version) {
		Reader result = produceReader(version);
		readerVersions.compute(version, (v, c) -> c == null ? 1 : c + 1);
		return result;
	}

	protected abstract Reader produceReader(long version);

	protected abstract Writer produceWriter() throws StorageException;
}
