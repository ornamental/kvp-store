package dev.ornamental.storage.kvp;

import dev.ornamental.storage.StorageException;

/**
 * Represents a storage with multi-version concurrency control. Readers and writer do not block
 * each other while there may only exist one writer at a time. Each reader observes the storage
 * state which was actual at the moment the when reader was created. The writer operates on
 * the last committed storage state.
 */
public interface VersionedStorage extends AutoCloseable {

	/**
	 * Returns the reader for the most recent version of data in the storage.<br>
	 * This invocation is non-blocking.
	 * @return the reader for the current storage version
	 */
	Reader getReader() throws StorageException;

	/**
	 * Returns the writer for the new storage version.<br>
	 * This invocation only blocks if there already exists an active writer.
	 * @return the writer for the new storage version whose data
	 * is based on the current storage version
	 * @throws InterruptedException if the requester thread is interrupted while awaiting a writer
	 */
	Writer getWriter() throws InterruptedException, StorageException;
}
