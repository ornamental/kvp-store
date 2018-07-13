package dev.ornamental.storage.kvp;

import dev.ornamental.storage.StorageException;

/**
 * Reports the version of the storage a transaction acts upon.
 */
public interface VersionedTransaction extends AutoCloseable {

	/**
	 * Reports the version of the storage the transaction acts upon
	 * (the version of data visible through it).<br>
	 * A reader reports the version of data actual at the moment of its creation.<br>
	 * A writer reports the next (uncommitted) version which is only visible to it.
	 * @return the version of the storage the transaction acts upon
	 */
	long getVersion();

	@Override
	void close() throws StorageException;
}
