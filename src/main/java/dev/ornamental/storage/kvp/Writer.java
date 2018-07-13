package dev.ornamental.storage.kvp;

import dev.ornamental.storage.StorageException;
import dev.ornamental.storage.kvp.value.Value;

/**
 * Represents read & modify transactions on a storage.<br>
 * The implementing classes are not required to allow concurrent usage.
 */
public interface Writer extends Reader {

	/**
	 * Adds or replaces an entry by its key.
	 * @param key the key to add or replace by
	 * @param value the value to associate with the key
	 */
	void put(Key key, Value value) throws StorageException;

	/**
	 * Removes a stored entry by its key.
	 * @param key the key to remove by
	 * @return {@code true} if and only if the entry was present
	 */
	boolean remove(Key key) throws StorageException;

	/**
	 * Commits the changes made using this instance since last call to this method
	 * or to {@link #rollback()} and increments the storage version
	 * (if there were no changes, this last action is optional).
	 */
	void commit() throws StorageException;

	/**
	 * Rolls back the uncommitted changes (the ones made since last call to this method
	 * or {@link #commit()}).
	 * Does nothing if there were no changes.
	 */
	void rollback() throws StorageException;
}
