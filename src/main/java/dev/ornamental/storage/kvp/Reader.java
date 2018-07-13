package dev.ornamental.storage.kvp;

import java.util.Optional;

import dev.ornamental.storage.StorageException;
import dev.ornamental.storage.kvp.value.Value;

/**
 * This interface represents read-only transactions on a storage.<br>
 * The implementing classes must either allow concurrent usage or explicitly
 * state the thread unsafety.
 */
public interface Reader extends VersionedTransaction {

	/**
	 * Checks if an entry with the specified key exists.
	 * @param key the key to check the existence of
	 * @return {@code true} if and only if an entry with the specified key exists
	 */
	boolean exists(Key key) throws StorageException;

	/**
	 * Reads the value associated with the specified key.
	 * @param key the key to read the associated value of
	 * @return the value associated with the key, if one is associated with the key; otherwise, empty
	 */
	Optional<Value> get(Key key) throws StorageException;
}
