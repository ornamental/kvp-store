package dev.ornamental.storage.kvp;

/**
 * Interface for repository keys.
 */
public interface Key {

	/**
	 * Returns the binary representation of the key.
	 * @return the binary representation of the key
	 */
	byte[] get();
}
