package dev.ornamental.storage.kvp;

/**
 * Interface for repository keys.<br/>
 * The implementations <em>must</em> override {@link #equals(Object)} and {@link #hashCode()}
 * so that the resulting equality relation is consistent with that of the arrays
 * returned by {@link #get()} (i. e. for non-null  instances <code>key1</code> and <code>key2</code>
 * the following condition holds: <code>Arrays.equals(key1.get(), key2.get()) == key1.equals(key2)</code>).
 */
public interface Key {

	/**
	 * Returns the binary representation of the key.
	 * @return the binary representation of the key
	 */
	byte[] get();
}
