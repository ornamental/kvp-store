package dev.ornamental.storage.kvp;

import java.util.Arrays;

/**
 * This class supplies default {@link #equals(Object)} and {@link #hashCode()} implementations
 * based on the array returned by {@link #get()}. If keys may be compared in a more effective manner,
 * this class should probably not be used.
 */
public abstract class AbstractKey implements Key {

	@Override
	public int hashCode() {
		return Arrays.hashCode(get());
	}

	@Override
	public boolean equals(Object obj) {
		return
			obj == this
			|| (obj instanceof Key && Arrays.equals(get(), ((Key)obj).get()));
	}
}
