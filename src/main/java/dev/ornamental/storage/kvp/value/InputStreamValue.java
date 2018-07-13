package dev.ornamental.storage.kvp.value;

import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

/**
 * This class is an adapter for {@link InputStream} to {@link Value} interface.
 */
public final class InputStreamValue implements Value {

	private final InputStream stream;

	private final int length;

	private boolean used = false;

	/**
	 * Wraps the specified {@link InputStream} to implement the {@link Value} interface.
	 * @param stream the stream to wrap
	 * @param length the amount of data in the stream representing the desired value
	 */
	public InputStreamValue(InputStream stream, int length) {
		this.stream = stream;
		this.length = length;
	}

	@Override
	public ReadableByteChannel getChannel() {
		return Channels.newChannel(getStream());
	}

	@Override
	public int getLength() {
		return length;
	}

	@Override
	public InputStream getStream() {
		if (used) {
			throw new IllegalStateException(
				"The output has already been returned, the method cannot be used twice.");
		}
		used = true;
		return stream;
	}
}
