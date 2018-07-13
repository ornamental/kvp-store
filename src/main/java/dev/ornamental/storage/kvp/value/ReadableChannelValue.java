package dev.ornamental.storage.kvp.value;

import java.nio.channels.ReadableByteChannel;

/**
 * This class is an adapter for {@link ReadableByteChannel} to {@link Value} interface.
 */
public final class ReadableChannelValue implements Value {

	private final ReadableByteChannel channel;

	private final int length;

	/**
	 * Wraps the specified {@link ReadableByteChannel} instance to implement the {@link Value} interface.
	 * @param channel the channel to wrap
	 * @param length the amount of data, in bytes, to be available through the {@link Value} interface
	 */
	public ReadableChannelValue(ReadableByteChannel channel, int length) {
		this.channel = channel;
		this.length = length;
	}

	@Override
	public ReadableByteChannel getChannel() {
		return channel;
	}

	@Override
	public int getLength() {
		return length;
	}
}
