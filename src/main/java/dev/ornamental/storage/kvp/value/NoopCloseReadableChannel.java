package dev.ornamental.storage.kvp.value;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * This is a decorator for {@link ReadableByteChannel} instances which
 * intercepts calls to {@link ReadableByteChannel#close()} and turns them into
 * no-ops.
 */
public class NoopCloseReadableChannel implements ReadableByteChannel {

	private final ReadableByteChannel channel;

	/**
	 * Wraps the {@link ReadableByteChannel} instance to prevent the closing of.
	 * @param channel the channel to wrap
	 */
	public NoopCloseReadableChannel(ReadableByteChannel channel) {
		this.channel = channel;
	}

	@Override
	public int read(ByteBuffer dst) throws IOException {
		return channel.read(dst);
	}

	@Override
	public boolean isOpen() {
		return channel.isOpen();
	}

	@Override
	public void close() { }
}
