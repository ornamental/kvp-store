package dev.ornamental.storage.kvp.value;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;

/**
 * This class is an adapter for <code>byte[]</code> type to {@link ReadableByteChannel} interface.
 */
public final class ReadableByteArrayChannel implements ReadableByteChannel {

	private boolean isOpen = true;

	private final byte[] data;

	private int position = 0;

	/**
	 * Wraps the specified byte array to implement the {@link ReadableByteChannel} interface.
	 * @param data the byte array to wrap
	 */
	public ReadableByteArrayChannel(byte[] data) {
		this.data = data;
	}

	@Override
	public int read(ByteBuffer dst) throws IOException {
		checkOpen();
		if (position == data.length) {
			return -1;
		}

		int bytesToCopy = dst.remaining();
		if (bytesToCopy > 0) {
			bytesToCopy = Math.min(bytesToCopy, data.length - position);
			dst.put(data, position, bytesToCopy);
			position += bytesToCopy;
		}

		return bytesToCopy;
	}

	@Override
	public boolean isOpen() {
		return isOpen;
	}

	@Override
	public void close() {
		isOpen = false;
	}

	private void checkOpen() throws ClosedChannelException {
		if (!isOpen) {
			throw new ClosedChannelException();
		}
	}
}
