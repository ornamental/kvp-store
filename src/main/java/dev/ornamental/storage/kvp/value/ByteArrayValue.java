package dev.ornamental.storage.kvp.value;

import java.nio.channels.ReadableByteChannel;

/**
 * This class is an adapter for <code>byte[]</code> type to {@link Value} interface.
 */
public final class ByteArrayValue implements Value {

	private final byte[] bytes;

	/**
	 * Wraps the passed byte array to implement the {@link Value} interface.
	 * @param bytes the byte array to wrap
	 */
	public ByteArrayValue(byte[] bytes) {
		this.bytes = bytes;
	}

	@Override
	public ReadableByteChannel getChannel() {
		return new ReadableByteArrayChannel(bytes);
	}

	@Override
	public byte[] getBytes() {
		return bytes;
	}

	@Override
	public int getLength() {
		return bytes.length;
	}
}
