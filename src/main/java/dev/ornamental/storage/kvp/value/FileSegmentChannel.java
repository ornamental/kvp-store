package dev.ornamental.storage.kvp.value;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;

/**
 * This class represents a byte channel for a segment of a file.
 */
public final class FileSegmentChannel implements ReadableByteChannel {

	private boolean isOpen = true;

	private final FileChannel channel;

	private final int offset;

	private final int size;

	/**
	 * Creates a channel instance given the underlying file channel and segment offset and size.
	 * @param channel the channel to wrap
	 * @param offset the file offset
	 * @param size the size of the file segment
	 */
	public FileSegmentChannel(FileChannel channel, int offset, int size) {
		this.channel = channel;
		this.offset = offset;
		this.size = size;
	}

	@Override
	public int read(ByteBuffer dst) throws IOException {
		checkOpen();

		int totalRead = 0;
		while (totalRead < size) {
			int read = channel.read(dst, offset + totalRead);
			if (read == -1) {
				break;
			}
			totalRead += read;
		}

		return totalRead;
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
