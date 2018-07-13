package dev.ornamental.storage.kvp.value;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import dev.ornamental.storage.StorageException;

/**
 * Interface for the stored values. The size of each value has to be know in advance.<br>
 * The implementations of this interface do not have to be thread-safe.
 */
public interface Value {

	/**
	 * Returns a channel from which the value can be read. This method does not have to be reusable.
	 * @return the buffer the value can be read from
	 * @throws IllegalStateException any of the methods {@code getChannel()}, {@link #getStream()},
	 * or {@link #getBytes()} has already been used on this instance
	 */
	ReadableByteChannel getChannel() throws IllegalStateException, StorageException;

	/**
	 * Returns a stream from which the value can be read. This method does not have to be reusable.
	 * @return the stream the value can be read from
	 * @throws IllegalStateException any of the methods {@link #getChannel()}, {@code getStream()},
	 * or {@link #getBytes()} has already been used on this instance
	 */
	default InputStream getStream() throws IllegalStateException, StorageException {
		return Channels.newInputStream(getChannel());
	}

	/**
	 * Reads the value into a byte array.
	 * @return the binary representation of the value
	 * @throws IllegalStateException any of the methods {@link #getChannel()}, {@link #getStream()},
	 * or {@code getBytes()} has already been used on this instance
	 */
	default byte[] getBytes() throws IllegalStateException, StorageException {
		try (
			InputStream is = getStream();
			ByteArrayOutputStream os = new ByteArrayOutputStream()) {

			long bytesReadTotal = 0;
			byte[] buffer = new byte[8192];
			int bytesRead;
			while ((bytesRead = is.read(buffer)) > 0) {
				os.write(buffer, 0, bytesRead);
				bytesReadTotal += bytesRead;
			}

			return os.toByteArray();
		} catch (IOException e) {
			throw new StorageException(e);
		}
	}

	/**
	 * Returns the number of bytes in the value.
	 * @return the number of bytes in the value
	 */
	int getLength();
}
