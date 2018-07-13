package dev.ornamental.storage.kvp.grouping;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.StampedLock;

import dev.ornamental.storage.kvp.value.Value;

/**
 * This class writes the supplied chunks of data to a series of log-like files
 * so that a new file is started when the previous one reaches the [approximate]
 * size limit. When a record is too big for such condition to hold, it is placed
 * in a separate ('off-log') file. The class supports concurrent writes and requires
 * to determine the size of each record before writing.
 */
public final class SplittingWriter implements AutoCloseable {

	private static class FileChannelWrapper {

		private final Object sync = new Object();

		private volatile FileId fileId;

		private volatile FileChannel channel;

		public void setChannel(FileId fileId, FileChannel channel) {
			this.fileId = fileId;
			this.channel = channel;
			synchronized (sync) {
				sync.notifyAll();
			}
		}

		public FileId getFileId() {
			return fileId;
		}

		public FileChannel getChannel() throws InterruptedException {
			if (channel == null) {
				synchronized (sync) {
					while (channel == null) {
						sync.wait();
					}
				}
			}
			return channel;
		}

		public void force() throws IOException {
			if (channel != null) {
				channel.force(false);
			}
		}
	}

	private static class FileId {

		private final String file;

		private final Object fileTag;

		public FileId(String file, Object fileTag) {
			this.file = file;
			this.fileTag = fileTag;
		}

		public String getFile() {
			return file;
		}

		public Object getFileTag() {
			return fileTag;
		}
	}

	private class WriteInterval {

		private final boolean isInitial; // is responsible for channel creation

		private final int startOffset;

		private final int endOffset; // exclusive

		private final FileChannelWrapper fileChannelWrapper;

		private final AtomicInteger writers;

		private final AtomicBoolean fileFinalized;

		private final AtomicBoolean channelClosed;

		public WriteInterval(int size) {
			this.isInitial = true;
			this.startOffset = 0;
			this.endOffset = size;
			this.fileChannelWrapper = new FileChannelWrapper();
			this.writers = new AtomicInteger(0);
			this.fileFinalized = new AtomicBoolean(false);
			this.channelClosed = new AtomicBoolean(false);
		}

		public WriteInterval(String file, Object fileTag, FileChannel channel, int offset) {
			this.isInitial = false;
			this.startOffset = offset;
			this.endOffset = offset;
			this.fileChannelWrapper = new FileChannelWrapper();
			this.writers = new AtomicInteger(0);
			this.fileFinalized = new AtomicBoolean(false); // TODO [WHAT IF YES?]
			this.channelClosed = new AtomicBoolean(false);

			this.fileChannelWrapper.setChannel(new FileId(file, fileTag), channel);
		}

		private WriteInterval(WriteInterval previous, int addend) {
			this.isInitial = false;
			this.startOffset = previous.endOffset;
			this.endOffset = this.startOffset + addend;
			this.fileChannelWrapper = previous.fileChannelWrapper;
			this.writers = previous.writers;
			this.fileFinalized = previous.fileFinalized;
			this.channelClosed = previous.channelClosed;
		}

		public int getStartOffset() {
			return startOffset;
		}

		public int getEndOffset() {
			return endOffset;
		}

		public FileChannelWrapper getFileChannelWrapper() {
			return fileChannelWrapper;
		}

		public void acquire() {
			this.writers.incrementAndGet();
		}

		public void release() throws IOException {
			if (writers.decrementAndGet() == 0
				&& fileFinalized.get()
				&& channelClosed.compareAndSet(false, true)) {

				wrappedCloseChannel();
			}
		}

		private void wrappedCloseChannel() throws IOException {
			try {
				closeChannel();
			} catch (InterruptedException e) {
				// this method is expected to be called after writing to the channel occurs,
				// so this exception is not normally expected
				Thread.currentThread().interrupt();
				throw new RuntimeException(e);
			}
		}

		public void finalizeFile() throws IOException {
			if (fileFinalized.compareAndSet(false, true)
				&& writers.get() == 0
				&& channelClosed.compareAndSet(false, true)) {

				wrappedCloseChannel();
			}
			fileFinalized.set(true);
		}

		public void closeChannel() throws InterruptedException, IOException {
			FileChannel channel = fileChannelWrapper.getChannel();
			channel.force(false);
			int size = (int)channel.size();
			channel.close();
			notifyListener(
				fileChannelWrapper.getFileId().getFile(),
				fileChannelWrapper.getFileId().getFileTag(),
				size);
		}

		public boolean isInitial() {
			return isInitial;
		}
	}

	private class WritePositionCounter {

		private final AtomicReference<WriteInterval> position;

		public WritePositionCounter(WriteInterval initial) {
			this.position = new AtomicReference<>(initial);
		}

		public Optional<WriteInterval> allocate(int totalData) throws IOException {
			WriteInterval old;
			WriteInterval updated;
			do {
				old = position.get();
				if (old == null || old.getEndOffset() >= config.getMinLogFileSize()) { // start new file
					if (totalData >= config.getMinLogFileSize()) { // off the log
						updated = null;
						break;
					} else { // on the log, next file
						updated = new WriteInterval(totalData);
					}
				} else if (old.getEndOffset() + totalData
					>= (1.0 + config.getMaxLogFileSizeOvershoot()) * config.getMinLogFileSize()) { // off log

					updated = null;
					break;
				} else { // on the log, same file as previous
					updated = new WriteInterval(old, totalData);
				}
			} while (!position.compareAndSet(old, updated));

			if (updated != null) {
				updated.acquire();
			}
			if (old != null && updated != null
				&& !updated.getFileChannelWrapper().equals(old.getFileChannelWrapper())) {

				old.finalizeFile();
			}

			return Optional.ofNullable(updated);
		}

		public WriteInterval getPosition() {
			return position.get();
		}
	}

	private final SplittingWriterConfig config;

	private final AtomicBoolean closed = new AtomicBoolean(false);

	private final WritePositionCounter counter;

	private final StampedLock writeForceLock = new StampedLock();

	private volatile FileEventListener eventListener;

	/**
	 * Creates a new writer given the writer configuration.
	 * @param config the writer configuration
	 */
	public SplittingWriter(SplittingWriterConfig config) {
		this.config = config;
		this.counter = new WritePositionCounter(null);
	}

	/**
	 * Creates a writer restoring the state of a previously closed writer targeted at the same directory.
	 * @param config the writer configuration; it must match the original writer configuration
	 * @param lastFile the last file written to by the previous writer
	 * @param lastFileOffset the offset in the last file written to continue writing from
	 * @param lastFileTag the tag assigned to the last file
	 */
	public SplittingWriter(
		SplittingWriterConfig config, String lastFile, int lastFileOffset, Object lastFileTag)
		throws IOException {

		this.config = config;

		FileChannel channel = FileChannel.open(getPath(lastFile), StandardOpenOption.WRITE);
		try {
			channel.truncate(lastFileOffset);
		} catch (IOException e) {
			channel.close();
			throw e;
		}

		WriteInterval initial = new WriteInterval(lastFile, lastFileTag, channel, lastFileOffset);
		this.counter = new WritePositionCounter(initial);
	}

	/**
	 * Writes a piece of data to the store. The write is not forced to be persisted on disk.
	 * @param data the data to be written to the store
	 * @return the file segment from which the data may be read back
	 */
	public FileSegment write(Value data) throws InterruptedException, IOException {
		long lock = writeForceLock.readLock();
		try {
			if (closed.get()) {
				throw new IOException("The store has already been closed.");
			}

			Optional<WriteInterval> position = counter.allocate(data.getLength());
			if (!position.isPresent()) {
				return writeFreeObject(data);
			} else {
				WriteInterval writeInterval = position.get();
				try {
					FileChannel channel = createOrAwaitFile(writeInterval);
					writeAtOffset(channel, writeInterval, data);

					return new FileSegment(
						writeInterval.getFileChannelWrapper().getFileId().getFile(),
						writeInterval.getFileChannelWrapper().getFileId().getFileTag(),
						writeInterval.getStartOffset(), data.getLength());
				} finally {
					writeInterval.release();
				}
			}
		} finally {
			writeForceLock.unlockRead(lock);
		}
	}

	/**
	 * Forces the changes made to the store be reliably flushed to disk.
	 */
	public void sync() throws IOException {
		long lock = writeForceLock.writeLock();
		try {
			WriteInterval position = counter.getPosition();
			if (position != null) {
				position.getFileChannelWrapper().force();
			}
		} finally {
			writeForceLock.unlockWrite(lock);
		}
	}

	/**
	 * Sets or drops the listener for the file creation events of this writer.
	 * @param eventListener the event listener to set ({@code null} to drop one)
	 */
	public void setEventListener(FileEventListener eventListener) {
		this.eventListener = eventListener;
	}

	@Override
	public void close() throws IOException, InterruptedException {
		if (closed.compareAndSet(false, true)) {
			long lock = writeForceLock.writeLock();
			try {
				WriteInterval position = counter.getPosition();
				if (position != null) {
					position.closeChannel();
				}
			} finally {
				writeForceLock.unlockWrite(lock);
			}
		}
	}

	private void writeAtOffset(
		FileChannel channel, WriteInterval writeInterval, Value entry) throws IOException {

		try (ReadableByteChannel entryChannel = entry.getChannel()) {
			channel.transferFrom(entryChannel, writeInterval.getStartOffset(), entry.getLength());
		}
	}

	private FileSegment writeFreeObject(Value entry) throws IOException {
		String fileName = config.getFileNameSupplier().get();
		Object fileTag = notifyListener(fileName);
		Path path = getPath(fileName);
		ensureParentExists(path);
		try (FileChannel channel = FileChannel.open(path,
				StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
			ReadableByteChannel entryChannel = entry.getChannel()) {

			channel.transferFrom(entryChannel, 0, entry.getLength());
			channel.force(false);
		}
		notifyListener(fileName, fileTag, entry.getLength());
		return new FileSegment(fileName, fileTag, 0, entry.getLength());
	}

	private void ensureParentExists(Path path) throws IOException {
		Path parent = path.getParent();
		if (parent != null) {
			Files.createDirectories(parent);
		}
	}

	private FileChannel createOrAwaitFile(WriteInterval writeInterval) throws InterruptedException, IOException {
		FileChannel channel;
		if (writeInterval.isInitial()) {
			channel = createFile(writeInterval);
		} else {
			channel = writeInterval.getFileChannelWrapper().getChannel();
		}

		return channel;
	}

	private FileChannel createFile(WriteInterval writeInterval) throws IOException {
		String fileName = config.getFileNameSupplier().get();
		Object fileTag = notifyListener(fileName);
		FileId fileId = new FileId(fileName, fileTag);
		Path file = getPath(fileName);
		ensureParentExists(file);
		FileChannel channel = null;
		try {
			channel = FileChannel.open(file,
				StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
			//channel.position(MIN_LOG_FILE_SIZE - 1);
			//channel.write(ByteBuffer.wrap(new byte[1]));
			channel.force(false);
			writeInterval.getFileChannelWrapper().setChannel(fileId, channel);
			return channel;
		} catch (Throwable t) {
			if (channel != null) {
				channel.close();
			}
			throw t;
		}
	}

	private Path getPath(String fileName) {
		return config.getRoot().resolve(fileName);
	}

	private Object notifyListener(String fileName) {
		FileEventListener eventListener = this.eventListener;
		return eventListener == null ? null : eventListener.beforeFileCreated(fileName);
	}

	private void notifyListener(String fileName, Object fileTag, int size) {
		FileEventListener eventListener = this.eventListener;
		if (eventListener != null) {
			eventListener.afterFileClosed(fileName, fileTag, size);
		}
	}
}
