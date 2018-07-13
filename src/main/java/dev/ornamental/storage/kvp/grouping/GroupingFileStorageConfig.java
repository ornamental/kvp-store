package dev.ornamental.storage.kvp.grouping;

import java.nio.file.Path;

import dev.ornamental.storage.gc.GcScheduler;

/**
 * Configuration class for {@link GroupingFileStorage} instances.
 */
public final class GroupingFileStorageConfig {

	private final Path directory;

	private final GcScheduler gcScheduler;

	/**
	 * See {@link SplittingWriterConfig#minLogFileSize}.
	 */
	private long minLogFileSize = 32_000_000;

	/**
	 * See {@link SplittingWriterConfig#maxLogFileSizeOvershoot}.
	 */
	private float maxLogFileSizeOvershoot = 0.5f;

	/**
	 * The minimum amount of data in a completed log file for it to not be subjected to compaction
	 */
	private long logFileUnderflow = 10_000_000;

	/**
	 * The maximum amount of unused space in a log file (after deletions) for it not to be
	 * subjected to compaction
	 */
	private float maxUnusedSpace = 0.25f;

	/**
	 * Creates a storage configuration with specified root directory and garbage collection scheduler.<br>
	 * The default minimum log file size is 32 000 000 bytes with overshoot ratio of 0.5.
	 * These parameters may be changed.
	 * @param directory the directory to place the storage files in; it will be searched for existing
	 *  storage files upon storage instance creation
	 * @param gcScheduler the garbage collection scheduler
	 */
	public GroupingFileStorageConfig(Path directory, GcScheduler gcScheduler) {
		this.directory = directory;
		this.gcScheduler = gcScheduler;
	}

	/**
	 * Return the root directory of the storage.
	 * @return the root directory of the storage
	 */
	public Path getDirectory() {
		return directory;
	}

	/**
	 * Returns the garbage collector scheduler.
	 * @return the garbage collector scheduler
	 */
	public GcScheduler getGcScheduler() {
		return gcScheduler;
	}

	/**
	 * Returns the minimum log file size.
	 * @return the minimum log file size, in bytes
	 */
	public long getMinLogFileSize() {
		return minLogFileSize;
	}

	/**
	 * Returns the maximum log file size overshoot ratio.
	 * @return the maximum log file size overshoot ratio
	 */
	public float getMaxLogFileSizeOvershoot() {
		return maxLogFileSizeOvershoot;
	}

	/**
	 * Returns the minimum amount of data in a completed log file for it to not be subjected to compaction.
	 * @return the completed log file compaction threshold, in bytes
	 */
	public long getLogFileUnderflow() {
		return logFileUnderflow;
	}

	/**
	 * Returns the maximum unused part of a log file for it to be compacted.
	 * @return the maximum part of a log file which may be left unused after deletions
	 */
	public float getMaxUnusedSpace() {
		return maxUnusedSpace;
	}

	/**
	 * Sets the minimum log file size. Each file produced by the writer will be at least this big
	 * unless it is an incomplete log file where the future records may be added.
	 * @param bytes the minimum log file size, in bytes
	 * @return this configuration
	 */
	public GroupingFileStorageConfig withMinLogFileSize(long bytes) {
		if (bytes <= 0) {
			throw new IllegalArgumentException("The log size must be positive.");
		}
		this.minLogFileSize = bytes;
		return this;
	}

	/**
	 * Sets the maximum size overshoot ratio for a log file. The minimum log file size
	 * is going to be exceeded by more than <code>(1 + k)</code>, <code>k</code> being this ratio,
	 * either a new log file will be created to avoid this, or the record will be put into a
	 * separate 'off-log' file, depending on the amount of data.
	 * @param factor the maximum log file size overshoot ratio
	 * @return this configuration
	 */
	public GroupingFileStorageConfig withMaxLogFileSizeOvershoot(float factor) {
		if (factor <= 0 || factor >= 1.0) {
			throw new IllegalArgumentException("The factor must be between 0.0 and 1.0.");
		}
		this.maxLogFileSizeOvershoot = factor;
		return this;
	}

	/**
	 * Sets the minimum amount of data in a completed log file for it to not be subjected to compaction.
	 * @param bytes the minimum size of a log file, in bytes
	 * @return this configuration
	 */
	public GroupingFileStorageConfig withLogFileUnderflow(long bytes) {
		if (bytes <= 0) {
			throw new IllegalArgumentException("The argument must be positive.");
		}
		this.logFileUnderflow = bytes;
		return this;
	}

	/**
	 * Sets the maximum amount of unused space in a log file (after deletions) for it not to be
	 * subjected to compaction.
	 * @param factor the maximum part of the file
	 * @return this configuration
	 */
	public GroupingFileStorageConfig withMaxUnusedSpace(float factor) {
		if (factor <= 0.0 || factor >= 1.0) {
			throw new IllegalArgumentException("The factor must be between 0.0 and 1.0.");
		}
		this.maxUnusedSpace = factor;
		return this;
	}
}
