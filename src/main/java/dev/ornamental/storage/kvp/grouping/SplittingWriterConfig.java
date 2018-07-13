package dev.ornamental.storage.kvp.grouping;

import java.nio.file.Path;
import java.util.function.Supplier;

/**
 * Configuration class for {@link SplittingWriter} instances.
 */
public final class SplittingWriterConfig {

	private final Path root;

	private final Supplier<String> fileNameSupplier;

	/**
	 * The minimum log file size in bytes (<code>M</code>); standard log files will have size between
	 * <code>M</code> and <code>(1 + k) * M</code> (see {@link #maxLogFileSizeOvershoot}).
	 */
	private long minLogFileSize = 32_000_000;

	/**
	 * The file size range multiplier (<code>k</code>). Defines the maximum log file size compared to its minimum
	 * size (the multiplicand is <code>1 + k</code>, see {@link #minLogFileSize}). Also, records of sizes starting
	 * from <code>k * M</code> may be put in separate ('off-log') files.
	 */
	private float maxLogFileSizeOvershoot = 0.5f;

	/**
	 * Creates a writer configuration with specified target path and unique file name supplier.<br>
	 * The default minimum log file size is 32 000 000 bytes with overshoot ratio of 0.5.
	 * These parameters may be changed.
	 * @param root the root path for the log and off-log files
	 * @param fileNameSupplier the generator of file names; it must produce unique names
	 */
	public SplittingWriterConfig(Path root, Supplier<String> fileNameSupplier) {
		if (root == null || fileNameSupplier == null) {
			throw new IllegalArgumentException("Both arguments must be specified.");
		}

		this.root = root;
		this.fileNameSupplier = fileNameSupplier;
	}

	/**
	 * Returns the root directory path the writer must operate on.
	 * @return the root directory path
	 */
	public Path getRoot() {
		return root;
	}

	/**
	 * Returns the supplier of unique file names to be used by the writer.
	 * @return the supplier of unique file names
	 */
	public Supplier<String> getFileNameSupplier() {
		return fileNameSupplier;
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
	 * Sets the minimum log file size. Each file produced by the writer will be at least this big
	 * unless it is an incomplete log file where the future records may be added.
	 * @param bytes the minimum log file size, in bytes
	 * @return this configuration
	 */
	public SplittingWriterConfig withMinLogFileSize(long bytes) {
		if (bytes <= 0) {
			throw new IllegalArgumentException("The file size must be positive.");
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
	public SplittingWriterConfig withMaxLogFileSizeOvershoot(float factor) {
		if (factor <= 0 || factor >= 1.0) {
			throw new IllegalArgumentException("The factor must be between 0.0 and 1.0.");
		}
		this.maxLogFileSizeOvershoot = factor;
		return this;
	}
}
