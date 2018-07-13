package dev.ornamental.storage.kvp.grouping;

/**
 * This interface represents a listener for the events raised by {@link SplittingWriter}
 * instances.
 */
public interface FileEventListener {

	/**
	 * This method is invoked before the {@link SplittingWriter} creates a new file.
	 * @param fileName the relative path to the file which is going to be created
	 * @return an optional object to associate with the file; it will be available to
	 * retrieve from the {@link FileSegment} objects returned for writes into this file
	 */
	Object beforeFileCreated(String fileName);

	/**
	 * This method is invoked after a file is closed. That happens either when the
	 * last entry is written to a file or the {@link SplittingWriter} is closed.
	 * @param fileName the name of the closed file
	 * @param fileTag the optional tag specified for the file before its creation
	 * @param fileSize the size of the file when it is closed (may exceed the total
	 * size of entries written to the file due to pre-allocation)
	 */
	void afterFileClosed(String fileName, Object fileTag, int fileSize);
}
