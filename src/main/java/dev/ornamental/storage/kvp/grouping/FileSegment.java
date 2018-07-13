package dev.ornamental.storage.kvp.grouping;

/**
 * This class represents a part of a file.
 */
public final class FileSegment {

	private final String file;

	private final int offset;

	private final int size;

	private final Object fileTag;

	/**
	 * Creates a new object representing a file segment.
	 * @param file the file name
	 * @param fileTag the tag allowing to identify the file
	 * @param offset the segment offset from the start of the file, in bytes
	 * @param size the segment size, in bytes
	 */
	public FileSegment(String file, Object fileTag, int offset, int size) {
		this.file = file;
		this.offset = offset;
		this.size = size;
		this.fileTag = fileTag;
	}

	/**
	 * Returns the file name.
	 * @return the file name
	 */
	public String getFile() {
		return file;
	}

	/**
	 * Returns the file segment offset from the start of the file.
	 * @return the file segment offset in bytes
	 */
	public int getOffset() {
		return offset;
	}

	/**
	 * Returns the size of the file segment.
	 * @return the file segment size, in bytes
	 */
	public int getSize() {
		return size;
	}

	/**
	 * Returns the tag the file may be identified by.
	 * @return the file tag
	 */
	public Object getFileTag() {
		return fileTag;
	}
}
