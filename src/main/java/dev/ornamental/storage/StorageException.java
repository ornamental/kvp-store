package dev.ornamental.storage;

import java.io.IOException;

/**
 * The base class for the (checked) exceptions occurring during the storage operations.
 */
public class StorageException extends IOException {

	public StorageException() { }

	public StorageException(String message) {
		super(message);
	}

	public StorageException(String message, Throwable cause) {
		super(message, cause);
	}

	public StorageException(Throwable cause) {
		super(cause);
	}
}
