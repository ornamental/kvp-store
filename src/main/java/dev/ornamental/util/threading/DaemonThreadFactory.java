package dev.ornamental.util.threading;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * This thread factory produces daemon threads instead of ones preventing the application termination.
 */
public class DaemonThreadFactory implements ThreadFactory {

	@Override
	public Thread newThread(Runnable r) {
		Thread t = Executors.defaultThreadFactory().newThread(r);
		t.setDaemon(true);
		return t;
	}
}
