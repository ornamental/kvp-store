package dev.ornamental.storage.gc;

import java.util.function.BooleanSupplier;

/**
 * This interface is implemented by listeners of garbage collection scheduler events.
 * Its only method is invoked by a scheduler when it is time to perform a garbage collection.
 */
public interface GcAgent {

	/**
	 * The method performs garbage collections on a storage. An exception must only be thrown if
	 * a garbage collection encounters an irrecoverable error and future attempts of GC must not
	 * be taken. Otherwise, in case of recoverable error, {@link GcResult#RECORDS_REMAIN} must
	 * be returned to allow a garbage collection retry.<br>
	 * If an exception is thrown, the agent will be deregistered automatically. It is up to
	 * the agent / storage to decide to what extent it remains operable without GC.
	 * @param interruption the supplier of interruption flag; this flag should be periodically
	 *                     checked to allow GC to be interrupted
	 * @return {@link GcResult#INTERRUPTED} if interruption flag was detected in the set state;
	 * {@link GcResult#RECORDS_REMAIN} if not all of the entries could be deleted
	 * (e. g. being held by previous versions' readers);
	 * {@link GcResult#DONE} if all the files which were seen as requiring deletion were deleted
	 * @throws Throwable if an unexpected error occurs
	 */
	GcResult gc(BooleanSupplier interruption) throws Throwable;
}
