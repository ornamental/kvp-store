package dev.ornamental.storage.kvp.grouping;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import dev.ornamental.storage.StorageException;
import dev.ornamental.storage.kvp.Key;
import dev.ornamental.storage.kvp.Reader;
import dev.ornamental.storage.kvp.VersionedStorage;
import dev.ornamental.storage.kvp.Writer;
import dev.ornamental.storage.gc.GcScheduler;
import dev.ornamental.storage.kvp.value.ByteArrayValue;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class VersionedRepositoryTest {

	private final class StringKey implements Key {

		private final byte[] k;

		public StringKey(String s) {
			this.k = s.getBytes(Charset.forName("UTF-8"));
		}

		@Override
		public byte[] get() {
			return k;
		}
	}

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	private Path storagePath;

	private VersionedStorage repository;

	private GcScheduler gc;

	@Before
	public void setup() throws SQLException, IOException {
		gc = new GcScheduler();
		storagePath = folder.newFolder("storage").toPath();
		repository = new GroupingFileStorage(new GroupingFileStorageConfig(storagePath, gc));
	}

	@After
	public void tearDown() throws Exception {
		repository.close();
		repository = null;
		gc.close();
		deleteRecursively(storagePath);
	}

	@Test
	public void testReadsAndWritesSingleThreaded() throws Exception {

		Key key1 = new StringKey("key1");
		Key key2 = new StringKey("key2");

		byte[] array;

		try (Reader reader = repository.getReader()) {

			assertNotPresent(reader, key1);

			try (Writer write = repository.getWriter()) {
				Assert.assertFalse(write.remove(key1));

				array = new byte[64_000];
				write.put(key1, new ByteArrayValue(array));

				assertPresent(write, key1, array);
				assertNotPresent(reader, key1);

				Assert.assertTrue(write.remove(key1));
				assertNotPresent(write, key1);

				array = new byte[100];
				write.put(key2, new ByteArrayValue(array));
				assertPresent(write, key2, array);
				assertNotPresent(reader, key2);

				write.commit();
			}

			assertNotPresent(reader, key1);
			assertNotPresent(reader, key2);
		}

		try (Reader reader = repository.getReader()) {
			assertNotPresent(reader, key1);
			assertPresent(reader, key2, array);

			try (Writer write = repository.getWriter()) {
				Assert.assertFalse(write.remove(key1));
				Assert.assertTrue(write.remove(key2));
				assertNotPresent(write, key2);
			} // implicit rollback

			assertPresent(reader, key2, array);

			try (Writer write = repository.getWriter()) {
				Assert.assertTrue(write.remove(key2));
				assertNotPresent(write, key2);

				write.commit();
			}

			assertPresent(reader, key2, array);
		}

		try (Reader reader = repository.getReader()) {
			assertNotPresent(reader, key2);
		}
	}

	@Test
	public void testMultipleReadersSingleThreaded() throws Exception {

		Key key1 = new StringKey("key1");
		Key key2 = new StringKey("key2");

		int steps = 7;
		List<Reader> readers = new ArrayList<>(steps + 1);
		readers.add(repository.getReader());

		for (int i = 0; i < steps; i++) {
			try (Writer write = repository.getWriter()) {
				// scenario for key1
				if (i % 2 == 0) {
					write.put(key1, new ByteArrayValue(new byte[(i + 1) * 1_000]));
				} else {
					write.remove(key1);
				}
				// scenario for key 2
				write.put(key2, new ByteArrayValue(new byte[1_000 + i]));

				write.commit();
			}

			// create a reader for the version
			readers.add(repository.getReader());
		}

		for (int version = 0; version <= steps; version++) {
			Reader read = readers.get(version);
			// scenario for key1
			Assert.assertTrue(read.exists(key1) == (version % 2 == 1));
			if (version % 2 == 1) {
				Assert.assertEquals(
					version * 1_000,
					read.get(key1).orElseThrow(AssertionError::new).getBytes().length);
			} else {
				Assert.assertFalse(read.get(key1).isPresent());
			}

			// scenario for key 2
			Assert.assertTrue(read.exists(key2) == (version > 0));
			if (version > 0) {
				Assert.assertEquals(
					1_000 + version - 1,
					read.get(key2).orElseThrow(AssertionError::new).getBytes().length);
			} else {
				Assert.assertFalse(read.get(key2).isPresent());
			}
		}

		for (Reader reader : readers) {
			reader.close();
		}
	}

	@Test(timeout = 20_000L)
	public void testConcurrentReadsAndWrites() throws Throwable {
		Key key = new StringKey("key");

		long testDuration = 7_000L; // milliseconds
		CountDownLatch latch = new CountDownLatch(2);

		Throwable[] readerError = new Throwable[1];
		Thread readersThread = new Thread(() -> {
			latch.countDown();
			try {
				latch.await();
			} catch (InterruptedException e) {
				readerError[0] = e;
				return;
			}

			// scenario: alternating puts and removes on the same key
			long nanos = System.nanoTime();
			while ((System.nanoTime() - nanos) / 1_000_000 < testDuration) {
				try (Reader reader = repository.getReader()) {
					long actualVersion = reader.getVersion() / 2; // each transaction increments version by 2
					Assert.assertTrue(reader.exists(key) == (actualVersion % 2 == 1));
				} catch (Throwable e) {
					readerError[0] = e;
					return;
				}
			}
		});
		readersThread.setDaemon(true);

		Throwable[] writerError = new Throwable[1];
		Thread writerThread = new Thread(() -> {
			latch.countDown();
			try {
				latch.await();
			} catch (InterruptedException e) {
				writerError[0] = e;
				return;
			}

			try (Writer write = repository.getWriter()) {
				long nanos = System.nanoTime();
				int i = 0;
				while ((System.nanoTime() - nanos) / 1_000_000 < testDuration) {
					if (i % 2 == 0) {
						write.put(key, new ByteArrayValue(new byte[1]));
					} else {
						write.remove(key);
					}
					write.commit();
					i++;
				}
			} catch (Throwable e) {
				writerError[0] = e;
			}
		});
		writerThread.setDaemon(true);

		readersThread.start();
		writerThread.start();

		readersThread.join();
		writerThread.join();

		if (readerError[0] != null) {
			throw readerError[0];
		}
		if (writerError[0] != null) {
			throw writerError[0];
		}
	}

	private void assertPresent(Reader reader, Key key, byte[] array)
		throws IOException {

		Assert.assertTrue(reader.exists(key));
		Assert.assertArrayEquals(
			array, reader.get(key).orElseThrow(AssertionError::new).getBytes());
	}

	private void assertNotPresent(Reader reader, Key key)
		throws StorageException {

		Assert.assertFalse(reader.exists(key));
		Assert.assertFalse(reader.get(key).isPresent());
	}

	private void deleteRecursively(Path path) throws IOException {
		Files.walk(path)
			.sorted(Comparator.reverseOrder())
			.forEach(p -> {
				try {
					Files.delete(p);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			});
	}
}