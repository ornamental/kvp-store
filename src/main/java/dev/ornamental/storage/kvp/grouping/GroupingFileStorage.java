package dev.ornamental.storage.kvp.grouping;

import static dev.ornamental.sqlite.statement.Functions.coalesce;
import static dev.ornamental.sqlite.statement.Functions.countAll;
import static dev.ornamental.sqlite.statement.Functions.max;
import static dev.ornamental.sqlite.statement.Functions.min;
import static dev.ornamental.sqlite.statement.Functions.sum;
import static dev.ornamental.sqlite.statement.Literal.NULL;
import static dev.ornamental.sqlite.statement.Literal.value;
import static dev.ornamental.sqlite.statement.SelectStatements.numericValues;
import static dev.ornamental.sqlite.statement.SelectStatements.select;
import static dev.ornamental.sqlite.statement.SelectStatements.stringValues;
import static dev.ornamental.sqlite.statement.SortingOrder.DESC;
import static dev.ornamental.sqlite.statement.SqlExpressions.column;
import static dev.ornamental.sqlite.statement.SqlExpressions.rowId;
import static dev.ornamental.sqlite.statement.SqlExpressions.rowIdOf;
import static dev.ornamental.sqlite.statement.SqlExpressions.rowOf;
import static dev.ornamental.sqlite.statement.SqlStatements.createIndex;
import static dev.ornamental.sqlite.statement.SqlStatements.createTable;
import static dev.ornamental.sqlite.statement.SqlStatements.deleteFrom;
import static dev.ornamental.sqlite.statement.SqlStatements.insertInto;
import static dev.ornamental.sqlite.statement.SqlStatements.insertOrIgnoreInto;
import static dev.ornamental.sqlite.statement.SqlStatements.insertOrReplaceInto;
import static dev.ornamental.sqlite.statement.SqlStatements.update;
import static dev.ornamental.sqlite.statement.SqlStatements.with;
import static dev.ornamental.sqlite.statement.TableExpressions.table;
import static dev.ornamental.sqlite.statement.Types.BIT;
import static dev.ornamental.sqlite.statement.Types.INTEGER;
import static dev.ornamental.sqlite.statement.Types.VARBINARY;
import static dev.ornamental.sqlite.statement.Types.VARCHAR;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.j256.ormlite.field.FieldType;
import com.j256.ormlite.support.ConnectionSource;
import com.j256.ormlite.support.DatabaseConnection;
import dev.ornamental.storage.StorageException;
import dev.ornamental.storage.kvp.AbstractVersionedStorage;
import dev.ornamental.storage.kvp.Key;
import dev.ornamental.storage.kvp.Reader;
import dev.ornamental.storage.kvp.ThreadSafeWriter;
import dev.ornamental.storage.kvp.Writer;
import dev.ornamental.storage.gc.GcAgent;
import dev.ornamental.storage.gc.GcResult;
import dev.ornamental.storage.kvp.value.ByteArrayValue;
import dev.ornamental.storage.kvp.value.FileSegmentChannel;
import dev.ornamental.storage.kvp.value.ReadableChannelValue;
import dev.ornamental.storage.kvp.value.Value;
import dev.ornamental.util.sql.DirectDbOperations;
import dev.ornamental.util.sql.PooledSqliteConnectionSource;
import dev.ornamental.sqlite.statement.SqlExpression;
import dev.ornamental.sqlite.statement.Table;

/**
 * This class is an MVCC storage implementation writing the records in files
 * sequentially and maintaining an indexing database.
 */
public final class GroupingFileStorage extends AbstractVersionedStorage {

	// region Classes used to store the data fetched from the database
	private static class OffsetSizePair {

		private final long rowId;

		private final int offset;

		private final int size;

		public OffsetSizePair(long rowId, int offset, int size) {
			this.rowId = rowId;
			this.offset = offset;
			this.size = size;
		}

		public long getRowId() {
			return rowId;
		}

		public int getOffset() {
			return offset;
		}

		public int getSize() {
			return size;
		}
	}

	private static class Meta {

		private final long internalVersion;

		private final long committedVersion;

		private final boolean hasUncommittedChanges;

		public Meta(long internalVersion, long committedVersion, boolean hasUncommittedChanges) {
			this.internalVersion = internalVersion;
			this.committedVersion = committedVersion;
			this.hasUncommittedChanges = hasUncommittedChanges;
		}

		public long getInternalVersion() {
			return internalVersion;
		}

		public long getCommittedVersion() {
			return committedVersion;
		}

		public boolean hasUncommittedChanges() {
			return hasUncommittedChanges;
		}
	}

	private static class UsedSpace {

		private final long fileId;

		private final int usedSpace;

		public UsedSpace(long fileId, int usedSpace) {
			this.fileId = fileId;
			this.usedSpace = usedSpace;
		}

		public long getFileId() {
			return fileId;
		}

		public int getUsedSpace() {
			return usedSpace;
		}
	}

	private static class GcSavepoint {

		private final Long fileId;

		private final String file;

		private final int offset;

		private final long rowId;

		public GcSavepoint(Long fileId, String file, int offset, long rowId) {
			this.fileId = fileId;
			this.file = file;
			this.offset = offset;
			this.rowId = rowId;
		}

		public Long getFileId() {
			return fileId;
		}

		public String getFile() {
			return file;
		}

		public int getOffset() {
			return offset;
		}

		public long getRowId() {
			return rowId;
		}
	}

	private static class DeletedEntry {

		private final byte[] id;

		private final long insertionVersion;

		private final long deletionVersion;

		private final long fileId;

		private final int size;

		public DeletedEntry(
			byte[] id, long insertionVersion, long deletionVersion, long fileId, int size) {

			this.id = id;
			this.insertionVersion = insertionVersion;
			this.deletionVersion = deletionVersion;
			this.fileId = fileId;
			this.size = size;
		}

		public byte[] getId() {
			return id;
		}

		public long getInsertionVersion() {
			return insertionVersion;
		}

		public long getDeletionVersion() {
			return deletionVersion;
		}

		public long getFileId() {
			return fileId;
		}

		public int getSize() {
			return size;
		}
	}

	private static class FileInfo {

		private final long id;

		private final int fullSize;

		private final int unusedBytes;

		public FileInfo(long id, int fullSize, int unusedBytes) {
			this.id = id;
			this.fullSize = fullSize;
			this.unusedBytes = unusedBytes;
		}

		public long getId() {
			return id;
		}

		public int getFullSize() {
			return fullSize;
		}

		public int getUnusedBytes() {
			return unusedBytes;
		}
	}

	private static class GcFileQueueElement {

		private final long rowId;

		private final long fileId;

		private final String fileName;

		public GcFileQueueElement(long rowId, long fileId, String fileName) {
			this.rowId = rowId;
			this.fileId = fileId;
			this.fileName = fileName;
		}

		public long getRowId() {
			return rowId;
		}

		public long getFileId() {
			return fileId;
		}

		public String getFileName() {
			return fileName;
		}
	}

	private static class FileToDelete {

		private final long rowId;

		private final String name;

		public FileToDelete(long rowId, String name) {
			this.rowId = rowId;
			this.name = name;
		}

		public long getRowId() {
			return rowId;
		}

		public String getName() {
			return name;
		}
	}

	private static class FileVersion {

		private final long id;

		private final String name;

		private final long sinceVersion;

		private final boolean isGcFile;

		public FileVersion(long id, String name, long sinceVersion, boolean isGcFile) {
			this.id = id;
			this.name = name;
			this.sinceVersion = sinceVersion;
			this.isGcFile = isGcFile;
		}

		public long getId() {
			return id;
		}

		public String getName() {
			return name;
		}

		public long getSinceVersion() {
			return sinceVersion;
		}

		public boolean isGcFile() {
			return isGcFile;
		}
	}
	// endregion

	// region Table helper classes
	private final class EntryTable extends Table {

		public final String id = "id";

		public final String version = "version";

		public final String deletedRowId = "deletedRowId";

		public final String file = "file";

		public final String offset = "offset";

		public final String size = "size";

		private final Map<String, SqlExpression> columns =
			Stream.of(id, version, deletedRowId, file, offset, size)
				.collect(Collectors.toMap(Function.identity(), columnFactory));

		public EntryTable() {
			super(entryTable);
		}

		public SqlExpression id() {
			return columns.get(id);
		}

		public SqlExpression version() {
			return columns.get(version);
		}

		public SqlExpression deletedRowId() {
			return columns.get(deletedRowId);
		}

		public SqlExpression file() {
			return columns.get(file);
		}

		public SqlExpression offset() {
			return columns.get(offset);
		}

		public SqlExpression size() {
			return columns.get(size);
		}
	}

	private final class FileTable extends Table {

		public final String id = "id";

		public final String name = "name";

		public final String sinceVersion = "sinceVersion";

		public final String fullSize = "fullSize";

		public final String unusedBytes = "unusedBytes";

		private final Map<String, SqlExpression> columns =
			Stream.of(id, name, sinceVersion, fullSize, unusedBytes)
				.collect(Collectors.toMap(Function.identity(), columnFactory));

		public FileTable() {
			super(fileTable);
		}

		public SqlExpression id() {
			return columns.get(id);
		}

		public SqlExpression name() {
			return columns.get(name);
		}

		public SqlExpression sinceVersion() {
			return columns.get(sinceVersion);
		}

		public SqlExpression fullSize() {
			return columns.get(fullSize);
		}

		public SqlExpression unusedBytes() {
			return columns.get(unusedBytes);
		}
	}

	private final class MetaTable extends Table {

		public final String internalVersion = "internalVersion";

		public final String committedVersion = "committedVersion";

		public final String hasUncommittedChanges = "hasUncommittedChanges";

		private final Map<String, SqlExpression> columns =
			Stream.of(internalVersion, committedVersion, hasUncommittedChanges)
				.collect(Collectors.toMap(Function.identity(), columnFactory));

		public MetaTable() {
			super(metaTable);
		}

		public SqlExpression internalVersion() {
			return columns.get(internalVersion);
		}

		public SqlExpression committedVersion() {
			return columns.get(committedVersion);
		}

		public SqlExpression hasUncommittedChanges() {
			return columns.get(hasUncommittedChanges);
		}
	}

	private final class GcSavepointTable extends Table {

		public final String file = "file";

		public final String offset = "offset";

		public final String lastRowId = "lastRowId";

		private final Map<String, SqlExpression> columns =
			Stream.of(file, offset, lastRowId)
				.collect(Collectors.toMap(Function.identity(), columnFactory));

		public GcSavepointTable() {
			super(gcSavepointTable);
		}

		public SqlExpression file() {
			return columns.get(file);
		}

		public SqlExpression offset() {
			return columns.get(offset);
		}

		public SqlExpression lastRowId() {
			return columns.get(lastRowId);
		}
	}

	private final class GcFileQueueTable extends Table {

		public final String file = "file";

		private final Map<String, SqlExpression> columns =
			Stream.of(file)
				.collect(Collectors.toMap(Function.identity(), columnFactory));

		public GcFileQueueTable() {
			super(gcFileQueueTable);
		}

		public SqlExpression file() {
			return columns.get(file);
		}
	}

	private final class GcUncommittedFilesTable extends Table {

		public final String file = "file";

		private final Map<String, SqlExpression> columns =
			Stream.of(file)
				.collect(Collectors.toMap(Function.identity(), columnFactory));

		public GcUncommittedFilesTable() {
			super(gcUncommittedFilesTable);
		}

		public SqlExpression file() {
			return columns.get(file);
		}
	}

	private final class FileDeletionQueueTable extends Table {

		public final String name = "name";

		private final Map<String, SqlExpression> columns =
			Stream.of(name)
				.collect(Collectors.toMap(Function.identity(), columnFactory));

		public FileDeletionQueueTable() {
			super(fileDeletionQueueTable);
		}

		public SqlExpression name() {
			return columns.get(name);
		}
	}
	// endregion

	// region Listeners used with the splitting writer
	private abstract class AbstractFileEventListener implements FileEventListener {

		public Object beforeFileCreated(String fileName) {
			long id = fileIdGenerator.incrementAndGet();
			try {
				dbOperations.useConnection(true, c -> {
					registerNewFile(c, fileName, id);
					return null;
				});
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}

			return id;
		}

		@Override
		public void afterFileClosed(String fileName, Object fileTag, int fileSize) {
			try {
				dbOperations.useConnection(false, c -> {
					registerFileClosed(c, fileName, fileTag, fileSize);
					return null;
				});
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}

		protected void registerNewFile(
			DatabaseConnection c, String fileName, long id) throws SQLException {

			String statement =
				insertInto(fileTable).columns(file.id, file.name, file.fullSize, file.unusedBytes, file.sinceVersion)
					.values(rowOf(value(id), value(fileName), NULL, value(0), value(getSinceVersion())))
					.build();
			c.insert(statement, EMPTY_ARGS, EMPTY_TYPES, null);
		}

		protected void registerFileClosed(
			DatabaseConnection c, String fileName, Object fileTag, int fileSize) throws SQLException {

			String statement =
				update(fileTable).set(file.fullSize, value(fileSize)).where(file.id().eq(value((long)fileTag)))
					.build();
			c.update(statement, EMPTY_ARGS, EMPTY_TYPES);
		}

		protected abstract Long getSinceVersion();
	}

	private class GcFileEventListener extends AbstractFileEventListener {

		@Override
		protected void registerNewFile(DatabaseConnection c, String fileName, long id) throws SQLException {
			super.registerNewFile(c, fileName, id);
			String statement =
				insertInto(gcUncommittedFilesTable).columns(gcUncommittedFiles.file).values(rowOf(id))
					.build();
			c.insert(statement, EMPTY_ARGS, EMPTY_TYPES, null);
		}

		@Override
		protected Long getSinceVersion() {
			return null;
		}
	}

	private class WriterFileEventListener extends AbstractFileEventListener {

		private final long version;

		private volatile boolean underflowRegistered = false;

		public WriterFileEventListener(long version) {
			this.version = version;
		}

		@Override
		protected void registerFileClosed(
			DatabaseConnection c, String fileName, Object fileTag, int fileSize) throws SQLException {

			super.registerFileClosed(c, fileName, fileTag, fileSize);
			if (fileSize < config.getLogFileUnderflow()) {
				String statement =
					insertOrIgnoreInto(gcFileQueueTable).columns(gcFileQueue.file).values(rowOf((long)fileTag))
						.build();
				c.insert(statement, EMPTY_ARGS, EMPTY_TYPES, null);
				underflowRegistered = true;
			}
		}

		@Override
		protected Long getSinceVersion() {
			return version;
		}

		public boolean isUnderflowRegistered() {
			return underflowRegistered;
		}
	}
	// endregion

	// region Transaction implementations
	private class ReaderImpl extends AbstractReader {

		public ReaderImpl(long version) {
			super(version);
		}

		@Override
		public boolean exists(Key key) throws StorageException {
			try {
				return dbOperations.useReadonlyConnection(false, c -> {
					String statement =
						select(coalesce(
							select(entry.deletedRowId().isNull()).from(table(entryTable))
								.where(entry.id().eq(value(key.get())).and(entry.version().leq(value(version))))
								.orderBy(entry.version(), DESC).limit(1),
							value(0)))
						.build();
					return 1 == c.queryForLong(statement);
				});
			} catch (SQLException e) {
				throw new StorageException(e);
			}
		}

		@Override
		public Optional<Value> get(Key key) throws StorageException {
			FileSegment location = getReadLocation(key);
			if (location == null) {
				return Optional.empty();
			}

			do {
				try {
					return Optional.of(new ByteArrayValue(readAtLocation(location)));
				} catch (IOException e) {
					FileSegment refreshedLocation = getReadLocation(key);
					if (refreshedLocation.getFile().equals(location.getFile())) {
						throw new StorageException(e);
					}
				}
			} while (true); // retry if the GC has moved the record
		}

		private FileSegment getReadLocation(Key key) throws StorageException {
			String statement =
				with("LastEntry").ofColumns("file", "offset", "size", "deletedRowId")
				.as(select(entry.file(), entry.offset(), entry.size(), entry.deletedRowId())
					.from(table(entryTable))
					.where(entry.id().eq(value(key.get())).and(entry.version().leq(value(version))))
					.orderBy(entry.version(), DESC).limit(1))
				.select(file.name(), column("file"), column("offset"), column("size"))
				.from(table("LastEntry").innerJoin(file).on(column("file").eq(file.id())))
				.where(column("deletedRowId").isNull())
				.build();

			FileSegment file;
			try {
				file = dbOperations.useReadonlyConnection(false, c -> {
					List<FileSegment> result = dbOperations.selectQuery(statement,
						c, r -> new FileSegment(r.getString(0), r.getInt(1), r.getInt(2), r.getInt(3)));
					return result.isEmpty() ? null : result.get(0);
				});
			} catch (SQLException e) {
				throw new StorageException(e);
			}

			return file;
		}

		protected byte[] readAtLocation(FileSegment location) throws IOException {
			ByteBuffer buffer = ByteBuffer.allocate(location.getSize());
			try (FileChannel channel =
				FileChannel.open(getPath(location.getFile()), StandardOpenOption.READ)) {

				long bytesRead = 0;
				while (bytesRead < location.getSize()) {
					bytesRead += channel.read(buffer, location.getOffset() + bytesRead);
				}
			}

			return buffer.array();
		}
	}

	private class WriterImpl extends AbstractWriter {

		/**
		 * Shows if the current transaction has set the uncommitted changes flag in the database.
		 */
		private volatile boolean dbFlagSet = false;

		/**
		 * Synchronization object used for double-checked setting (only!) of {@link #dbFlagSet}.
		 */
		private final Object dbFlagSettingLock = new Object();

		/**
		 * Shows if the current transaction has performed any deletions on the previous versions.
		 */
		private boolean hasDeletions = false;

		private SplittingWriter storage;

		private WriterFileEventListener fileEventListener;

		public WriterImpl(long version) throws StorageException {
			super(version);
			resetStorage(version);
		}

		@Override
		public void put(Key key, Value value) throws StorageException {
			checkShowstopper();

			try {
				lazyPrepareTransaction();
				hasChanges = true;

				FileSegment location = storage.write(value);
				dbOperations.useConnection(false, c -> {
					removePreviousVersion(key.get(), c);
					String statement =
						insertOrReplaceInto(entryTable)
							.columns(entry.id, entry.version, entry.deletedRowId, entry.file, entry.offset, entry.size)
						.values(rowOf(
							value(key.get()), value(getVersion()), NULL, value((long)location.getFileTag()),
							value(location.getOffset()), value(location.getSize())))
						.build();
					c.insert(statement, EMPTY_ARGS, EMPTY_TYPES, null);
					return null;
				});
			} catch (SQLException | IOException e) {
				throw new StorageException(e);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new StorageException(e);
			}
		}

		@Override
		public boolean remove(Key key) throws StorageException {
			checkShowstopper();

			boolean existed;
			try {
				lazyPrepareTransaction();

				existed = dbOperations.useConnection(true, c -> {
					String statement = deleteFrom(entryTable).where(
						entry.id().eq(value(key.get()))
						.and(entry.version().eq(value(getVersion())))
						.and(entry.deletedRowId().isNull()))
						.build();
					boolean newVersionDeleted = c.delete(statement, EMPTY_ARGS, EMPTY_TYPES) == 1;

					// remove visible data (from previous versions) for the key, if any
					boolean previousVersionDeleted = removePreviousVersion(key.get(), c);

					return newVersionDeleted || previousVersionDeleted;
				});
			} catch (SQLException e) {
				throw new StorageException(e);
			}

			hasChanges |= existed;
			return existed;
		}

		@Override
		public void rollback() throws StorageException {
			if (!hasChanges) {
				return;
			}

			try {
				storage.close();

				Long guardRowId = dbOperations.useReadonlyConnection(false,
					c -> dbOperations.selectQuery(guardRowIdQuery, c, r -> r.getLong(0)))
						.stream().findFirst().orElse(null);
				if (guardRowId != null) {
					// delete all changes to the Entry table having version of the writer and the previous one
					// (see comment to nextVersion(long) to find out why two versions are affected)
					deleteUncommittedEntries(guardRowId);
				} // else this part of rollback was already performed

				// all the new files pertaining to the failed commit must be moved to deletion queue
				deleteUncommittedFiles();

				// the internal version is switched but the committed version is not;
				// unset the database flag indicating that there are uncommitted changes;
				// drop the ROWID guard
				updateDbAfterRollback(guardRowId);
			} catch (SQLException | IOException e) {
				throw new StorageException(e);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new StorageException(e);
			}

			long version = getVersion();
			synchronized (versionLock) {
				internalVersion = version;
			}
			afterCommit(nextVersion(version));

			dbFlagSet = false;
			hasDeletions = false;

			config.getGcScheduler().enqueueGc(gcAgent);
		}

		/**
		 * Only to be used in the rollback-on-initialization scenario.
		 */
		public void markDirty() {
			hasChanges = true;
		}

		@Override
		protected long commitImpl() throws StorageException {
			checkShowstopper();

			long currentWriterVersion = getVersion();

			if (hasChanges) {
				try {
					// make sure the last file written to is synchronized to disk and a record is created for it
					storage.close();

					Long guardRowId = dbOperations.useReadonlyConnection(false,
						c -> dbOperations.selectQuery(guardRowIdQuery, c, r -> r.getLong(0)))
						.stream().findFirst().orElse(null);

					if (guardRowId != null) {
						updateFileSpaceUsage(guardRowId);
					} // otherwise no entries were created in the database

					updateDbAfterCommit(currentWriterVersion, guardRowId);
				} catch (IOException | SQLException e) {
					throw new StorageException(e);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new StorageException(e);
				}

				synchronized (versionLock) {
					currentVersion = internalVersion = currentWriterVersion;
				}

				if (hasDeletions || fileEventListener.isUnderflowRegistered()) {
					config.getGcScheduler().enqueueGc(gcAgent);
				}

				hasChanges = false;
				hasDeletions = false;

				return nextVersion(currentWriterVersion);
			} else {
				return currentWriterVersion;
			}
		}

		@Override
		protected void afterCommit(long newVersion) throws StorageException {
			super.afterCommit(newVersion);
			resetStorage(newVersion);
		}

		private void resetStorage(long version) {
			createStorage(version);
		}

		private void createStorage(long version) {
			this.storage = new SplittingWriter(getWriterConfig(getFileNameSupplier(version)));
			this.fileEventListener = new WriterFileEventListener(version);
			this.storage.setEventListener(fileEventListener);
		}

		private void updateFileSpaceUsage(long guardRowId) throws SQLException {
			// the approach below may be simplified by introducing indices but that gravely affects performance

			List<UsedSpace> usage = dbOperations.useReadonlyConnection(false, c ->
				dbOperations.selectQuery(
					select(entry.file(), coalesce(sum(entry.size()), value(0))).from(table(entryTable))
						.where(rowId().gt(value(guardRowId)).and(entry.deletedRowId().isNull()))
						.groupBy(entry.file())
					.build(),
					c, r -> new UsedSpace(r.getLong(0), r.getInt(1))));

			int batchSize = 1000;
			for (int i = 0; i < usage.size(); i += batchSize) {
				List<UsedSpace> sublist = usage.subList(i, Math.min(i + batchSize, usage.size()));
				dbOperations.useConnection(true, c -> {
					for (UsedSpace usedSpace : sublist) {
						c.update(
							update(fileTable)
								.set(file.unusedBytes, file.fullSize().minus(value(usedSpace.getUsedSpace())))
								.where(file.id().eq(value(usedSpace.getFileId())))
							.build(), EMPTY_ARGS, EMPTY_TYPES);
					}
					return null;
				});
			}
		}

		private boolean removePreviousVersion(byte[] key, DatabaseConnection c) throws SQLException {
			boolean result = 1 == c.insert(
				with("PreviousVersion").ofColumns("previousVersionRowId", "deletedRowId")
				.as(select(rowId(), entry.deletedRowId()).from(table(entryTable))
					.where(entry.id().eq(value(key)).and(entry.version().leq(value(getVersion() - 2))))
					.orderBy(entry.version(), DESC).limit(1))
				.insertOrIgnoreInto(entryTable)
					.columns(entry.id, entry.version, entry.deletedRowId, entry.file, entry.offset, entry.size)
				.from(
					select(
						value(key), value(getVersion() - 1), column("previousVersionRowId"),
						NULL, value(0), value(0))
					.from(table("PreviousVersion")).where(column("deletedRowId").isNull()))
				.build(), EMPTY_ARGS, EMPTY_TYPES, null);
			hasDeletions |= result;

			return result;
		}

		/**
		 * Checks that the writing transaction can proceed and updates the database
		 * to reflect the presence of changes.
		 * As this {@link Writer} is meant to be wrapped in {@link ThreadSafeWriter}, it has
		 * to be thread-safe unless one key is accessed for modification concurrently. This implies
		 * that lazy writing transaction initialization must be intrinsically thread-safe.
		 * Note that the unsetting of the flag in multithreaded scenarios still requires
		 * appropriate external synchronization delivered by {@link ThreadSafeWriter}.
		 */
		private void lazyPrepareTransaction() throws SQLException, StorageException {
			if (!dbFlagSet) { // double check
				synchronized (dbFlagSettingLock) {
					doLazyPrepareTransaction();
				}
			}
		}

		private void doLazyPrepareTransaction() throws SQLException, StorageException {
			if (!dbFlagSet) { // check the previous transaction was properly ended
				boolean canProceed = dbOperations.useConnection(true, c -> {
					boolean wasSet = 0 == c.update(
						update(metaTable)
							.set(meta.hasUncommittedChanges, value(1))
							.where(meta.hasUncommittedChanges().eq(value(0)))
						.build(), EMPTY_ARGS, EMPTY_TYPES);
					if (wasSet) { // illegal state, the transaction cannot proceed
						return false;
					}

					/*
					 * insert a 'guarding' Entry row which does not affect the visible state of the
					 * storage; by adding this guard in the beginning of a writing transaction we guarantee
					 * that the entries inserted in the Entry table during the transaction will have ROWID greater
					 * than that of the guard;
					 * note that simply memorizing MAX(ROWID) at the beginning of a transaction will not suffice
					 * because the row with the preceding ROWID may be concurrently removed by the garbage collector
					 * (if it is a deletion entry) thus decreasing MAX(ROWID)
					 */
					c.insert(
						insertInto(entryTable)
							.columns(entry.id, entry.version, entry.deletedRowId, entry.file, entry.offset, entry.size)
							.values(rowOf(value(new byte[0]), value(Long.MAX_VALUE), NULL, NULL, value(0), value(0)))
						.build(), EMPTY_ARGS, EMPTY_TYPES, null);
					return true;
				});

				if (!canProceed) {
					throw new StorageException(
						"Cannot perform write as the previous transaction was not rolled back properly.");
				}

				dbFlagSet = true;
			}
		}

		private void deleteUncommittedEntries(long guardRowId) throws SQLException {
			// just as in case with updateFileSpaceUsage(), instead of adding an index,
			// the fact that the transaction entries have the greatest ROWIDs is used

			int batchSize = 3_000;

			int deletedRows;
			do {
				deletedRows = dbOperations.useConnection(false,
					c -> c.delete(
						deleteFrom(entryTable)
							.where(rowId().gt(value(guardRowId))
								.and(rowId().leq(coalesce(
									select(min(rowId())).from(table(entryTable)).where(rowId().gt(value(guardRowId)))
										.plus(value(batchSize)),
									select(max(rowId())).from(table(entryTable))))))
						.build(), EMPTY_ARGS, EMPTY_TYPES));
			} while (deletedRows > 0);
		}

		private void deleteUncommittedFiles() throws SQLException {
			long committedVersion = dbOperations.useConnection(false,
				c -> c.queryForLong(select(meta.committedVersion()).from(meta).build()));

			// the files pertaining to the last transaction are in the end of the table
			// (with regard to the ever-increasing id field) and may only alternate with
			// GC files (which have NULL in sinceVersion field)
			int batchSize = 200;
			boolean hasMore;
			long maxId = dbOperations.useReadonlyConnection(false,
				c -> c.queryForLong(select(coalesce(max(file.id()), value(0))).from(file).build())) + 1;
			do {
				long continueFromId = maxId;
				List<FileVersion> files = dbOperations.useReadonlyConnection(false, c ->
					dbOperations.selectQuery(
						select(file.id(), file.name(), file.sinceVersion()).from(file)
							.where(file.id().lt(value(continueFromId)))
							.orderBy(file.id(), DESC).limit(batchSize)
						.build(),
						c, r -> {
							long sinceVersion = r.getLong(2);
							return new FileVersion(r.getLong(0), r.getString(1), sinceVersion, r.wasNull(2));
						}));
				if (files.isEmpty()) {
					hasMore = false;
				} else {
					maxId = files.get(files.size() - 1).getId();
					List<FileVersion> deletable = new ArrayList<>();
					hasMore = true;
					for (FileVersion file : files) {
						if (!file.isGcFile()) {
							if (file.getSinceVersion() <= committedVersion) {
								hasMore = false;
								break;
							} else {
								deletable.add(file);
							}
						}
					}

					if (!deletable.isEmpty()) {
						String insertionStatement =
							insertInto(fileDeletionQueueTable).columns(fileDeletionQueue.name)
							.from(stringValues(
								deletable.stream().map(FileVersion::getName).collect(Collectors.toList())))
							.build();
						String deletionStatement = deleteFrom(fileTable)
							.where(file.id().in(deletable.stream().mapToLong(FileVersion::getId).toArray()))
							.build();

						dbOperations.useConnection(true, c -> {
							c.insert(insertionStatement, EMPTY_ARGS, EMPTY_TYPES, null);
							c.delete(deletionStatement, EMPTY_ARGS, EMPTY_TYPES);
							return null;
						});
					}
				}
			} while (hasMore);
		}

		private void updateDbAfterCommit(long currentWriterVersion, Long guardRowId) throws SQLException {
			dbOperations.useConnection(true, c -> {
				c.update(
					update(metaTable)
						.set(meta.internalVersion, value(currentWriterVersion))
						.set(meta.committedVersion, value(currentWriterVersion))
						.set(meta.hasUncommittedChanges, value(0))
					.build(), EMPTY_ARGS, EMPTY_TYPES);
				c.delete(
					deleteFrom(entryTable).where(rowId().eq(value(guardRowId))).build(),
					EMPTY_ARGS, EMPTY_TYPES);
				return null;
			});
			dbFlagSet = false;
		}

		private void updateDbAfterRollback(Long guardRowId) throws SQLException {
			// only switches the internal version, the last committed version stays intact
			dbOperations.useConnection(true, c -> {
				c.update(
					update(metaTable)
						.set(meta.internalVersion, value(getVersion()))
						.set(meta.hasUncommittedChanges, value(0))
					.build(), EMPTY_ARGS, EMPTY_TYPES);

				if (guardRowId != null) {
					// remove the transaction ROWID guard
					c.delete(
						deleteFrom(entryTable).where(rowId().eq(value(guardRowId))).build(),
						EMPTY_ARGS, EMPTY_TYPES);
				}
				return null;
			});
		}
	}
	// endregion

	/**
	 * This class is for single-threaded use only.
	 */
	private static class GcSessionStateHolder {

		private final GcResult lastBatchResult;

		private final long lastBatchMaxRowId;

		private final boolean finished;

		public GcSessionStateHolder(
			GcResult lastBatchResult, long lastBatchMaxRowId, boolean finished) {

			this.lastBatchResult = lastBatchResult;
			this.lastBatchMaxRowId = lastBatchMaxRowId;
			this.finished = finished;
		}

		public GcResult getLastBatchResult() {
			return lastBatchResult;
		}

		public long getLastBatchMaxRowId() {
			return lastBatchMaxRowId;
		}

		public boolean isFinished() {
			return finished;
		}
	}

	//region Table helper constants
	private final String entryTable = "Entry";

	private final String fileTable = "File";

	private final String metaTable = "Meta";

	private final String gcSavepointTable = "GcSavepoint";

	private final String gcFileQueueTable = "GcFileQueue";

	private final String gcUncommittedFilesTable = "GcUncommittedFiles";

	private final String fileDeletionQueueTable = "FileDeletionQueue";

	private final EntryTable entry = new EntryTable();

	private final FileTable file = new FileTable();

	private final MetaTable meta = new MetaTable();

	private final GcSavepointTable gcSavepoint = new GcSavepointTable();

	private final GcFileQueueTable gcFileQueue = new GcFileQueueTable();

	private final GcUncommittedFilesTable gcUncommittedFiles = new GcUncommittedFilesTable();

	private final FileDeletionQueueTable fileDeletionQueue = new FileDeletionQueueTable();
	//endregion

	// private static final Logger log = LoggerFactory.getLogger(GroupingFileStorage.class);

	private static final FieldType[] EMPTY_TYPES = new FieldType[0];

	private static final Object[] EMPTY_ARGS = new Object[0];

	private final List<String> dbCreationStatements = Collections.unmodifiableList(Arrays.asList(
		// the default ROWID generation algorithm is used (MAX(ROWID) + 1)
		// if deletedRowId is not null then file, offset, size must be filled, otherwise this is a deletion record
		createTable(entryTable)
			.addColumn(entry.id).ofType(VARBINARY).withColumnConstraint().notNull()
			.addColumn(entry.version).ofType(INTEGER).withColumnConstraint().notNull()
			.addColumn(entry.deletedRowId).ofType(INTEGER)
			.addColumn(entry.file).ofType(INTEGER)
			.addColumn(entry.offset).ofType(INTEGER).withColumnConstraint().notNull()
			.addColumn(entry.size).ofType(INTEGER).withColumnConstraint().notNull()
			.withTableConstraint().primaryKey().addColumn(entry.id).addColumn(entry.version)
		.build(),
		// fullSize is set when the file is closed
		// unusedBytes is 0 unless some entry is removed from the file
		// sinceVersion is NULL for GC files, otherwise reflects the version of the creating writer
		createTable(fileTable)
			.addColumn(file.id).ofType(INTEGER).withColumnConstraint().primaryKey()
			.addColumn(file.name).ofType(VARCHAR).withColumnConstraint().notNull()
			.addColumn(file.sinceVersion).ofType(INTEGER)
			.addColumn(file.fullSize).ofType(INTEGER)
			.addColumn(file.unusedBytes).ofType(INTEGER).withColumnConstraint().notNull()
		.build(),
		createTable(metaTable)
			.addColumn(meta.internalVersion).ofType(INTEGER).withColumnConstraint().primaryKey()
			.addColumn(meta.committedVersion).ofType(INTEGER).withColumnConstraint().notNull()
			.addColumn(meta.hasUncommittedChanges).ofType(BIT).withColumnConstraint().notNull()
		.build(),
		// file and offset designate the position from which the GC might proceed with compacting
		// rowId is the ROWID in the Entry table from which the GC must continue searching for deletion records
		// (in order to not maintain the index on (deletedRowId IS NULL) flag)
		createTable(gcSavepointTable)
			.addColumn(gcSavepoint.file).ofType(INTEGER)
			.addColumn(gcSavepoint.offset).ofType(INTEGER).withColumnConstraint().notNull()
			.addColumn(gcSavepoint.lastRowId).ofType(INTEGER)
		.build(),
		// file is the file id and not a primary key (the order of ROWID must be the order of insertion)
		createTable(gcFileQueueTable)
			.addColumn(gcFileQueue.file).ofType(INTEGER).withColumnConstraint().notNull().build(),
		createTable(gcUncommittedFilesTable)
			.addColumn(gcUncommittedFiles.file).ofType(INTEGER).withColumnConstraint().notNull().build(),
		createTable(fileDeletionQueueTable)
			.addColumn(fileDeletionQueue.name).ofType(VARCHAR).withColumnConstraint().notNull().build(),
		// the index used by the GC
		createIndex().named("IDX_Entry_file")
			.onTable(entryTable).addColumn(entry.file).where(entry.deletedRowId().isNull())
		.build(),
		// the initial version
		insertInto(metaTable).columns(meta.internalVersion, meta.committedVersion, meta.hasUncommittedChanges)
			.values(rowOf(0, 0, 0)).build(),
		// the initial GC state
		insertInto(gcSavepointTable).columns(gcSavepoint.file, gcSavepoint.offset, gcSavepoint.lastRowId)
			.values(rowOf(NULL, value(0), value(0))).build()));

	private final String guardRowIdQuery =
		select(rowId()).from(table(entryTable))
			.where(entry.id().eq(value(new byte[0])).and(entry.version().eq(value(Long.MAX_VALUE))))
		.build();

	private final GroupingFileStorageConfig config;

	/**
	 * The principal database, with forced synchronization to disk.
	 */
	private final ConnectionSource mainDb;

	private final DirectDbOperations dbOperations;

	private final AtomicLong fileIdGenerator;

	private final GcAgent gcAgent;

	private long internalVersion;

	/**
	 * Stores a critical error, should one occur, preventing the future writes in the storage.
	 */
	private volatile Throwable showstopper = null;

	/**
	 * Initializes a storage using the supplied configuration. If a storage already exists
	 * in the directory specified in the configuration, it is opened.
	 * @param config the storage configuration
	 */
	public GroupingFileStorage(GroupingFileStorageConfig config) throws SQLException {

		String path = config.getDirectory().toString();
		if (!path.endsWith("/") && !path.endsWith("\\")) {
			path += "/";
		}

		this.config = config;
		this.mainDb = new PooledSqliteConnectionSource(String.format("jdbc:sqlite:%s%s", path, "index.db"));
		this.dbOperations = new DirectDbOperations(this.mainDb);

		Meta meta = initialize();
		this.fileIdGenerator = new AtomicLong(getCurrentFileId());

		synchronized (versionLock) {
			this.currentVersion = meta.getCommittedVersion();
			this.internalVersion = meta.getInternalVersion();
		}

		this.gcAgent = interruption -> {
			try {
				return recordCleanupCycle(interruption);
			} catch (OutOfMemoryError e) { // this one may prove recoverable
				return GcResult.RECORDS_REMAIN;
			} catch (Throwable t) {
				halt(t);
				throw t;
			}
		};

		config.getGcScheduler().register(gcAgent);
	}

	@Override
	public void close() throws Exception {
		config.getGcScheduler().deregister(gcAgent);
		mainDb.close();
	}

	@Override
	protected Reader produceReader(long version) {
		return new ReaderImpl(version);
	}

	@Override
	protected Writer produceWriter() throws StorageException {
		return new ThreadSafeWriter(new WriterImpl(nextVersion(internalVersion)));
	}

	private Meta initialize() throws SQLException {

		Meta metadata = dbOperations.useConnection(true, c -> {
			boolean initialized = 1 == c.queryForLong(
				select(countAll()).from(table("sqlite_master"))
					.where(column("type").eq(value("table")).and(column("name").eq(value("Meta"))))
				.build());
			if (initialized) {
				return dbOperations.selectQuery(
					select(meta.internalVersion(), meta.committedVersion(), meta.hasUncommittedChanges())
					.from(meta).build(),
					c, r -> new Meta(r.getLong(0), r.getLong(1), r.getBoolean(2))).get(0);
			} else {
				for (String statement : dbCreationStatements) {
					c.executeStatement(statement, DatabaseConnection.DEFAULT_RESULT_FLAGS);
				}
				return new Meta(0, 0, false);
			}
		});

		if (metadata.hasUncommittedChanges()) {
			CountDownLatch latch = new CountDownLatch(1);

			Thread restoreThread = new Thread(() -> {
				try (WriterImpl writer = (WriterImpl)((ThreadSafeWriter)getWriter()).getUnderlyingWriter()) {
					latch.countDown();
					writer.markDirty(); // so that when closing, will roll back the failed transaction
				} catch (Throwable e) {
					halt(e);
				}
			}, "repo-restore");
			restoreThread.setDaemon(true);
			restoreThread.start();
			try {
				latch.await();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new RuntimeException(e);
			}
		}

		return metadata;
	}

	private long getCurrentFileId() throws SQLException {
		return this.dbOperations.useReadonlyConnection(false, c ->
			c.queryForLong(select(coalesce(max(file.id()), value(0))).from(file).build()));
	}

	private GcResult recordCleanupCycle(BooleanSupplier interruption) throws SQLException, IOException {

		GcSavepoint savepoint = prepareGcSession();

		long versionSnapshot;
		synchronized (versionLock) {
			versionSnapshot = currentVersion;
		}

		try {
			GcResult result = performEntryAnnihilation(interruption, savepoint, versionSnapshot);

			if (result != GcResult.INTERRUPTED) {
				try (SplittingWriter writer = getGcWriter(savepoint)) {
					writer.setEventListener(new GcFileEventListener());

					result = GcResult.merge(result, performFileCompaction(writer, interruption));
				}
			}
			if (result != GcResult.INTERRUPTED) {
				result = GcResult.merge(result, performFileRemovals(interruption));
			}

			return result;
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return GcResult.INTERRUPTED;
		}
	}

	private GcSavepoint prepareGcSession() throws SQLException {
		GcSavepoint lastSavepoint = getGcSavepoint();
		// move the uncommitted files of the last GC session to the removal set
		deleteUncommittedGcFiles();
		return lastSavepoint;
	}

	private void deleteUncommittedGcFiles() throws SQLException {
		dbOperations.useConnection(true,
			c -> {
				c.insert(
					insertInto(fileDeletionQueueTable).columns(fileDeletionQueue.name)
					.from(select(file.name())
						.from(file.innerJoin(gcUncommittedFiles).on(file.id().eq(gcUncommittedFiles.file()))))
					.build(), EMPTY_ARGS, EMPTY_TYPES, null);
				c.delete(
					deleteFrom(fileTable)
						.where(file.id().in(select(gcUncommittedFiles.file()).from(gcUncommittedFiles)))
					.build(), EMPTY_ARGS, EMPTY_TYPES);
				c.delete(deleteFrom(gcUncommittedFilesTable).build(), EMPTY_ARGS, EMPTY_TYPES);
				return null;
			});
	}

	private GcResult performEntryAnnihilation(
		BooleanSupplier interruption, GcSavepoint savepoint, long versionSnapshot) throws SQLException {

		GcSessionStateHolder stateHolder =
			new GcSessionStateHolder(GcResult.DONE, savepoint.getRowId(), false);

		while (!interruption.getAsBoolean() && !stateHolder.isFinished()) {
			stateHolder = cleanupRecordBatch(stateHolder, versionSnapshot);
		}
		return stateHolder.getLastBatchResult();
	}

	private GcSessionStateHolder cleanupRecordBatch(GcSessionStateHolder gcStateBefore, long version)
		throws SQLException {

		final int batchSize = 1000;

		// fetch a bunch of deletion entries
		List<DeletedEntry> batch = getDeletedEntriesBatch(gcStateBefore, version, batchSize);
		if (batch.isEmpty()) {
			return new GcSessionStateHolder(
				gcStateBefore.getLastBatchResult(), gcStateBefore.getLastBatchMaxRowId(), true);
		}

		DeletedEntry lastOne = batch.get(batch.size() - 1);
		long lastRowId = dbOperations.useReadonlyConnection(false, c -> c.queryForLong(
			select(rowId()).from(table(entryTable))
				.where(entry.id().eq(value(lastOne.getId()))
					.and(entry.version().eq(value(lastOne.getDeletionVersion()))))
			.build()));

		// see which (insert, delete) pairs can be mutually cancelled out
		// (no existing reader being interested in the intermediate versions)
		List<DeletedEntry> deletable = batch.stream()
			.filter(t -> {
				Long leastConcernedReaderVersion = readerVersions.ceilingKey(t.getInsertionVersion());
				return leastConcernedReaderVersion == null || leastConcernedReaderVersion >= t.getDeletionVersion();
			})
			.collect(Collectors.toList());
		if (deletable.isEmpty()) {
			return new GcSessionStateHolder(
				GcResult.RECORDS_REMAIN, gcStateBefore.getLastBatchMaxRowId(), false);
		}

		GcResult result = performEntryDeletions(gcStateBefore, batch, lastRowId, deletable);
		return new GcSessionStateHolder(result, lastRowId, false);
	}

	private GcResult performEntryDeletions(GcSessionStateHolder gcStateBefore,
		List<DeletedEntry> batch, long lastRowId, List<DeletedEntry> deletable) throws SQLException {

		boolean allCollectable = deletable.size() == batch.size();

		Map<Long, Integer> fileUsageChanges = deletable.stream().collect(Collectors.groupingBy(
			DeletedEntry::getFileId, Collectors.summingInt(DeletedEntry::getSize)));

		String entryDeletionStatement = getBatchDeletionStatement(deletable);

		// get the actual file usage for all the affected files
		Map<Long, FileInfo> fileInfo = dbOperations.useReadonlyConnection(false, c ->
			dbOperations.selectQuery(
				select(file.id(), file.fullSize(), file.unusedBytes())
					.from(file)
					.where(file.id().in(fileUsageChanges.keySet().stream().mapToLong(i -> i).toArray()))
				.build(),
				c, r -> new FileInfo(r.getLong(0), r.getInt(1), r.getInt(2)))
			.stream().collect(Collectors.toMap(FileInfo::getId, Function.identity())));

		List<String> updateStatements = new ArrayList<>();
		List<Long> enqueueIds = new ArrayList<>();
		for (Map.Entry<Long, Integer> entry : fileUsageChanges.entrySet()) {
			FileInfo fileState = fileInfo.get(entry.getKey());
			int unusedBefore = fileState.getUnusedBytes();
			int unusedAfter = unusedBefore + entry.getValue();
			if (!isFitForCompaction(unusedBefore, fileState.getFullSize())
				&& isFitForCompaction(unusedAfter, fileState.getFullSize())) {

				enqueueIds.add(fileState.getId());
			}
			updateStatements.add(
				update(fileTable).set(file.unusedBytes, value(unusedAfter)).where(file.id().eq(value(entry.getKey())))
				.build());
		}
		String enqueueStatement = enqueueIds.isEmpty()
			? null
			: insertOrIgnoreInto(gcFileQueueTable).columns(gcFileQueue.file).from(numericValues(enqueueIds)).build();

		dbOperations.useConnection(true, c -> { // necessarily in one transaction
			// delete the entry pairs
			c.delete(entryDeletionStatement, EMPTY_ARGS, EMPTY_TYPES);

			// update the file usage
			for (String statement : updateStatements) {
				c.update(statement, EMPTY_ARGS, EMPTY_TYPES);
			}
			if (enqueueStatement != null) {
				c.insert(enqueueStatement, EMPTY_ARGS, EMPTY_TYPES, null);
			}

			// if no deletion entries were skipped in the current GC session,
			// then advance the GC cursor to the last ROWID
			if (gcStateBefore.getLastBatchResult() == GcResult.DONE && allCollectable) {
				c.update(update(gcSavepointTable)
					.set(gcSavepoint.lastRowId, value(lastRowId)).build(), EMPTY_ARGS, EMPTY_TYPES);
			}

			return null;
		});

		return GcResult.DONE;
	}

	private GcResult performFileCompaction(SplittingWriter writer, BooleanSupplier interruption)
		throws SQLException, IOException, InterruptedException {

		GcFileQueueElement element;
		do {
			if (interruption.getAsBoolean()) {
				return GcResult.INTERRUPTED;
			}

			element = dbOperations.useReadonlyConnection(false, c ->
				dbOperations.selectQuery(
					select(rowIdOf(gcFileQueueTable), gcFileQueue.file(), file.name())
					.from(gcFileQueue.innerJoin(file).on(gcFileQueue.file().eq(file.id())))
					.where(value(1).eq(coalesce(
						gcFileQueue.file().neq(select(gcSavepoint.file()).from(gcSavepoint)),
						value(1))))
					.orderBy(rowIdOf(gcFileQueueTable)).limit(1)
					.build(),
					c, r -> new GcFileQueueElement(r.getLong(0), r.getLong(1), r.getString(2))))
				.stream().findFirst().orElse(null);
			if (element != null) {
				if (!performSingleFileCompaction(element, writer, interruption)) {
					return GcResult.INTERRUPTED;
				}
			}
		} while (element != null);

		return GcResult.DONE;
	}

	private GcResult performFileRemovals(BooleanSupplier interruption) throws SQLException {
		GcResult result = GcResult.DONE;

		// assuming there are not too many of these added between two GC batches
		List<FileToDelete> fileNames = dbOperations.useReadonlyConnection(false, c ->
			dbOperations.selectQuery(
				select(rowId(), fileDeletionQueue.name()).from(fileDeletionQueue).orderBy(rowId()).build(),
				c, r -> new FileToDelete(r.getLong(0), r.getString(1))));
		List<Long> deleted = new ArrayList<>();
		for (FileToDelete fileName : fileNames) {
			if (interruption.getAsBoolean()) {
				return GcResult.INTERRUPTED;
			}

			boolean success = true;
			try {
				Files.delete(getPath(fileName.getName()));
			} catch (NoSuchFileException e) {
				// that is the desired state
			} catch (IOException e) {
				success = false;
			}

			if (success) {
				deleted.add(fileName.getRowId());
			}
		}

		if (!deleted.isEmpty()) {
			dbOperations.useConnection(false, c ->
				c.delete(
					deleteFrom(fileDeletionQueueTable)
						.where(rowId().in(deleted.stream().mapToLong(i -> i).toArray()))
					.build(), EMPTY_ARGS, EMPTY_TYPES));
		}
		if (deleted.size() < fileNames.size()) {
			result = GcResult.RECORDS_REMAIN;
		}

		return result;
	}

	private List<DeletedEntry> getDeletedEntriesBatch(
		GcSessionStateHolder gcStateBefore, long version, int batchSize) throws SQLException {

		return dbOperations.useReadonlyConnection(false, c ->
			dbOperations.selectQuery(
				select(column("Del", "id"), column("Del", "version"),
					column("Ins", "version"), column("Ins", "file"), column("Ins", "size"))
				.from(
					select(rowId(), entry.id(), entry.version(), entry.deletedRowId())
						.from(table(entryTable))
						.where(rowId().gt(value(gcStateBefore.getLastBatchMaxRowId()))
							.and(entry.version().leq(value(version)))
							.and(entry.deletedRowId().isNotNull()))
						.orderBy(rowId()).limit(batchSize).alias("Del")
					.innerJoin(table(entryTable).alias("Ins")).on(rowIdOf("Ins").eq(column("Del", "deletedRowId"))))
				.orderBy(rowIdOf("Del"))
				.build(),
				c, r -> new DeletedEntry(r.getBytes(0), r.getLong(2), r.getLong(1), r.getLong(3), r.getInt(4))));
	}

	private boolean performSingleFileCompaction(
		GcFileQueueElement enqueuedFile, SplittingWriter writer, BooleanSupplier interruption)
		throws SQLException, IOException, InterruptedException {

		int batchSize = 1000;
		while (!interruption.getAsBoolean()) {
			// fetch a portion of entries remaining in the file
			List<OffsetSizePair> entries = getEntriesToCopyBatch(enqueuedFile, batchSize);
			if (entries.isEmpty()) {
				dbOperations.useConnection(true, c -> {
					c.delete(
						deleteFrom(gcFileQueueTable).where(rowId().eq(value(enqueuedFile.getRowId()))).build(),
						EMPTY_ARGS, EMPTY_TYPES);
					c.delete(
						deleteFrom(fileTable).where(file.id().eq(value(enqueuedFile.getFileId()))).build(),
						EMPTY_ARGS, EMPTY_TYPES);
					c.insert(
						insertInto(fileDeletionQueueTable).columns(fileDeletionQueue.name)
							.values().add(value(enqueuedFile.getFileName())).build(),
						EMPTY_ARGS, EMPTY_TYPES, null);

					return null;
				});
				break;
			}

			if (!moveEntries(enqueuedFile, writer, entries, interruption)) {
				return false;
			}
		}

		return !interruption.getAsBoolean();
	}

	private boolean moveEntries(GcFileQueueElement enqueuedFile, SplittingWriter writer,
		List<OffsetSizePair> entries, BooleanSupplier interruption)
		throws IOException, InterruptedException, SQLException {

		if (entries.isEmpty()) {
			throw new IllegalArgumentException("At least one entry must be present.");
		}

		// perform the actual moving
		List<String> updateStatements = new ArrayList<>();
		FileSegment newLocation = null;
		try (FileChannel channel = FileChannel.open(
			getPath(enqueuedFile.getFileName()), StandardOpenOption.READ)) {

			for (OffsetSizePair segment : entries) {
				if (interruption.getAsBoolean()) {
					return false;
				}
				newLocation = copyData(channel, segment, writer);
				updateStatements.add(
					update(entryTable)
						.set(entry.file, value((long)newLocation.getFileTag()))
						.set(entry.offset, value(newLocation.getOffset()))
					.where(rowId().eq(value(segment.getRowId())))
					.build());
			}
		}
		writer.sync();

		FileSegment lastTargetLocation = newLocation; // cannot be null
		dbOperations.useConnection(true, c -> { // in transaction
			// reset the references
			for (String update : updateStatements) {
				c.update(update, EMPTY_ARGS, EMPTY_TYPES);
			}

			// move target file offset; entries being written sequentially, there is a guarantee
			// that the last entry is in the last new file
			c.update(
				update(gcSavepointTable)
					.set(gcSavepoint.file, value((long)lastTargetLocation.getFileTag()))
					.set(gcSavepoint.offset, value(lastTargetLocation.getOffset() + lastTargetLocation.getSize()))
				.build(), EMPTY_ARGS, EMPTY_TYPES);

			// unmark uncommitted files
			c.delete(deleteFrom(gcUncommittedFilesTable).build(), EMPTY_ARGS, EMPTY_TYPES);

			return null;
		});

		return true;
	}

	private List<OffsetSizePair> getEntriesToCopyBatch(
		GcFileQueueElement enqueuedFile, int batchSize) throws SQLException {

		return dbOperations.useReadonlyConnection(false, c ->
			dbOperations.selectQuery(
				select(rowId(), entry.offset(), entry.size())
					.from(table(entryTable))
					.where(entry.file().eq(value(enqueuedFile.getFileId())))
					.limit(batchSize)
				.build(),
				c, r -> new OffsetSizePair(r.getLong(0), r.getInt(1), r.getInt(2))));
	}

	private FileSegment copyData(FileChannel channel, OffsetSizePair segment, SplittingWriter writer)
		throws IOException, InterruptedException {

		try (FileSegmentChannel segmentChannel =
			new FileSegmentChannel(channel, segment.getOffset(), segment.getSize())) {

			return writer.write(new ReadableChannelValue(segmentChannel, segment.getSize()));
		}
	}

	private boolean isFitForCompaction(int unused, int size) {
		return unused >= size * config.getMaxUnusedSpace();
	}

	private String getBatchDeletionStatement(List<DeletedEntry> deletable) {
		LinkedList<SqlExpression> alternatives = deletable.stream().map(
			t -> {
				// SQLite cannot apply an optimization if (A and (B or C)) is not manually expanded
				SqlExpression condition1 = entry.id().eq(value(t.getId()));
				return condition1.and(entry.version().eq(value(t.getInsertionVersion())))
					.or(condition1.and(entry.version().eq(value(t.getDeletionVersion()))));
			}
		).collect(Collectors.toCollection(LinkedList::new));

		// simple join of the ORs might lead to SQLite error (the maximum syntactic tree depth is 1000)
		// so we construct a binary balanced tree
		while (alternatives.size() > 1) {
			ListIterator<SqlExpression> iterator = alternatives.listIterator();
			while (iterator.hasNext()) {
				SqlExpression first = iterator.next();
				if (iterator.hasNext()) {
					iterator.remove();
					SqlExpression second = iterator.next();
					iterator.set(first.or(second));
				}
			}
		}
		// at this moment only one complex condition is left on the list

		return deleteFrom(entryTable).where(alternatives.getFirst()).build();
	}

	private SplittingWriter getGcWriter(GcSavepoint savepoint) throws IOException {
		Supplier<String> fileNameSupplier = getFileNameSupplier(null);
		return savepoint.getFile() == null
			? new SplittingWriter(getWriterConfig(fileNameSupplier))
			: new SplittingWriter(getWriterConfig(fileNameSupplier),
				savepoint.getFile(), savepoint.getOffset(), savepoint.getFileId());
	}

	private GcSavepoint getGcSavepoint() throws SQLException {
		return
			dbOperations.useReadonlyConnection(false, c -> dbOperations.selectQuery(
				select(file.id(), file.name(), gcSavepoint.offset(), gcSavepoint.lastRowId())
				.from(gcSavepoint.leftJoin(file).on(file.id().eq(gcSavepoint.file())))
				.build(),
				c, r -> {
					long fileId = r.getLong(0);
					return new GcSavepoint(
						r.wasNull(0) ? null : fileId, r.getString(1), r.getInt(2), r.getLong(3));
				})).get(0);
	}

	private Path getPath(String relativePath) {
		return config.getDirectory().resolve(relativePath);
	}

	/**
	 * @param version non-null for writers' name suppliers; null for GC
	 */
	private Supplier<String> getFileNameSupplier(Long version) {
		return () -> {
			String fileName = UUID.randomUUID().toString().replace("-", "_");
			return String.format(
				version == null ? "gc/%1$s/%2$s" : "commit/%1$s/%2$s",
				fileName.substring(0, 2), fileName.substring(2));
		};
	}

	private long nextVersion(long version) {
		// we add 2 instead of 1 because each write transaction is more naturally
		// represented as 2 sets of subsequent changes: first deletes, then inserts;
		// in this way, a replacement is represented as a succession of a deletion and an insertion
		return version + 2;
	}

	private void checkShowstopper() throws StorageException {
		if (showstopper != null) {
			throw new StorageException(
				"A critical error in the storage prevents future writes.", showstopper);
		}
	}

	private void halt(Throwable t) {
		showstopper = t;
		/*log.error(
			"The storage could not perform a critical operation and will be unavailable for writing.", t);*/
	}

	private SplittingWriterConfig getWriterConfig(Supplier<String> fileNameSupplier) {
		return new SplittingWriterConfig(config.getDirectory(), fileNameSupplier)
			.withMinLogFileSize(config.getMinLogFileSize())
			.withMaxLogFileSizeOvershoot(config.getMaxLogFileSizeOvershoot());
	}
}
