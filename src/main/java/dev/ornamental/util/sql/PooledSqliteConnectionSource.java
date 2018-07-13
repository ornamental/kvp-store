package dev.ornamental.util.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.j256.ormlite.db.DatabaseType;
import com.j256.ormlite.jdbc.JdbcDatabaseConnection;
import com.j256.ormlite.jdbc.JdbcPooledConnectionSource;
import com.j256.ormlite.support.ConnectionSource;
import com.j256.ormlite.support.DatabaseConnection;
import org.sqlite.SQLiteConfig;

public class PooledSqliteConnectionSource implements ConnectionSource {

	private static final int NUMBER_OF_CACHE_PAGES = 40_000;

	private static final long WRITE_LOCK_WAIT_TIMEOUT = 5_000L * 1_000_000;

	// needed due to a bug in the underlying JdbcPooledConnectionSource
	private AtomicBoolean closed = new AtomicBoolean(false);

	private final JdbcPooledConnectionSource roConnectionSource;

	private final String connectionUrl;

	private final String username;

	private final String password;

	private final Lock rwConnectionLock = new ReentrantLock(true);

	private final Condition rwConnectionLockCondition = rwConnectionLock.newCondition();

	private volatile DatabaseConnection readWriteConnection;

	private Thread rwConnectionOwner = null;

	private int ownerRecursion = 0;

	public PooledSqliteConnectionSource(final String connectionUrl) throws SQLException {
		this(connectionUrl, null, null);
	}

	public PooledSqliteConnectionSource(
		final String connectionUrl, final String username, final String password) throws SQLException {

		if (connectionUrl == null) {
			throw new IllegalArgumentException("Connection URL must be specified.");
		}

		this.connectionUrl = connectionUrl;
		this.username = username;
		this.password = password;
		this.roConnectionSource = new JdbcPooledConnectionSource(connectionUrl) {

			/**
			 * Only produces readonly connections.
			 */
			@Override
			protected DatabaseConnection makeConnection(com.j256.ormlite.logger.Logger logger) throws SQLException {
				return produceDatabaseConnection(true);
			}
		};

		// need to create a writable connection at once because if a rollback log is present
		// for the database and the first connection is read-only, it will fail to function
		// due to not being able to apply the rollback log
		this.readWriteConnection = produceDatabaseConnection(false);
	}

	@Override
	public DatabaseConnection getReadOnlyConnection() throws SQLException {
		DatabaseConnection readonly = roConnectionSource.getReadOnlyConnection();
		if (readonly == readWriteConnection
			/* no need in synchronization here: */
			|| Thread.currentThread() == rwConnectionOwner) {

			rwConnectionLock.lock();
			try {
				if (Thread.currentThread() != rwConnectionOwner) {
					throw new IllegalStateException();
				}
				if (readWriteConnection != readonly) {
					roConnectionSource.releaseConnection(readonly);
					readonly = readWriteConnection;
				}
				ownerRecursion++;
			} finally {
				rwConnectionLock.unlock();
			}
		}
		return readonly;
	}

	@Override
	public DatabaseConnection getReadWriteConnection() throws SQLException {
		rwConnectionLock.lock();
		try {
			long timeout = WRITE_LOCK_WAIT_TIMEOUT;
			while (rwConnectionOwner != null && rwConnectionOwner != Thread.currentThread() && timeout > 0) {
				long start = System.nanoTime();
				try {
					rwConnectionLockCondition.await(timeout, TimeUnit.NANOSECONDS);
				} catch (InterruptedException e) {
					throw new SQLException("The thread was interrupted while waiting for the RW connection.", e);
				}
				timeout -= System.nanoTime() - start;
			}

			if (rwConnectionOwner != null && rwConnectionOwner != Thread.currentThread()) {
				throw new SQLException(
					"The writing connection could not be obtained in 5 seconds. "
					+ "Possible deadlock or a concurring transaction takes too much time to complete.");
			}

			if (readWriteConnection == null || readWriteConnection.isClosed()) {
				readWriteConnection = produceDatabaseConnection(false);
			}
			rwConnectionOwner = Thread.currentThread();
			ownerRecursion++;
			return readWriteConnection;
		} finally {
			rwConnectionLock.unlock();
		}
	}

	@Override
	public void releaseConnection(DatabaseConnection connection) throws SQLException {
		if (readWriteConnection == connection) {
			rwConnectionLock.lock();
			try {
				if (Thread.currentThread() != rwConnectionOwner) {
					throw new IllegalStateException(
						"Release of the RW connection must be performed by the owning thread.");
				}

				ownerRecursion--;
				if (ownerRecursion == 0) {
					rwConnectionOwner = null;
					rwConnectionLockCondition.signalAll();
				}
			} finally {
				rwConnectionLock.unlock();
			}
		} else {
			roConnectionSource.releaseConnection(connection);
		}
	}

	@Override
	public void close() throws SQLException {
		if (closed.compareAndSet(false, true)) {
			try {
				roConnectionSource.close();
			} finally {
				readWriteConnection.close();
			}
		}
	}

	@Override
	public boolean saveSpecialConnection(DatabaseConnection connection) throws SQLException {
		return roConnectionSource.saveSpecialConnection(connection);
	}

	@Override
	public void clearSpecialConnection(DatabaseConnection connection) {
		roConnectionSource.clearSpecialConnection(connection);
	}

	@Override
	public boolean isOpen() {
		return roConnectionSource.isOpen();
	}

	@Override
	public void closeQuietly() {
		roConnectionSource.closeQuietly();
		readWriteConnection.closeQuietly();
	}

	@Override
	public DatabaseType getDatabaseType() {
		return roConnectionSource.getDatabaseType();
	}

	@Override
	public DatabaseConnection getSpecialConnection() {
		return roConnectionSource.getSpecialConnection();
	}

	private DatabaseConnection produceDatabaseConnection(final boolean readOnly) throws SQLException {

		Connection connection = null;
		try {
			SQLiteConfig config = new SQLiteConfig();
			config.setSynchronous(SQLiteConfig.SynchronousMode.NORMAL);
			config.setJournalMode(SQLiteConfig.JournalMode.WAL);
			config.setCacheSize(NUMBER_OF_CACHE_PAGES);
			if (readOnly) {
				config.setReadOnly(true);
			}
			config.enforceForeignKeys(true);
			Properties properties = config.toProperties();
			if (username != null) {
				properties.setProperty("user", username);
			}
			if (password != null) {
				properties.setProperty("password", password);
			}
			connection = DriverManager.getConnection(connectionUrl, config.toProperties());

			connection.setAutoCommit(true);
			return new JdbcDatabaseConnection(connection);
		} catch (SQLException ex) {
			if (connection != null) {
				try {
					connection.close();
				} catch (SQLException e) { /* ignore */ }
			}
			throw ex;
		}
	}
}
