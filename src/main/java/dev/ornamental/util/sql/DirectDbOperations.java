package dev.ornamental.util.sql;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.j256.ormlite.field.FieldType;
import com.j256.ormlite.stmt.StatementBuilder;
import com.j256.ormlite.support.CompiledStatement;
import com.j256.ormlite.support.ConnectionSource;
import com.j256.ormlite.support.DatabaseConnection;
import com.j256.ormlite.support.DatabaseResults;

public class DirectDbOperations {

	public interface RowConsumer {

		/**
		 * @param currentRow a {@link DatabaseResults} object positioned on the row to process
		 * @return {@literal true} if and only if the iteration must continue
		 */
		boolean consume(DatabaseResults currentRow) throws SQLException;
	}

	private static final FieldType[] EMPTY_TYPES = new FieldType[0];

	private final ConnectionSource db;

	public DirectDbOperations(ConnectionSource db) {
		this.db = db;
	}

	public <Q> Q useConnection(boolean transaction, ConnectionConsumer<Q> consumer) throws SQLException {
		return useConnection(false, transaction, consumer);
	}

	public <Q> Q useReadonlyConnection(boolean transaction, ConnectionConsumer<Q> consumer) throws SQLException {
		return useConnection(true, transaction, consumer);
	}

	/**
	 * @param consumer must return {@literal true} unless the result set iteration has to finish.
	 */
	public void selectQueryUnmapped(
		String query, DatabaseConnection connection, RowConsumer consumer)
		throws SQLException {

		CompiledStatement statement = null;
		DatabaseResults results = null;
		try {
			statement = connection.compileStatement(
				query, StatementBuilder.StatementType.SELECT,
				EMPTY_TYPES, DatabaseConnection.DEFAULT_RESULT_FLAGS);

			results = statement.runQuery(null);
			while (results.next()) {
				if (!consumer.consume(results)) {
					break;
				}
			}
		} finally {
			try {
				if (statement != null) {
					statement.close();
				}
			} finally {
				if (results != null) {
					results.close();
				}
			}
		}
	}

	public <Q> List<Q> selectQuery(
		String query, DatabaseConnection connection, RowReader<Q> singleItemReader)
		throws SQLException {

		List<Q> result = new ArrayList<>();
		selectQueryUnmapped(query, connection, r -> result.add(singleItemReader.readCurrentItem(r)));
		return result;
	}

	private <Q> Q useConnection(boolean ro, boolean transaction, ConnectionConsumer<Q> consumer)
		throws SQLException {

		boolean rolledBack = false;
		DatabaseConnection connection = null;
		try {
			connection = ro ? db.getReadOnlyConnection() : db.getReadWriteConnection();
			if (transaction) {
				connection.setAutoCommit(false);
			}
			try {
				return consumer.use(connection);
			} catch (Throwable t) {
				rolledBack = true;
				if (transaction) {
					connection.rollback(null);
				}
				throw t;
			} finally {
				if (transaction) {
					if (!rolledBack) {
						connection.commit(null);
					}
					connection.setAutoCommit(true);
				}
			}
		} finally {
			if (connection != null) {
				db.releaseConnection(connection);
			}
		}
	}
}
