package dev.ornamental.util.sql;

import java.sql.SQLException;

import com.j256.ormlite.support.DatabaseConnection;

@FunctionalInterface
public interface ConnectionConsumer<Q> {

	Q use(DatabaseConnection connection) throws SQLException;
}
