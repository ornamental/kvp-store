package dev.ornamental.util.sql;

import java.sql.SQLException;

import com.j256.ormlite.support.DatabaseResults;

@FunctionalInterface
public interface RowReader<Q> {

	Q readCurrentItem(DatabaseResults results) throws SQLException;
}
