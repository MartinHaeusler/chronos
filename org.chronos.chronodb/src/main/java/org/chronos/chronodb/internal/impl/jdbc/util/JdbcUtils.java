package org.chronos.chronodb.internal.impl.jdbc.util;

import static com.google.common.base.Preconditions.*;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.chronos.chronodb.internal.impl.jdbc.table.TableColumn;

import com.google.common.collect.Lists;

/**
 * This class contains commonly used JDBC methods.
 *
 * <p>
 * This class is thread-safe. All methods work exclusively on the provided objects.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public class JdbcUtils {

	/**
	 * Drops the table with the given name in the database, if it exists.
	 *
	 * <p>
	 * If the table does not exist in the database, this method does nothing.
	 *
	 * @param connection
	 *            The connection to use. Must not be <code>null</code>. Must not be closed.
	 * @param tableName
	 *            The name of the table to drop. Must not be <code>null</code>.
	 *
	 * @return <code>true</code> if the table was dropped successfully, or <code>false</code> if it did not exist.
	 *
	 * @throws SQLException
	 *             Thrown if a database error occurs during the operation.
	 */
	public static boolean dropTableIfExists(final Connection connection, final String tableName) throws SQLException {
		checkNotNull(connection, "Precondition violation - argument 'connection' must not be NULL!");
		checkNotNull(tableName, "Precondition violation - argument 'tableName' must not be NULL!");
		checkArgument(connection.isClosed() == false, "Precondition violation - connection must not be closed!");
		if (tableExists(connection, tableName) == false) {
			// table did not exist in the first place
			return false;
		}
		try (Statement statement = connection.createStatement()) {
			statement.executeUpdate("DROP TABLE " + tableName);
			statement.close();
			// table removed successfully
			return true;
		}
	}

	/**
	 * Checks if a table with the given name exists in the database.
	 *
	 * @param connection
	 *            The connection to use. Must not be <code>null</code>. Must not be closed.
	 * @param tableName
	 *            The name of the table to search for. Must not be <code>null</code>.
	 *
	 * @return <code>true</code> if the database contains a table with the given name, otherwise <code>false</code>.
	 *
	 * @throws SQLException
	 *             Thrown if a database error occurs during the operation.
	 */
	public static boolean tableExists(final Connection connection, final String tableName) throws SQLException {
		checkNotNull(connection, "Precondition violation - argument 'connection' must not be NULL!");
		checkNotNull(tableName, "Precondition violation - argument 'tableName' must not be NULL!");
		checkArgument(connection.isClosed() == false, "Precondition violation - connection must not be closed!");
		DatabaseMetaData metaData = connection.getMetaData();
		boolean foundIt = false;
		// first try: capitalize the name and try to find the table that way
		// (most SQL implementations have upper-cased table names)
		try (ResultSet tableMetadata = metaData.getTables(null, null, tableName.toUpperCase(), null)) {
			if (tableMetadata.next()) {
				// the table exists
				foundIt = true;
			} else {
				// table does not exist
				foundIt = false;
			}
			if (foundIt) {
				return true;
			}
			// try again, with the original name (no capitalization)
			try (ResultSet tableMetadata2 = metaData.getTables(null, null, tableName, null)) {
				if (tableMetadata.next()) {
					// the table exists
					foundIt = true;
				} else {
					// table does not exist
					foundIt = false;
				}
			}
		}
		return foundIt;
	}

	/**
	 * Returns the names of all tables in the given database.
	 *
	 * @param connection
	 *            The connection to use. Must not be <code>null</code>. Must not be closed.
	 *
	 * @return The list of table names in the given database. May be empty, but never <code>null</code>.
	 *
	 * @throws SQLException
	 *             Thrown if a database error occurs during the operation.
	 */
	public static List<String> getTableNames(final Connection connection) throws SQLException {
		checkNotNull(connection, "Precondition violation - argument 'connection' must not be NULL!");
		DatabaseMetaData metaData = connection.getMetaData();
		ResultSet tableMetadata = metaData.getTables(null, null, null, new String[] { "TABLE" });
		List<String> resultList = Lists.newArrayList();
		while (tableMetadata.next()) {
			resultList.add(tableMetadata.getString("TABLE_NAME"));
		}
		tableMetadata.close();
		return resultList;
	}

	public static String renderCreateTableStatement(final String tableName, final String pk, final String pkType,
			final String... columnsAndTypes) {
		checkNotNull(tableName, "Precondition violation - argument 'tableName' must not be NULL!");
		checkNotNull(pk, "Precondition violation - argument 'pk' must not be NULL!");
		checkNotNull(pkType, "Precondition violation - argument 'pkType' must not be NULL!");
		if (columnsAndTypes != null && columnsAndTypes.length > 0) {
			checkArgument(columnsAndTypes.length % 2 == 0,
					"Precondition violation - argument 'columnsAndTypes' must have even length (is: "
							+ columnsAndTypes.length + ")!");
		}
		StringBuilder sql = new StringBuilder();
		sql.append("CREATE TABLE ");
		sql.append(tableName);
		sql.append(" (\n");
		sql.append("\t\t");
		sql.append(pk);
		sql.append(" ");
		sql.append(pkType);
		sql.append(", \n");
		int columnIndex = 0;
		if (columnsAndTypes != null && columnsAndTypes.length > 0) {
			while (columnIndex < columnsAndTypes.length) {
				String columnName = columnsAndTypes[columnIndex];
				String columnType = columnsAndTypes[columnIndex + 1];
				sql.append("\t\t");
				sql.append(columnName);
				sql.append(" ");
				sql.append(columnType);
				sql.append(", \n");
				columnIndex += 2;
			}
		}
		sql.append("\tPRIMARY KEY ( ");
		sql.append(pk);
		sql.append(" )\n");
		sql.append(")\n");
		return sql.toString();
	}

	public static String renderCreateTableStatement(final String name, final TableColumn[] columns) {
		checkNotNull(name, "Precondition violation - argument 'name' must not be NULL!");
		checkNotNull(columns, "Precondition violation - argument 'columns' must not be NULL!");
		checkArgument(columns.length > 0, "Precondition violation - argument 'columns' must have at least one entry!");
		String primaryKeyName = columns[0].getName();
		String primaryKeyTypebound = columns[0].getSQLTypebound();
		String[] remainingColumnsAndTypeBounds = new String[(columns.length - 1) * 2];
		int arrayIndex = 0;
		for (int i = 1; i < columns.length; i++) {
			remainingColumnsAndTypeBounds[arrayIndex] = columns[i].getName();
			remainingColumnsAndTypeBounds[arrayIndex + 1] = columns[i].getSQLTypebound();
			arrayIndex += 2;
		}
		return renderCreateTableStatement(name, primaryKeyName, primaryKeyTypebound, remainingColumnsAndTypeBounds);
	}

	/**
	 * Resolves the given prepared statement by replacing the placeholders with the given arguments.
	 *
	 * <p>
	 * <b>WARNING:</b><br>
	 * This method is intended primarily for <b>debugging purposes</b> and does not perform any kind of sanity checking.
	 * It is <b>strongly discouraged</b> to execute the result of this operation on an SQL server!
	 *
	 * @param preparedStatement
	 *            The prepared statement to resolve into a full statement. Must not be <code>null</code>.
	 * @param args
	 *            The arguments to use as replacement for the placeholders in the prepared statement. The number of
	 *            arguments must be equal to the number of placeholders in the prepared statement.
	 * @return The resolved SQL statement as a string. Never <code>null</code>.
	 */
	public static String resolvePreparedStatement(final String preparedStatement, final Object... args) {
		checkNotNull(preparedStatement, "Precondition violation - argument 'preparedStatement' must not be NULL!");
		String stmt = preparedStatement;
		for (Object argument : args) {
			if (argument instanceof String) {
				stmt = stmt.replaceFirst("\\?", "'" + argument + "'");
			} else if (argument instanceof byte[]) {
				byte[] bytes = (byte[]) argument;
				stmt = stmt.replaceFirst("\\?", "BLOB{" + bytes.length + "}");
			} else if (argument == null) {
				stmt = stmt.replaceFirst("\\?", "NULL");
			} else {
				stmt = stmt.replaceFirst("\\?", String.valueOf(argument));
			}
		}
		return stmt;
	}

	public static boolean isConnectionOpen(final Connection connection) {
		checkNotNull(connection, "Precondition violation - argument 'connection' must not be NULL!");
		try {
			if (connection.isClosed()) {
				return false;
			} else {
				return true;
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}
}
