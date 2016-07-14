package org.chronos.chronodb.internal.impl.jdbc.table;

import static com.google.common.base.Preconditions.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;

import org.chronos.chronodb.api.exceptions.JdbcTableException;
import org.chronos.chronodb.internal.impl.jdbc.util.JdbcUtils;
import org.chronos.common.logging.ChronoLogger;

public abstract class AbstractJdbcTable implements JdbcTable {

	protected final Connection connection;

	protected AbstractJdbcTable(final Connection connection) {
		checkNotNull(connection, "Precondition violation - argument 'connection' must not be NULL!");
		checkArgument(JdbcUtils.isConnectionOpen(connection),
				"Precondition violation - argument 'connection' must refer to an OPEN connection!");
		this.connection = connection;
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	@Override
	public void create() {
		try {
			Statement stmt = this.connection.createStatement();
			String sqlCreateTable = this.getCreateTableStatement();
			ChronoLogger.logTrace("[DDL] " + sqlCreateTable);
			stmt.executeUpdate(sqlCreateTable);
			Set<String> createIndexStatements = this.getCreateIndexStatements();
			if (createIndexStatements != null) {
				for (String createIndexStatement : this.getCreateIndexStatements()) {
					ChronoLogger.logTrace("[DDL] " + createIndexStatement);
					stmt.executeUpdate(createIndexStatement);
				}
			}
		} catch (SQLException e) {
			throw new JdbcTableException("Failed to create Table '" + this.getName() + "'!", e);
		}
	}

	@Override
	public boolean exists() {
		try {
			return JdbcUtils.tableExists(this.connection, this.getName());
		} catch (SQLException e) {
			throw new JdbcTableException("Failed to check existence of table '" + this.getName() + "'!", e);
		}
	}

	@Override
	public void ensureExists() {
		if (this.exists() == false) {
			this.create();
		}
	}

	@Override
	public void drop() {
		try {
			JdbcUtils.dropTableIfExists(this.connection, this.getName());
		} catch (SQLException e) {
			throw new JdbcTableException("Failed to drop table '" + this.getName() + "'!", e);
		}
	}

	// =================================================================================================================
	// ABSTRACT METHOD DECLARATIONS
	// =================================================================================================================

	protected abstract String getName();

	protected abstract String getCreateTableStatement();

	protected abstract Set<String> getCreateIndexStatements();

}
