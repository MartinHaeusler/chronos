package org.chronos.chronodb.internal.impl.jdbc.table;

import static com.google.common.base.Preconditions.*;

import java.sql.Connection;
import java.util.Set;

import org.chronos.chronodb.internal.impl.jdbc.util.JdbcUtils;

import com.google.common.collect.Sets;

public abstract class DefaultJdbcTable extends AbstractJdbcTable {

	protected DefaultJdbcTable(final Connection connection) {
		super(connection);
	}

	// =================================================================================================================
	// API IMPLEMENTATION
	// =================================================================================================================

	@Override
	protected String getCreateTableStatement() {
		return JdbcUtils.renderCreateTableStatement(this.getName(), this.getColumns());
	}

	@Override
	protected Set<String> getCreateIndexStatements() {
		Set<String> resultSet = Sets.newHashSet();
		IndexDeclaration[] indexDeclarations = this.getIndexDeclarations();
		if (indexDeclarations != null) {
			for (IndexDeclaration indexDeclaration : this.getIndexDeclarations()) {
				resultSet.add(indexDeclaration.getSQLCreateIndexCommand(this.getName()));
			}
		}
		return resultSet;
	}

	public TableColumn getColumn(final String columnName) {
		checkNotNull(columnName, "Precondition violation - argument 'columnName' must not be NULL!");
		for (TableColumn column : this.getColumns()) {
			if (columnName.equals(column.getName())) {
				return column;
			}
		}
		return null;
	}

	// =================================================================================================================
	// ABSTRACT METHODS
	// =================================================================================================================

	protected abstract TableColumn[] getColumns();

	protected abstract IndexDeclaration[] getIndexDeclarations();

}
