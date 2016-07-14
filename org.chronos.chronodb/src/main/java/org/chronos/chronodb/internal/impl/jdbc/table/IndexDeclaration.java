package org.chronos.chronodb.internal.impl.jdbc.table;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;

public class IndexDeclaration {

	private final String indexName;

	private final List<String> columns;

	public IndexDeclaration(final String indexName, final String columnName) {
		this(indexName, columnName, new String[0]);
	}

	public IndexDeclaration(final String indexName, final String columnName, final String... remainingColumnNames) {
		this.indexName = indexName;
		List<String> columns = Lists.newArrayList();
		columns.add(columnName);
		if (remainingColumnNames != null) {
			for (String c : remainingColumnNames) {
				columns.add(c);
			}
		}
		this.columns = Collections.unmodifiableList(columns);
	}

	public String getIndexName() {
		return this.indexName;
	}

	public List<String> getColumns() {
		return this.columns;
	}

	public String getSQLCreateIndexCommand(final String tableName) {
		StringBuilder sql = new StringBuilder();
		sql.append("CREATE INDEX ");
		sql.append(this.indexName);
		sql.append(" ON ");
		sql.append(tableName);
		sql.append('(');
		String separator = "";
		for (String column : this.columns) {
			sql.append(separator);
			separator = ", ";
			sql.append(column);
		}
		sql.append(')');
		return sql.toString();
	}

}
