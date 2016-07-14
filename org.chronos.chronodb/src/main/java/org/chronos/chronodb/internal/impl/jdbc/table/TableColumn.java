package org.chronos.chronodb.internal.impl.jdbc.table;

import static com.google.common.base.Preconditions.*;

public class TableColumn {

	private final String name;
	private final String typebound;

	public TableColumn(final String name, final String typebound) {
		checkNotNull(name, "Precondition violation - argument 'name' must not be NULL!");
		checkNotNull(typebound, "Precondition violation - argument 'typebound' must not be NULL!");
		this.name = name;
		this.typebound = typebound;
	}

	public String getName() {
		return this.name;
	}

	public String getSQLTypebound() {
		return this.typebound;
	}

	public String getSQLColumnDefinition() {
		StringBuilder builder = new StringBuilder();
		builder.append(this.name);
		builder.append(" ");
		builder.append(this.typebound);
		return builder.toString();
	}

}
