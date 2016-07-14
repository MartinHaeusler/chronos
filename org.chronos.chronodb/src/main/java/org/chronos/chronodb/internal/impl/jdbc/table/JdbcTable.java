package org.chronos.chronodb.internal.impl.jdbc.table;

public interface JdbcTable {

	public void create();

	public boolean exists();

	public void ensureExists();

	public void drop();

}
