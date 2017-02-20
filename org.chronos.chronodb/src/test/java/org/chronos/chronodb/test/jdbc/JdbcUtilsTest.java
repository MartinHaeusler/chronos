package org.chronos.chronodb.test.jdbc;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.chronos.chronodb.internal.impl.jdbc.util.JdbcDataSourceUtil;
import org.chronos.chronodb.internal.impl.jdbc.util.JdbcUtils;
import org.chronos.common.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.sql.DataSource;

@Category(UnitTest.class)
public class JdbcUtilsTest {

	@Test
	public void testCreateTableRendering() {
		String createTableStmt = JdbcUtils.renderCreateTableStatement("Test", "ID", "VARCHAR(255) NOT NULL", "Name",
				"VARCHAR(255)", "Age", "INTEGER");
		createTableStmt = createTableStmt.replace("\t", "").replace("\n", "");
		String expected = "CREATE TABLE Test (ID VARCHAR(255) NOT NULL, Name VARCHAR(255), Age INTEGER, PRIMARY KEY ( ID ))";
		// System.out.println("Expected: " + expected);
		// System.out.println("Actual: " + createTableStmt);

		assertEquals(expected, createTableStmt);

		DataSource dataSource = JdbcDataSourceUtil.getH2InMemoryDataSource("Test");
		try (Connection connection = dataSource.getConnection(); Statement stmt = connection.createStatement()) {
			stmt.executeUpdate(createTableStmt);
			assertTrue(JdbcUtils.tableExists(connection, "Test"));
		} catch (SQLException e) {
			fail("SQL Exception: " + e.getMessage());
		}
	}
}
