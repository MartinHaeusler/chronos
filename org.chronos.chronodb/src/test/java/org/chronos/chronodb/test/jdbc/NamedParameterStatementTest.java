package org.chronos.chronodb.test.jdbc;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import javax.sql.DataSource;

import org.chronos.chronodb.internal.impl.jdbc.util.NamedParameterStatement;
import org.chronos.common.test.ChronosUnitTest;
import org.chronos.common.test.junit.categories.UnitTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.mchange.v2.c3p0.DataSources;

@Category(UnitTest.class)
public class NamedParameterStatementTest extends ChronosUnitTest {

	private static final String TABLE_DEFINITION = "CREATE TABLE Test ( ID VARCHAR(255), MapTime INT, MapKey VARCHAR(255), MapVal VARCHAR(255), PRIMARY KEY (ID) )";

	private DataSource dataSource;
	private Connection connection;

	@Before
	public void setupDataSource() throws SQLException {
		String name = UUID.randomUUID().toString().replace("-", "");
		String dbUrl = "jdbc:h2:mem:" + name;
		this.dataSource = DataSources.unpooledDataSource(dbUrl);
		this.connection = this.dataSource.getConnection();
		try (Statement stmt = this.connection.createStatement()) {
			stmt.executeUpdate(TABLE_DEFINITION);
			stmt.executeUpdate("INSERT INTO Test VALUES ('123', 40, 'Hello', 'World')");
			stmt.executeUpdate("INSERT INTO Test VALUES ('124', 60, 'Hello', 'Foo')");
			stmt.executeUpdate("INSERT INTO Test VALUES ('125', 100, 'Hello', 'Bar')");
			stmt.executeUpdate("INSERT INTO Test VALUES ('126', 40, 'Name', 'Martin')");
			stmt.executeUpdate("INSERT INTO Test VALUES ('127', 60, 'Name', 'John')");
			stmt.executeUpdate("INSERT INTO Test VALUES ('128', 100, 'Name', 'Jack')");
		}
		this.connection.commit();
	}

	@After
	public void tearDownDataSource() throws SQLException {
		DataSources.destroy(this.dataSource);
	}

	@Test
	public void namedParameterStatementCreationWorks() throws SQLException {
		try (Connection connection = this.dataSource.getConnection()) {
			String sql = "SELECT * FROM Test WHERE MapTime = ${time}";
			NamedParameterStatement nStmt = new NamedParameterStatement(connection, sql);
			assertNotNull(nStmt);
		}
	}

	@Test
	public void simpleReplacementWorks() throws SQLException {
		String sql = "SELECT * FROM Test WHERE MapTime = ${time}";
		try (NamedParameterStatement nStmt = new NamedParameterStatement(this.connection, sql)) {
			nStmt.setParameter("time", 100);
			try (ResultSet resultSet = nStmt.executeQuery()) {
				assertTrue(resultSet.next());
				String key1 = resultSet.getString("MapKey");
				if (key1.equals("Name") == false && key1.equals("Hello") == false) {
					fail("Query returned unexpected result: '" + key1 + "'!");
				}
				assertTrue(resultSet.next());
				String key2 = resultSet.getString("MapKey");
				if (key2.equals("Name") == false && key2.equals("Hello") == false) {
					fail("Query returned unexpected result: '" + key2 + "'!");
				}
				assertFalse(resultSet.next());
			}
		}
	}

	@Test
	public void replaceMultipleWorks() throws SQLException {
		String sql = "SELECT tFloor.mapval AS MapVal, tFloor.maptime AS FloorTime, tCeil.maptime AS CeilTime FROM Test tFloor, "
				+ "( SELECT * FROM Test tTemp2 WHERE tTemp2.mapkey = ${name} AND tTemp2.maptime = ( "
				+ "SELECT MIN(tTemp3.maptime) FROM Test tTemp3 WHERE tTemp3.mapkey = ${name} AND tTemp3.maptime > ${time} ) "
				+ ") AS tCeil WHERE tFloor.mapkey = ${name} AND tFloor.maptime = ( "
				+ "SELECT MAX(tTemp1.maptime) FROM Test tTemp1 WHERE tTemp1.mapkey = ${name} AND tTemp1.maptime <= ${time} )";
		try (NamedParameterStatement nStmt = new NamedParameterStatement(this.connection, sql)) {
			nStmt.setParameter("name", "Hello");
			nStmt.setParameter("time", 50);
			try (ResultSet resultSet = nStmt.executeQuery()) {
				assertTrue(resultSet.next());
				assertEquals("World", resultSet.getString("MapVal"));
				assertEquals(40, resultSet.getLong("FloorTime"));
				assertEquals(60, resultSet.getLong("CeilTime"));
				// there should be only one match
				assertFalse(resultSet.next());
			}
		}
	}

}
