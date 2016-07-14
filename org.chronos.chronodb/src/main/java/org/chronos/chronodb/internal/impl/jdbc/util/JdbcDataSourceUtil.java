package org.chronos.chronodb.internal.impl.jdbc.util;

import static com.google.common.base.Preconditions.*;

import java.io.File;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcConnectionPool;

/**
 * This is a utility class that allows for the creation of a wide range of {@link DataSource} instances.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public class JdbcDataSourceUtil {

	/**
	 * Returns a {@link DataSource} that connects to an in-memory H2 Database.
	 *
	 * @param dbName
	 *            The name of the database to connect to. Must not be <code>null</code>.
	 * @return The DataSource. Never <code>null</code>.
	 */
	public static DataSource getH2InMemoryDataSource(final String dbName) {
		String url = "jdbc:h2:mem:" + dbName + ";MVCC=true";
		JdbcConnectionPool cp = JdbcConnectionPool.create(url, "sa", "sa");
		return cp;
	}

	/**
	 * Returns a {@link DataSource} that connects to a H2 Database that stores its files in the given directory.
	 *
	 * @param filepath
	 *            The file path to use. Must point to a valid, existing directory. Must not be <code>null</code>.
	 * @return The DataSource. Never <code>null</code>.
	 */
	public static DataSource getH2FileDataSource(final String filepath) {
		checkNotNull(filepath, "Precondition violation - argument 'filepath' must not be NULL!");
		File file = new File(filepath);
		if (file.isDirectory() == false || file.exists() == false) {
			throw new IllegalArgumentException(
					"The given filepath does not point to an existing directory: '" + filepath + "'!");
		}
		String url = "jdbc:h2:" + filepath + ";MVCC=true";
		JdbcConnectionPool cp = JdbcConnectionPool.create(url, "sa", "sa");
		return cp;
	}

}
