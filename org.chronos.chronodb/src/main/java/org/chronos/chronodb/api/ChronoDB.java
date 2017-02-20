package org.chronos.chronodb.api;

import java.io.File;

import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.api.cache.ChronoDBCache;

/**
 * The top-level interface for interaction with a {@link ChronoDB} instance.
 *
 * <p>
 * This interface represents the entire database instance. Its primary purpose is to spawn instances of
 * {@link ChronoDBTransaction}. Instances of this interface represent (and abstract from) an active connection to a
 * backing datastore.
 *
 * <p>
 * You can acquire an instance of this class by using the static {@link #FACTORY} variable, like so:
 *
 * <pre>
 *    ChronoDB db = ChronoDB.FACTORY... ;
 * </pre>
 *
 * <p>
 * Note that this interface extends {@link AutoCloseable}. The {@link #close()} method is used to close this database.
 * As a database instance may hold resources that do not get released automatically upon garbage collection, <b>calling
 * {@link #close()} is mandatory</b>.
 *
 * <p>
 * Instances of this class are guaranteed to be thread-safe and may be shared freely among threads.
 *
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface ChronoDB extends TransactionSource, AutoCloseable {

	/**
	 * Provides access to the factory for this class.
	 */
	public static final ChronoDBFactory FACTORY = ChronoDBFactory.INSTANCE;

	/**
	 * Returns the configuration of this {@link ChronoDB} instance.
	 *
	 * @return The configuration. Never <code>null</code>.
	 */
	public ChronoDBConfiguration getConfiguration();

	/**
	 * Returns the branch manager associated with this database instance.
	 *
	 * @return The branch manager. Never <code>null</code>.
	 */
	public BranchManager getBranchManager();

	/**
	 * Returns the index manager for this {@link ChronoDB} instance.
	 *
	 * @return The index manager. Never <code>null</code>.
	 */
	public IndexManager getIndexManager();

	/**
	 * Returns the serialization manager associated with this database instance.
	 *
	 * @return The serialization manager. Never <code>null</code>.
	 */
	public SerializationManager getSerializationManager();

	/**
	 * Returns the maintenance manager associated with this database instance.
	 *
	 * @return The maintenance manager. Never <code>null</code>.
	 */
	public MaintenanceManager getMaintenanceManager();

	/**
	 * Returns the cache of this database instance.
	 *
	 * @return The cache. Never <code>null</code>. May be a bogus instance if caching is disabled.
	 */
	public ChronoDBCache getCache();

	/**
	 * Creates a database dump of the entire current database state.
	 *
	 * <p>
	 * Please note that the database is not available for write operations while the dump process is running. Read
	 * operations will work concurrently as usual.
	 *
	 * <p>
	 * <b>WARNING:</b> The file created by this process may be very large (several gigabytes), depending on the size of
	 * the database. It is the responsibility of the user of this API to ensure that enough disk space is available;
	 * this method does not perform any checks regarding disk space availability!
	 *
	 * <p>
	 * <b>WARNING:</b> The given file will be <b>overwritten</b> without further notice!
	 *
	 * @param dumpFile
	 *            The file to store the dump into. Must not be <code>null</code>. Must point to a file (not a
	 *            directory). The standard file extension <code>*.chronodump</code> is recommmended, but not required.
	 *            If the file does not exist, the file (and any missing parent directory) will be created.
	 * @param dumpOptions
	 *            The options to use for this dump (optional). Please refer to the documentation of the invididual
	 *            constants for details.
	 */
	public void writeDump(File dumpFile, DumpOption... dumpOptions);

	/**
	 * Reads the contents of the given dump file into this database.
	 *
	 * <p>
	 * This is a management operation; it completely locks the database. No concurrent writes or reads will be permitted
	 * while this operation is being executed.
	 *
	 * <p>
	 * <b>WARNING:</b> The current contents of the database will be <b>merged</b> with the contents of the dump! In case
	 * of conflicts, the data stored in the dump file will take precedence. It is <i>strongly recommended</i> to perform
	 * this operation only on an <b>empty</b> database!
	 *
	 * <p>
	 * <b>WARNING:</b> As this is a management operation, there is no rollback or undo option!
	 *
	 * @param dumpFile
	 *            The dump file to read the data from. Must not be <code>null</code>, must exist, and must point to a
	 *            file (not a directory).
	 * @param options
	 *            The options to use while reading (optional). May be empty.
	 */
	public void readDump(File dumpFile, DumpOption... options);

	/**
	 * Closes this instance of ChronoDB.
	 *
	 * <p>
	 * This completely shuts down the database. Any further calls to retrieve or store data will fail.
	 *
	 * <p>
	 * Users are responsible for calling this method when the interaction with the database stops in order to allow
	 * ChronoDB to shutdown gracefully.
	 */
	@Override
	public void close();

	/**
	 * Checks if this instance of ChronoDB has already been closed or not.
	 *
	 * <p>
	 * This method is safe to call even after {@link #close()} has been called.
	 *
	 * @return <code>true</code> if this ChronoDB instance is closed, otherwise <code>false</code>.
	 */
	public boolean isClosed();

}
