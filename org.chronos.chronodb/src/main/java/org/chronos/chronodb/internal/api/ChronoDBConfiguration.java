package org.chronos.chronodb.internal.api;

import java.io.File;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.DuplicateVersionEliminationMode;
import org.chronos.chronodb.api.conflict.ConflictResolutionStrategy;
import org.chronos.chronodb.api.exceptions.ChronoDBCommitConflictException;
import org.chronos.chronodb.internal.util.ChronosBackend;
import org.chronos.common.configuration.ChronosConfiguration;

/**
 * This class represents the configuration of a single {@link ChronoDB} instance.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface ChronoDBConfiguration extends ChronosConfiguration {

	// =====================================================================================================================
	// STATIC KEY NAMES
	// =====================================================================================================================

	/** The namespace which all settings in this configuration have in common. */
	public static final String NAMESPACE = "org.chronos.chronodb";

	/** A helper constant that combines the namespace and a trailing dot (.) character. */
	public static final String NS_DOT = NAMESPACE + '.';

	/**
	 * The debug setting is intended for internal use only.
	 *
	 * <p>
	 * Type: boolean<br>
	 * Default: false<br>
	 * Maps to: {@link #isDebugModeEnabled()}
	 */
	public static final String DEBUG = NS_DOT + "debug";

	/**
	 * The storage backend determines which kind of backend a {@link ChronoDB} is writing data to.
	 *
	 * <p>
	 * Depending on this setting, other configuration elements may become necessary.
	 *
	 * <p>
	 * Type: string<br>
	 * Values: all literals in {@link ChronosBackend} (in their string representation)<br>
	 * Default: none (mandatory setting)<br>
	 * Maps to: {@link #getBackendType()}
	 */
	public static final String STORAGE_BACKEND = NS_DOT + "storage.backend";

	/**
	 * Sets the maximum size of the cache managed by the storage backend (in bytes).
	 *
	 * <p>
	 * Not all storage backends support this property.
	 *
	 * <p>
	 * Type: long<br>
	 * Default: 209715200 bytes (200MB)<br>
	 * Maps to: {@link #getStorageBackendCacheMaxSize()}
	 */
	public static final String STORAGE_BACKEND_CACHE = NS_DOT + "storage.backend.cache.maxSize";

	/**
	 * Determines if regular entry caching is enabled or not.
	 * <p>
	 * This allows to enable/disable caching without touching the actual {@link #CACHE_MAX_SIZE}.
	 *
	 * <p>
	 * Type: boolean<br>
	 * Default: false<br>
	 * Maps to: {@link #isCachingEnabled()}
	 */
	public static final String CACHING_ENABLED = NS_DOT + "cache.enabled";

	/**
	 * The maximum number of elements in the entry cache.
	 *
	 * <p>
	 * Type: int<br>
	 * Default: none; needs to be assiged if {@link #CACHING_ENABLED} is set to <code>true</code>.<br>
	 * Maps to: {@link #getCacheMaxSize()}
	 */
	public static final String CACHE_MAX_SIZE = NS_DOT + "cache.maxSize";

	/**
	 * Determines if the query cache is enabled or not.
	 *
	 * <p>
	 * This allows to enable/disable the query cache without touching hte actual {@link #QUERY_CACHE_MAX_SIZE}.
	 *
	 * <p>
	 * Type: boolean<br>
	 * Default: false<br>
	 * Maps to: {@link #isIndexQueryCachingEnabled()}
	 */
	public static final String QUERY_CACHE_ENABLED = NS_DOT + "querycache.enabled";

	/**
	 * The maximum number of query results to cache.
	 *
	 * <p>
	 * Please note that the amount of RAM consumed by this cache is determined by the number of results per cached
	 * query. Very large datasets tend to produce larger query results, which lead to increased RAM consumption while
	 * staying at the same number of cached queries.
	 *
	 * <p>
	 * Type: integer<br>
	 * Default: 0<br>
	 * Maps to: {@link #getIndexQueryCacheMaxSize()}
	 */
	public static final String QUERY_CACHE_MAX_SIZE = NS_DOT + "querycache.maxsize";

	/**
	 * Enables or disables the assumption that user-provided values in the entry cache are immutable.
	 *
	 * <p>
	 * Setting this value to "true" will enhance the performance of the cache in terms of runtime, but may lead to data
	 * corruption if the values returned by {@link ChronoDBTransaction#get(String) tx.get(...)} or
	 * {@link ChronoDBTransaction#find() tx.find()} will be modified by application code. By default, this value is
	 * therefore set to "false".
	 *
	 * <p>
	 * Type: boolean<br>
	 * Default value: false<br>
	 * Maps to: {@link #isAssumeCachedValuesAreImmutable()}
	 */
	public static final String ASSUME_CACHE_VALUES_ARE_IMMUTABLE = NS_DOT + "cache.assumeValuesAreImmutable";

	/**
	 * Sets the {@link ConflictResolutionStrategy} to use in this database instance.
	 *
	 * <p>
	 * This setting can be overruled on a by-transaction-basis.
	 *
	 * <p>
	 * <b><u>/!\ WARNING /!\</u></b><br>
	 * The values of this setting are <b>case sensitive</b>!
	 *
	 * <p>
	 * Valid values for this setting are:
	 * <ul>
	 * <li><b>DO_NOT_RESOLVE</b>: Conflicts will not be resolved. Instead, a {@link ChronoDBCommitConflictException}
	 * will be thrown in case of conflicts.
	 * <li><b>OVERWRITE_WITH_SOURCE</b>: Conflicts are resolved by using the values provided by the transaction change
	 * set. Existing conflicting values in the target branch will be overwritten. This is similar to a "force push".
	 * <li><b>OVERWRITE_WITH_TARGET</b>: Conflicts are resolved by using the pre-existing values of conflicting keys
	 * provided by the target branch.
	 * <li><b>Custom Class Name</b>: Clients can provide their own implementation of the
	 * {@link ConflictResolutionStrategy} class and specify the fully qualified class name here. ChronoDB will
	 * instantiate this class (so it needs a visible default constructor) and use it for conflict resolution.
	 * </ul>
	 *
	 * <p>
	 * Type: String (fully qualified class name <i>or</i> one of the literals listed above)<br>
	 * Default value: DO_NOT_RESOLVE<br>
	 * Maps to: {@link #getConflictResolutionStrategy()}
	 */
	public static final String COMMIT_CONFLICT_RESOLUTION_STRATEGY = NS_DOT + "conflictresolver";

	/**
	 * Compaction mechanism that discards a version of a key-value pair on commit if the value is identical to the
	 * previous one.
	 *
	 * <p>
	 * Duplicate versions do no "harm" to the consistency of the database, but consume memory on disk and slow down
	 * searches without adding any information value in return.
	 *
	 * <p>
	 * Duplicate version elimination does come with a performance penalty on commit (not on read).
	 *
	 * <p>
	 * Type: string<br>
	 * Values: all literals of {@link DuplicateVersionEliminationMode} (in their string representation)<br>
	 * Default value: "onCommit"<br>
	 * Maps to: {@link #getDuplicateVersionEliminationMode()}
	 */
	public static final String DUPLICATE_VERSION_ELIMINATION_MODE = NS_DOT + "temporal.duplicateVersionEliminationMode";

	/**
	 * The working file, i.e. the file to which {@link ChronoDB} is writing in {@link ChronosBackend#MAPDB} mode.
	 *
	 * <p>
	 * Required when {@link #STORAGE_BACKEND} is set to {@link ChronosBackend#MAPDB}, otherwise this setting is ignored.
	 *
	 * <p>
	 * Sibling files and folders may also be created for secondary indexing.
	 *
	 * <p>
	 * Type: string<br>
	 * Values: any valid filepath in your file system<br>
	 * Default value: none (mandatory setting)<br>
	 * Maps to: {@link #getWorkingFile()}
	 */
	public static final String WORK_FILE = NS_DOT + "storage.file.work_directory";

	/**
	 * Controls if the database contents should be dropped upon database shutdown.
	 *
	 * <p>
	 * This is intended primarily for testing purposes and may be ignored by some backends.
	 *
	 * <p>
	 * Type: boolean<br>
	 * Default value: false<br>
	 * Maps to: {@link #isDropOnShutdown()}
	 */
	public static final String DROP_ON_SHUTDOWN = NS_DOT + "storage.file.drop_on_shutdown";

	/**
	 * Sets the JDBC database connection URL when {@link ChronoDB} is operating in {@link ChronosBackend#JDBC} mode.
	 *
	 * <p>
	 * Required when {@link #STORAGE_BACKEND} is set to {@link ChronosBackend#JDBC}, otherwise this setting is ignored.
	 *
	 * <p>
	 * Type: string<br>
	 * Values: any valid JDBC URL<br>
	 * Default value: none (mandatory setting)<br>
	 * Maps to: {@link #getJdbcConnectionUrl()}
	 */
	public static final String JDBC_CONNECTION_URL = NS_DOT + "storage.jdbc.connection_url";

	/**
	 * Sets the username in the JDBC credentials when {@link ChronoDB} is operating in {@link ChronosBackend#JDBC} mode.
	 *
	 * <p>
	 * Optional when {@link #STORAGE_BACKEND} is set to {@link ChronosBackend#JDBC}, otherwise this setting is ignored.
	 *
	 * <p>
	 * Type: string<br>
	 * Default value: none<br>
	 * Maps to: {@link #getJdbcCredentialsUsername()}
	 */
	public static final String JDBC_CREDENTIALS_USERNAME = NS_DOT + "storage.jdbc.credentials.username";

	/**
	 * Sets the password in the JDBC credentials when {@link ChronoDB} is operating in {@link ChronosBackend#JDBC} mode.
	 *
	 * <p>
	 * Optional when {@link #STORAGE_BACKEND} is set to {@link ChronosBackend#JDBC}, otherwise this setting is ignored.
	 *
	 * <p>
	 * Type: string<br>
	 * Default value: none<br>
	 * Maps to: {@link #getJdbcCredentialsPassword()}
	 */
	public static final String JDBC_CREDENTIALS_PASSWORD = NS_DOT + "storage.jdbc.credentials.password";

	// =================================================================================================================
	// GENERAL CONFIGURATION
	// =================================================================================================================

	/**
	 * Returns <code>true</code> if debug mode is enabled, otherwise <code>false</code>.
	 *
	 * <p>
	 * Mapped by setting: {@value #DEBUG}
	 *
	 * @return <code>true</code> if debug mode is enabled, otherwise <code>false</code>.
	 */
	public boolean isDebugModeEnabled();

	/**
	 * Returns the type of backend this {@link ChronoDB} instance is running on.
	 *
	 * <p>
	 * Mapped by setting: {@value #STORAGE_BACKEND}
	 *
	 * @return The backend type. Never <code>null</code>.
	 */
	public ChronosBackend getBackendType();

	/**
	 * Returns the maximum size of the cache maintained by the storage backend in bytes.
	 *
	 * <p>
	 * Not all backends support this property and may choose to ignore this option.
	 *
	 * @return The maximum size of the storage backend cache in bytes. Must not be negative.
	 */
	public long getStorageBackendCacheMaxSize();

	/**
	 * Returns <code>true</code> if caching is enabled in this {@link ChronoDB} instance.
	 *
	 * <p>
	 * Mapped by setting: {@value #CACHING_ENABLED}
	 *
	 * @return <code>true</code> if caching is enabled, otherwise <code>false</code>.
	 */
	public boolean isCachingEnabled();

	/**
	 * Returns the maximum number of elements that may reside in the cache.
	 *
	 * <p>
	 * Mapped by setting: {@value #CACHE_MAX_SIZE}
	 *
	 * @return The maximum number of elements allowed to reside in the cache (a number greater than zero) if caching is
	 *         enabled, otherwise <code>null</code> if caching is disabled.
	 */
	public Integer getCacheMaxSize();

	/**
	 * Returns <code>true</code> when cached values may be assumed to be immutable, otherwise <code>false</code>.
	 *
	 * <p>
	 * Mapped by setting: {@value #ASSUME_CACHE_VALUES_ARE_IMMUTABLE}
	 *
	 * @return <code>true</code> if it is safe to assume immutability of cached values, otherwise <code>false</code>.
	 */
	public boolean isAssumeCachedValuesAreImmutable();

	/**
	 * Checks if caching of index queries is allowed in this {@link ChronoDB} instance.
	 *
	 * <p>
	 * Mapped by setting: {@value #QUERY_CACHE_ENABLED}
	 *
	 * @return <code>true</code> if caching is enabled, otherwise <code>false</code>.
	 */
	public boolean isIndexQueryCachingEnabled();

	/**
	 * Returns the maximum number of index query results to cache.
	 *
	 * <p>
	 * This is only relevant if {@link #isIndexQueryCachingEnabled()} is set to <code>true</code>.
	 *
	 * <p>
	 * Mapped by setting: {@value #QUERY_CACHE_MAX_SIZE}
	 *
	 * @return The maximum number of index query results to cache. If index query caching is disabled, this method will
	 *         return <code>null</code>. Otherwise, this method will return an integer value greater than zero.
	 */
	public Integer getIndexQueryCacheMaxSize();

	/**
	 * Returns the conflict resolution strategy that will be applied in case of commit conflicts.
	 *
	 * <p>
	 * Please note that this is the <i>default</i> configuration for all transactions. This setting can be overwritten
	 * by each individual transaction.
	 *
	 * @return The conflict resolution strategy. Never <code>null</code>.
	 */
	public ConflictResolutionStrategy getConflictResolutionStrategy();

	/**
	 * Returns the {@link DuplicateVersionEliminationMode} used by this {@link ChronoDB} instance.
	 *
	 * <p>
	 * Mapped by setting: {@value #DUPLICATE_VERSION_ELIMINATION_MODE}
	 *
	 * @return The duplicate version elimination mode. Never <code>null</code>.
	 */
	public DuplicateVersionEliminationMode getDuplicateVersionEliminationMode();

	/**
	 * Checks if the database contents should be cleared upon database shutdown.
	 *
	 * <p>
	 * This is intended primarily for testing purposes and may be ignored by some backends.
	 *
	 * <p>
	 * Mapped by setting: {@value #DROP_ON_SHUTDOWN}
	 *
	 * @return <code>true</code> if the database contents should be cleared on shutdown, otherwise <code>false</code>.
	 */
	public boolean isDropOnShutdown();

	// =================================================================================================================
	// FILE BACKEND CONFIGURATION
	// =================================================================================================================

	/**
	 * Returns the working directory in which the persistence takes place in case of the file-based backend.
	 *
	 * <p>
	 * This is essentially the immediate parent directory which contains the {@link #getWorkingFile()}.
	 *
	 * @return The working directory. May be <code>null</code> if file backend is not used by this instance.
	 */
	public File getWorkingDirectory();

	/**
	 * Returns the actual file in which the persistence takes place in case of the file-based backend.
	 *
	 * <p>
	 * Mapped by setting: {@value #WORK_FILE}
	 *
	 * @return The backend file receiving the database contents. May be <code>null</code> if file backend is not used by
	 *         this instance.
	 */
	public File getWorkingFile();

	// =================================================================================================================
	// JDBC CONFIGURATION
	// =================================================================================================================

	/**
	 * Returns the JDBC Connection URL to which this {@link ChronoDB} instance is connected.
	 *
	 * <p>
	 * Mapped by setting: {@value #JDBC_CONNECTION_URL}
	 *
	 * @return The connection URL, or <code>null</code> if this ChronoDB instance is not connected via JDBC.
	 */
	public String getJdbcConnectionUrl();

	/**
	 * Returns the JDBC Username to which this {@link ChronoDB} instance is connected.
	 *
	 * <p>
	 * Mapped by setting: {@value #JDBC_CREDENTIALS_USERNAME}
	 *
	 * @return The username, or <code>null</code> if this ChronoDB instance is not connected via JDBC.
	 */
	public String getJdbcCredentialsUsername();

	/**
	 * Returns the JDBC Password to which this {@link ChronoDB} instance is connected.
	 *
	 * <p>
	 * Mapped by setting: {@value #JDBC_CREDENTIALS_PASSWORD}
	 *
	 * @return The password, or <code>null</code> if this ChronoDB instance is not connected via JDBC.
	 */
	public String getJdbcCredentialsPassword();

}
