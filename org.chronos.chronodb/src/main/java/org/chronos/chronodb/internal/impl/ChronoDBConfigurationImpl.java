package org.chronos.chronodb.internal.impl;

import java.io.File;

import org.chronos.chronodb.api.DuplicateVersionEliminationMode;
import org.chronos.chronodb.api.conflict.ConflictResolutionStrategy;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.util.ChronosBackend;
import org.chronos.common.configuration.AbstractConfiguration;
import org.chronos.common.configuration.Comparison;
import org.chronos.common.configuration.ParameterValueConverters;
import org.chronos.common.configuration.annotation.EnumFactoryMethod;
import org.chronos.common.configuration.annotation.IgnoredIf;
import org.chronos.common.configuration.annotation.Namespace;
import org.chronos.common.configuration.annotation.Parameter;
import org.chronos.common.configuration.annotation.RequiredIf;
import org.chronos.common.configuration.annotation.ValueConverter;

@Namespace(ChronoDBConfiguration.NAMESPACE)
public class ChronoDBConfigurationImpl extends AbstractConfiguration implements ChronoDBConfiguration {

	// =====================================================================================================================
	// DEFAULT SETTINGS
	// =====================================================================================================================

	private static final long DEFAULT__STORAGE_BACKEND_CACHE = 1024L * 1024L * 200L; // 200 MB (in bytes)

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	// general settings

	@Parameter(key = DEBUG)
	private boolean debugModeEnabled = false;

	@EnumFactoryMethod("fromString")
	@Parameter(key = STORAGE_BACKEND)
	private ChronosBackend backendType;

	@Parameter(key = STORAGE_BACKEND_CACHE)
	@IgnoredIf(field = "backendType", comparison = Comparison.IS_SET_TO, compareValue = "JDBC")
	@IgnoredIf(field = "backendType", comparison = Comparison.IS_SET_TO, compareValue = "FILE")
	@IgnoredIf(field = "backendType", comparison = Comparison.IS_SET_TO, compareValue = "INMEMORY")
	private long storageBackendCacheMaxSize = DEFAULT__STORAGE_BACKEND_CACHE;

	@Parameter(key = CACHING_ENABLED)
	private boolean cachingEnabled = false;

	@Parameter(key = CACHE_MAX_SIZE)
	@RequiredIf(field = "cachingEnabled", comparison = Comparison.IS_SET_TO, compareValue = "true")
	private Integer cacheMaxSize;

	@Parameter(key = QUERY_CACHE_ENABLED)
	private boolean indexQueryCachingEnabled = false;

	@Parameter(key = QUERY_CACHE_MAX_SIZE)
	@RequiredIf(field = "indexQueryCachingEnabled", comparison = Comparison.IS_SET_TO, compareValue = "true")
	private Integer indexQueryCacheMaxSize;

	@Parameter(key = ASSUME_CACHE_VALUES_ARE_IMMUTABLE)
	@RequiredIf(field = "cachingEnabled", comparison = Comparison.IS_SET_TO, compareValue = "true")
	private boolean assumeCachedValuesAreImmutable = false;

	@Parameter(key = COMMIT_CONFLICT_RESOLUTION_STRATEGY, optional = true)
	private String conflictResolutionStrategyName;

	@EnumFactoryMethod("fromString")
	@Parameter(key = DUPLICATE_VERSION_ELIMINATION_MODE, optional = true)
	private DuplicateVersionEliminationMode duplicateVersionEliminationMode = DuplicateVersionEliminationMode.ON_COMMIT;

	// file backend settings
	@Parameter(key = WORK_FILE)
	@RequiredIf(field = "backendType", comparison = Comparison.IS_SET_TO, compareValue = "file")
	@RequiredIf(field = "backendType", comparison = Comparison.IS_SET_TO, compareValue = "metadb")
	@ValueConverter(ParameterValueConverters.StringToFileConverter.class)
	private File workingFile;

	@Parameter(key = DROP_ON_SHUTDOWN)
	@RequiredIf(field = "backendType", comparison = Comparison.IS_SET_TO, compareValue = "file")
	@IgnoredIf(field = "backendType", comparison = Comparison.IS_NOT_SET_TO, compareValue = "file")
	private boolean dropOnShutdown = false;

	// jdbc backend settings
	@Parameter(key = JDBC_CONNECTION_URL)
	@RequiredIf(field = "backendType", comparison = Comparison.IS_SET_TO, compareValue = "jdbc")
	@IgnoredIf(field = "backendType", comparison = Comparison.IS_NOT_SET_TO, compareValue = "jdbc")
	private String jdbcConnectionUrl;

	@Parameter(key = JDBC_CREDENTIALS_USERNAME, optional = true)
	@IgnoredIf(field = "backendType", comparison = Comparison.IS_NOT_SET_TO, compareValue = "jdbc")
	@IgnoredIf(field = "backendType", comparison = Comparison.IS_NOT_SET_TO, compareValue = "jdbc")
	private String jdbcCredentialsUsername;

	@Parameter(key = JDBC_CREDENTIALS_PASSWORD, optional = true)
	@IgnoredIf(field = "backendType", comparison = Comparison.IS_NOT_SET_TO, compareValue = "jdbc")
	private String jdbcCredentialsPassword;

	// =================================================================================================================
	// CACHES
	// =================================================================================================================

	private transient ConflictResolutionStrategy conflictResolutionStrategy;

	// =================================================================================================================
	// GENERAL SETTINGS
	// =================================================================================================================

	@Override
	public boolean isDebugModeEnabled() {
		return this.debugModeEnabled;
	}

	@Override
	public ChronosBackend getBackendType() {
		return this.backendType;
	}

	@Override
	public long getStorageBackendCacheMaxSize() {
		return this.storageBackendCacheMaxSize;
	}

	@Override
	public boolean isCachingEnabled() {
		return this.cachingEnabled;
	}

	@Override
	public Integer getCacheMaxSize() {
		return this.cacheMaxSize;
	}

	@Override
	public boolean isIndexQueryCachingEnabled() {
		return this.indexQueryCachingEnabled;
	}

	@Override
	public Integer getIndexQueryCacheMaxSize() {
		return this.indexQueryCacheMaxSize;
	}

	@Override
	public boolean isAssumeCachedValuesAreImmutable() {
		return this.assumeCachedValuesAreImmutable;
	}

	@Override
	public ConflictResolutionStrategy getConflictResolutionStrategy() {
		if (this.conflictResolutionStrategy == null) {
			// setting was not yet resolved, do it now
			this.conflictResolutionStrategy = ConflictResolutionStrategyLoader
					.load(this.conflictResolutionStrategyName);
			// we already resolved this setting, use the cached instance
		}
		return this.conflictResolutionStrategy;
	}

	@Override
	public DuplicateVersionEliminationMode getDuplicateVersionEliminationMode() {
		return this.duplicateVersionEliminationMode;
	}

	// =================================================================================================================
	// FILE BACKEND SETTINGS
	// =================================================================================================================

	@Override
	public boolean isDropOnShutdown() {
		return this.dropOnShutdown;
	}

	@Override
	public File getWorkingFile() {
		return this.workingFile;
	}

	@Override
	public File getWorkingDirectory() {
		if (this.getWorkingFile() == null) {
			return null;
		}
		return this.getWorkingFile().getParentFile();
	}

	// =================================================================================================================
	// JDBC BACKEND SETTINGS
	// =================================================================================================================

	@Override
	public String getJdbcConnectionUrl() {
		return this.jdbcConnectionUrl;
	}

	@Override
	public String getJdbcCredentialsUsername() {
		return this.jdbcCredentialsUsername;
	}

	@Override
	public String getJdbcCredentialsPassword() {
		return this.jdbcCredentialsPassword;
	}

	// =================================================================================================================
	// INTERNAL HELPER METHODS
	// =================================================================================================================

	// private void loadSettings(final Map<String, String> settings) {
	// for (Entry<String, String> entry : settings.entrySet()) {
	// this.loadSetting(entry.getKey(), entry.getValue());
	// }
	// }
	//
	// private void loadSetting(final String key, final String value) {
	// switch (key) {
	//
	// // General settings
	// case ChronoDBSettings.STORAGE_BACKEND:
	// this.backendType = ChronoDBSettings.getSettingValueAsChronoDBBackend(key, value);
	// break;
	// case ChronoDBSettings.ENABLE_BLIND_OVERWRITE_PROTECTION:
	// this.blindOverwriteProtectionEnabled = ChronoDBSettings.getSettingValueAsBoolean(key, value);
	// break;
	// case ChronoDBSettings.DUPLICATE_VERSION_ELIMINATION_MODE:
	// this.duplicateVersionEliminationMode = ChronoDBSettings
	// .getSettingValueAsDuplicateVersionEliminationMode(key, value);
	// break;
	//
	// // File backend
	// case ChronoDBSettings.DROP_ON_SHUTDOWN:
	// this.dropOnShutdown = ChronoDBSettings.getSettingValueAsBoolean(key, value);
	// break;
	// case ChronoDBSettings.WORK_FILE:
	// String path = ChronoDBSettings.getSettingValueAsString(key, value);
	// File file = new File(path);
	// if (file.exists() == false || file.isFile() == false) {
	// throw new ChronoDBConfigurationException(
	// "The backend file does not exist or is not a file: '" + file.getAbsolutePath() + "'!");
	// }
	// this.workingFile = new File(path);
	// break;
	//
	// // JDBC Backend
	// case ChronoDBSettings.JDBC_CONNECTION_URL:
	// this.jdbcConnectionUrl = ChronoDBSettings.getSettingValueAsString(key, value);
	// break;
	// case ChronoDBSettings.JDBC_CREDENTIALS_USERNAME:
	// this.jdbcCredentialsUsername = ChronoDBSettings.getSettingValueAsString(key, value);
	// break;
	// case ChronoDBSettings.JDBC_CREDENTIALS_PASSWORD:
	// this.jdbcCredentialsPassword = ChronoDBSettings.getSettingValueAsString(key, value);
	// break;
	//
	// // in any other case, we don't recognize the setting
	// default:
	// ChronoLogger.logWarning("Detected unknown setting (will be ignored): '" + key + "'");
	// break;
	// }
	// }

}
