package org.chronos.chronodb.exodus

import io.github.oshai.kotlinlogging.KotlinLogging
import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.StoreConfig
import org.apache.commons.io.FileUtils
import org.chronos.chronodb.api.*
import org.chronos.chronodb.api.exceptions.ChronosBuildVersionConflictException
import org.chronos.chronodb.exodus.builder.ExodusChronoDBBuilder
import org.chronos.chronodb.exodus.builder.ExodusChronoDBBuilderImpl
import org.chronos.chronodb.exodus.configuration.ExodusChronoDBConfiguration
import org.chronos.chronodb.exodus.environment.EnvironmentManager
import org.chronos.chronodb.exodus.kotlin.ext.*
import org.chronos.chronodb.exodus.layout.ChronoDBDirectoryLayout
import org.chronos.chronodb.exodus.layout.ChronoDBStoreLayout
import org.chronos.chronodb.exodus.manager.*
import org.chronos.chronodb.exodus.manager.chunk.GlobalChunkManager
import org.chronos.chronodb.inmemory.InMemorySerializationManager
import org.chronos.chronodb.internal.api.BranchManagerInternal
import org.chronos.chronodb.internal.api.DatebackManagerInternal
import org.chronos.chronodb.internal.api.StatisticsManagerInternal
import org.chronos.chronodb.internal.api.cache.ChronoDBCache
import org.chronos.chronodb.internal.api.migration.MigrationChain
import org.chronos.chronodb.internal.api.query.QueryManager
import org.chronos.chronodb.internal.api.stream.ChronoDBEntry
import org.chronos.chronodb.internal.impl.IBranchMetadata
import org.chronos.chronodb.internal.impl.engines.base.AbstractChronoDB
import org.chronos.chronodb.internal.impl.query.StandardQueryManager
import org.chronos.common.version.ChronosVersion
import java.io.File

class ExodusChronoDB : AbstractChronoDB {

    companion object {
        const val BACKEND_NAME = "xodus"

        val BUILDER: Class<out ExodusChronoDBBuilder> = ExodusChronoDBBuilderImpl::class.java

        private val log = KotlinLogging.logger {}

    }

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    val globalChunkManager: GlobalChunkManager
    private val indexManager: ExodusIndexManager
    private val serializationManager: SerializationManager
    private val branchManager: BranchManagerInternal
    private val queryManager: QueryManager
    private val statisticsManager: StatisticsManagerInternal
    private val maintenanceManager: ExodusMaintenanceManager
    private val datebackManager: DatebackManagerInternal
    private val backupManager: BackupManager
    private val cache: ChronoDBCache

    val configuration: ExodusChronoDBConfiguration
        get() = super.getConfiguration() as ExodusChronoDBConfiguration

    // =================================================================================================================
    // CONSTRUCTOR
    // =================================================================================================================

    constructor(configuration: ExodusChronoDBConfiguration) : super(configuration) {
        val workDir = configuration.workDirectory!!
        val isNewDatabaseInstance = !File(workDir, ChronoDBDirectoryLayout.BRANCHES_DIRECTORY).exists()
        this.serializationManager = InMemorySerializationManager()
        val exodusConfig = configuration.extractExodusConfiguration()
        val environmentManager = EnvironmentManager(exodusConfig, configuration.keepOpenEnvironments, configuration.environmentCleanPeriodSeconds)
        val globalEnvironment = environmentManager.getEnvironment(File(workDir, ChronoDBDirectoryLayout.GLOBAL_DIRECTORY))
        val branchNameResolver = createBranchNameResolver(globalEnvironment)
        this.globalChunkManager = GlobalChunkManager.create(workDir, branchNameResolver, environmentManager)
        this.updateBuildVersionInDatabase(this.configuration.isReadOnly)
        this.branchManager = ExodusBranchManager(this)
        this.indexManager = ExodusIndexManager(this)
        this.queryManager = StandardQueryManager(this)
        this.maintenanceManager = ExodusMaintenanceManager(this)
        this.statisticsManager = ExodusStatisticsManager(this)
        this.datebackManager = ExodusDatebackManager(this)
        this.backupManager = ExodusBackupManager(this)
        this.cache = ChronoDBCache.createCacheForConfiguration(configuration)
        this.addShutdownHook {
            this.globalChunkManager.close()
        }
        if (isNewDatabaseInstance) {
            // this database is a completely new instance -> write the latest chronos version into the database
            this.updateChronosVersionTo(ChronosVersion.getCurrentVersion())
        }
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    override fun updateBuildVersionInDatabase(readOnly: Boolean): ChronosVersion {
        // note: since after 0.1.0, we store the chronos version in the database. If there
        // is no stored version, we have to assume it is from that version.
        val initialVersion = ChronosVersion.parse("0.1.0")
        val storedVersion = this.storedChronosVersion.orIfNull(initialVersion)
        val currentVersion = ChronosVersion.getCurrentVersion()
        when {
            storedVersion == currentVersion -> {
                // precise version match; no migration necessary
                return storedVersion
            }

            storedVersion.isGreaterThan(currentVersion) -> {
                // the database has been written by a NEWER version of chronos; we might be incompatible
                if (currentVersion.isReadCompatibleWith(storedVersion)) {
                    log.warn {
                        "The database was written by Chronos '$storedVersion', but this is the older version '$currentVersion'! " +
                            "Some features may be unsupported by this older version. " +
                            "We strongly recommend updating Chronos to version '$storedVersion' or higher for working with this database!"
                    }
                    return storedVersion
                } else {
                    // the current chronos version is not read-compatible with the (newer) version that created this
                    // database; we must not touch it
                    throw ChronosBuildVersionConflictException(
                        "The database was written by Chronos '$storedVersion', but this is the older version '${ChronosVersion.getCurrentVersion()}'! " +
                            "Older versions of Chronos cannot open databases created by newer versions!"
                    )
                }
            }

            else -> {
                // stored version is LESS than the current version
                if (readOnly) {
                    if (storedVersion == initialVersion) {
                        throw ChronosBuildVersionConflictException("Cannot create a new database in read-only mode.")
                    }

                    // check if we can still open the database (only difference in the patch level)
                    if (canOpenDatabaseReadOnlyWithoutUpdatingVersions(storedVersion, currentVersion)) {
                        log.warn {
                            "Patch Version in database (${storedVersion}) is different from runtime (${currentVersion})!" +
                                " Cannot update the database because it is in read-only mode."
                        }
                        return storedVersion
                    }

                    throw ChronosBuildVersionConflictException(
                        "The database was written by Chronos '$storedVersion', " +
                            "but this is the newer version '${ChronosVersion.getCurrentVersion()}'. " +
                            "This newer version requires a migration of persistent data, but the " +
                            "database has been started in read-only mode, which prevents the execution of this migration. " +
                            "Please either start the database in read-write mode, or use Chronos version $storedVersion to open it."
                    )
                }
                val workDir = this.configuration.workDirectory
                val branchesDir = File(workDir, ChronoDBDirectoryLayout.BRANCHES_DIRECTORY)
                val masterDir = File(branchesDir, ChronoDBDirectoryLayout.MASTER_BRANCH_DIRECTORY)
                // do we have a master branch? If we don't, this is a new (empty) database instance that doesn't require migrations.
                if (masterDir.exists() && !FileUtils.isEmptyDirectory(masterDir)) {
                    // database was created by an older version of chronos; update it
                    this.executeMigrationChainStartingFrom(storedVersion)
                } else {
                    log.debug { "Creating a new empty instance of ChronoDB at '${workDir}'." }
                }
                // if the migration chain has been executed successfully, we can safely update our chronos version
                // to the current one (we might override the field with the same value, but that doesn't do any damage)
                this.updateChronosVersionTo(currentVersion)
                return storedVersion
            }
        }
    }

    private fun canOpenDatabaseReadOnlyWithoutUpdatingVersions(
        storedVersion: ChronosVersion,
        currentVersion: ChronosVersion,
    ): Boolean {
        if(!differenceAtMostOnPatchLevel(storedVersion, currentVersion)){
            // major or minor version difference -> can't open.
            return false
        }

        if(this.getMigrationChain(storedVersion).isNotEmpty()){
            // there is a required migration -> can't open
            return false
        }

        // should be fine
        return true
    }

    private fun differenceAtMostOnPatchLevel(versionA: ChronosVersion, versionB: ChronosVersion): Boolean {
        if (versionA.majorVersion != versionB.majorVersion) {
            // major version difference
            return false
        }
        if (versionA.minorVersion != versionB.minorVersion) {
            // minor version difference
            return false
        }

        // at most patch-level difference
        return true
    }

    override fun getQueryManager(): QueryManager {
        return this.queryManager
    }

    override fun getBranchManager(): BranchManagerInternal {
        return this.branchManager
    }

    override fun getIndexManager(): ExodusIndexManager {
        return this.indexManager
    }

    override fun getSerializationManager(): SerializationManager {
        return this.serializationManager
    }

    override fun getMaintenanceManager(): MaintenanceManager {
        return this.maintenanceManager
    }

    override fun getStatisticsManager(): StatisticsManagerInternal {
        return this.statisticsManager
    }

    override fun getDatebackManager(): DatebackManagerInternal {
        return this.datebackManager
    }

    override fun getBackupManager(): BackupManager {
        return this.backupManager
    }

    override fun getStoredChronosVersion(): ChronosVersion? {
        return this.globalChunkManager.openReadOnlyTransactionOnGlobalEnvironment().use { tx ->
            val binary = tx.get(ChronoDBStoreLayout.STORE_NAME__CHRONOS_VERSION, ChronoDBStoreLayout.KEY__CHRONOS_VERSION)
            if (binary == null) {
                return@use null
            }
            val string = binary.parseAsString()
            if (string.trim().isEmpty()) {
                return@use null
            }
            return@use ChronosVersion.parse(string)
        }
    }

    override fun updateChronosVersionTo(chronosVersion: ChronosVersion) {
        this.globalChunkManager.openReadWriteTransactionOnGlobalEnvironment().use { tx ->
            tx.put(ChronoDBStoreLayout.STORE_NAME__CHRONOS_VERSION, ChronoDBStoreLayout.KEY__CHRONOS_VERSION, chronosVersion.toString().toByteIterable())
            tx.commit()
        }
    }

    override fun requiresAutoReindexAfterDumpRead(): Boolean {
        return true
    }

    override fun getCache(): ChronoDBCache {
        return this.cache
    }

    override fun isFileBased(): Boolean {
        return true
    }

    override fun getFeatures(): ChronoDBFeatures {
        return ExodusChronoDBFeatures
    }

    // =================================================================================================================
    // MIGRATION CHAIN
    // =================================================================================================================

    private fun executeMigrationChainStartingFrom(from: ChronosVersion) {
        val migrationChain = this.getMigrationChain(from)

        if (migrationChain.isEmpty()) {
            // nothing to do
            return
        }

        log.warn { "Executing Chronos migration chain. DO NOT TERMINATE THE PROCESS until this operation is complete." }
        migrationChain.execute(this)
        log.info { "Chronos migration chain executed successfully." }
    }

    private fun getMigrationChain(from: ChronosVersion): MigrationChain<ExodusChronoDB> {
        return MigrationChain
            .fromPackage<ExodusChronoDB>("org.chronos.chronodb.exodus.migration")
            .startingAt(from)
    }

    // =================================================================================================================
    // INTERNAL HELPER METHODS
    // =================================================================================================================

    fun loadEntriesIntoChunks(entries: List<ChronoDBEntry>) {
        this.loadEntries(entries, true)
    }

    private fun createBranchNameResolver(globalEnvironment: Environment): (File) -> String? {
        val dirNameToBranchName = mutableMapOf<String, String>()
        globalEnvironment.computeInExclusiveTransaction { tx ->
            val store = globalEnvironment.openStore(ChronoDBStoreLayout.STORE_NAME__BRANCH_METADATA, StoreConfig.WITHOUT_DUPLICATES, tx)
            store.openCursor(tx).use { cursor ->
                while (cursor.next) {
                    val branchMetadata = this.serializationManager.deserialize(cursor.value.toByteArray()) as IBranchMetadata
                    dirNameToBranchName[branchMetadata.directoryName] = branchMetadata.name
                }
            }
            tx.commit()
        }
        return { directory ->
            // note: we can *always* resolve the name of the master branch, even if we have an empty branch metadata store.
            if (directory.name == ChronoDBDirectoryLayout.BRANCH_DIRECTORY_PREFIX + ChronoDBConstants.MASTER_BRANCH_IDENTIFIER) {
                ChronoDBConstants.MASTER_BRANCH_IDENTIFIER
            }
            dirNameToBranchName[directory.name]
        }
    }


}
