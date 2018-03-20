package org.chronos.chronodb.internal.impl.engines.chunkdb;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.chronos.chronodb.internal.api.Period;
import org.chronos.chronodb.internal.impl.engines.tupl.TuplUtils;
import org.chronos.common.exceptions.ChronosIOException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.google.common.base.Preconditions.*;

public class BranchChunkManager {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private final File rootDirectory;

    private final ReadWriteLock accessLock = new ReentrantReadWriteLock(true);

    private final NavigableMap<Period, ChronoChunk> periodToChunk;

    // =================================================================================================================
    // CONSTRUCTOR
    // =================================================================================================================

    public BranchChunkManager(final File rootDir) {
        checkNotNull(rootDir, "Precondition violation - argument 'rootDir' must not be NULL!");
        checkArgument(rootDir.exists(),
                "Precondition violation - argument 'rootDir' must refer to an existing directory!");
        checkArgument(rootDir.isDirectory(),
                "Precondition violation - argument 'rootDir' must refer to a directory (not a file)!");
        this.rootDirectory = rootDir;
        this.periodToChunk = this.scanDirectoryForChunks();
        this.createHeadRevisionChunkIfNecessary();
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    public File getRootDirectory() {
        return this.rootDirectory;
    }

    public ChronoChunk getChunkForHeadRevision() {
        return this.getChunkForTimestamp(Long.MAX_VALUE);
    }

    public ChronoChunk getChunkForTimestamp(final long timestamp) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        this.accessLock.readLock().lock();
        try {
            // the map from period to chunk is sorted by period. Periods implement "comparable" by
            // comparing their lower bounds ONLY.
            Period searchPeriod = Period.createOpenEndedRange(timestamp);
            Entry<Period, ChronoChunk> entry = this.periodToChunk.floorEntry(searchPeriod);
            if (entry == null) {
                return null;
            }
            return entry.getValue();
        } finally {
            this.accessLock.readLock().unlock();
        }
    }

    /**
     * Gets all chunks within the given period in ascending order.
     *
     * @param period The period to get all overlapping and contained chunks of. Must not be <code>null</code>.
     * @return The list of chunks in the period in ascending order. Maybe empty, never <code>null</code>.
     */
    public List<ChronoChunk> getChunksForPeriod(final Period period) {
        checkNotNull(period, "Precondition violation - argument 'period' must not be NULL!");
        this.accessLock.readLock().lock();
        try {
            List<ChronoChunk> resultList = Lists.newArrayList();
            // iterate all chunks and pick the ones that are within the bounds of the period
            for (Entry<Period, ChronoChunk> entry : this.periodToChunk.entrySet()) {
                if (entry.getKey().overlaps(period)) {
                    resultList.add(entry.getValue());
                }
            }
            return resultList;
        } finally {
            this.accessLock.readLock().unlock();
        }
    }

    public ChronoChunk terminateChunkAndCreateNewHeadRevision(final long timestamp, final File newChunkDataFile) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkNotNull(newChunkDataFile, "Precondition violation - argument 'newChunkDataFile' must not be NULL!");
        checkArgument(newChunkDataFile.exists(),
                "Precondition violation - argument 'newChunkDataFile' must refer to an existing file!");
        checkArgument(newChunkDataFile.isFile(),
                "Precondition violation - argument 'newChunkDataFile' must refer to a file (not a directory)!");
        checkArgument(newChunkDataFile.canRead(),
                "Precondition violation - argument 'newChunkDataFile' must be readable!");
        checkArgument(newChunkDataFile.canWrite(),
                "Precondition violation - arugment 'newChunkDataFile' must be writable!");
        this.accessLock.writeLock().lock();
        try {
            // the general approach here is to first terminate the current head revision data chunk file at the
            // given timestamp (inclusive), including the corresponding index chunk file (if any). Then, we create
            // the new head revision data chunk file with the given timestamp + 1 as lower bound.
            Optional<Entry<Period, ChronoChunk>> maybeEntry = this.periodToChunk.entrySet().stream()
                    .filter(entry -> entry.getKey().isOpenEnded()).findAny();
            if (maybeEntry.isPresent() == false) {
                throw new IllegalStateException("There is no head revision data chunk to terminate!");
            }
            Entry<Period, ChronoChunk> entry = maybeEntry.get();
            Period oldPeriod = entry.getKey();
            ChronoChunk oldDataChunk = entry.getValue();
            // calculate the new chunk sequence number
            long sequenceNumber = oldDataChunk.getSequenceNumber() + 1;
            // delete all existing files that could conflict with the new chunk
            this.clearAllFilesOfChunk(sequenceNumber);
            // create the new head revision chunk
            File metaDataFile = new File(this.rootDirectory, sequenceNumber + "." + ChronoChunk.META_FILE_EXTENSION);
            try {
                metaDataFile.delete();
                metaDataFile.createNewFile();
            } catch (IOException ioe) {
                // nothing to undo, just throw the exception
                throw new IllegalStateException("Unable to write data chunk file!", ioe);
            }
            // terminate the old period at the given timestamp
            oldDataChunk.getMetaData().setValidTo(timestamp);
            oldDataChunk.getMetaData().flush();
            ChronoChunkMetaData newMetaData = new ChronoChunkMetaData(metaDataFile);
            newMetaData.setValidFrom(timestamp);
            newMetaData.setValidTo(Long.MAX_VALUE);
            newMetaData.flush();
            // rename the file to use the sequence number
            Path sourcePath = newChunkDataFile.toPath();
            File chunkDbFile = new File(this.rootDirectory,
                    sequenceNumber + "." + ChronoChunk.CHUNK_FILE_EXTENSION + "." + TuplUtils.TUPL_DB_FILE_EXTENSION);
            Path destPath = chunkDbFile.toPath();
            try {
                File destinationFile = destPath.toFile();
                if (destinationFile.exists()) {
                    boolean deleted = destinationFile.delete();
                    if (!deleted) {
                        throw new IOException("Failed to delete file '" + destinationFile.getAbsolutePath() + "'!");
                    }
                }
                Files.move(sourcePath, destPath, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                throw new ChronosIOException("Failed to rename data chunk file!", e);
            }
            // create the cchunk file. Creating an empty file is an atomic operation on most file systems, and
            // "seals off" the rollover process on disk. Without this file, chronos will not recognize the
            // other created files at startup and ignore them, treating the chunk as non-existent.
            File cchunkFile = new File(this.rootDirectory, sequenceNumber + "." + ChronoChunk.CHUNK_FILE_EXTENSION);
            try {
                cchunkFile.createNewFile();
            } catch (IOException e) {
                throw new ChronosIOException(
                        "Failed to create file '" + cchunkFile.getAbsolutePath() + "'! See root cause for details.", e);
            }
            // update our file map
            this.periodToChunk.remove(oldPeriod);
            this.periodToChunk.put(oldPeriod.setUpperBound(timestamp), oldDataChunk);
            // create the new head revision chunk
            ChronoChunk headRevisionChunk = new ChronoChunk(newMetaData, cchunkFile);
            Period newHeadRevisionPeriod = Period.createOpenEndedRange(timestamp);
            this.periodToChunk.put(newHeadRevisionPeriod, headRevisionChunk);
            return headRevisionChunk;
        } finally {
            this.accessLock.writeLock().unlock();
        }
    }

    public void dropChunkIndexFiles() {
        for (ChronoChunk chunk : this.periodToChunk.values()) {
            chunk.deleteIndexFile();
        }
    }

    public String getBranchName() {
        return this.getChunkForHeadRevision().getBranchName();
    }

    // =================================================================================================================
    // INITIALIZATION HELPERS
    // =================================================================================================================

    private void createHeadRevisionChunkIfNecessary() {
        Entry<Period, ChronoChunk> lastEntry = this.periodToChunk.lastEntry();
        if (lastEntry == null) {
            try {
                // no chunks, create an initial (open-ended) one
                // first, clear any existing files
                this.clearAllFilesOfChunk(0);
                File metaFile = new File(this.rootDirectory, "0." + ChronoChunk.META_FILE_EXTENSION);
                metaFile.delete();
                metaFile.createNewFile();
                // create meta data
                ChronoChunkMetaData metaData = new ChronoChunkMetaData(metaFile);
                metaData.setValidFrom(0);
                metaData.setValidTo(Long.MAX_VALUE);
                metaData.flush();
                File dataFile = new File(this.rootDirectory, "0." + ChronoChunk.CHUNK_FILE_EXTENSION);
                dataFile.delete();
                dataFile.createNewFile();
                // create chunk and place it in our routing map
                ChronoChunk chunk = new ChronoChunk(metaData, dataFile);
                this.periodToChunk.put(metaData.getValidPeriod(), chunk);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to create head revision chunk!", e);
            }
        } else {
            Period period = lastEntry.getKey();
            if (period.isOpenEnded() == false) {
                // we found a chunk and it is not open ended, open-up period
                this.periodToChunk.remove(period);
                ChronoChunk chunk = lastEntry.getValue();
                ChronoChunkMetaData metaData = chunk.getMetaData();
                metaData.setValidTo(Long.MAX_VALUE);
                metaData.flush();
                this.periodToChunk.put(metaData.getValidPeriod(), chunk);
            }
        }
    }

    private NavigableMap<Period, ChronoChunk> scanDirectoryForChunks() {
        this.accessLock.writeLock().lock();
        try {
            NavigableMap<Period, ChronoChunk> resultMap = Maps.newTreeMap();
            // list the contents of the branch directory, and consider only the files that match our naming pattern
            File[] metaFiles = this.listDataMetaFiles(this.rootDirectory);
            for (File metaFile : metaFiles) {
                // read and parse meta file
                ChronoChunkMetaData chunkMetaFile = new ChronoChunkMetaData(metaFile);
                if (chunkMetaFile.isValid() == false) {
                    // integrity of this meta-file has been compromised, delete it
                    chunkMetaFile.getMetaFile().delete();
                    continue;
                }
                Period period = chunkMetaFile.getValidPeriod();
                // create chunk container
                ChronoChunk chunk = this.getChronoChunkFromMetaData(chunkMetaFile);
                if (chunk == null) {
                    // ignore invalid/missing chunk
                    continue;
                }
                resultMap.put(period, chunk);
            }
            return resultMap;
        } finally {
            this.accessLock.writeLock().unlock();
        }
    }

    // =================================================================================================================
    // MISCEALLANEOUS HELPER METHODS
    // =================================================================================================================

    private File[] listDataMetaFiles(final File directory) {
        checkNotNull(directory, "Precondition violation - argument 'directory' must not be NULL!");
        return directory.listFiles((dir, name) -> ChronoChunk.META_FILENAME_PATTERN.matcher(name).matches());
    }

    private ChronoChunk getChronoChunkFromMetaData(final ChronoChunkMetaData metaData) {
        checkNotNull(metaData, "Precondition violation - argument 'metaData' must not be NULL!");
        String metaFileName = metaData.getMetaFile().getName();
        String sequenceNumber = metaFileName.substring(0, metaFileName.lastIndexOf("."));
        File dataFile = new File(this.rootDirectory, sequenceNumber + "." + ChronoChunk.CHUNK_FILE_EXTENSION);
        File indexFile = new File(this.rootDirectory, sequenceNumber + "." + ChronoChunk.INDEX_FILE_EXTENSION);
        if (indexFile.exists() == false || indexFile.isDirectory() || indexFile.canWrite() == false) {
            indexFile = null;
        }
        try {
            return new ChronoChunk(metaData, dataFile, indexFile);
        } catch (Exception e) {
            return null;
        }
    }

    private void clearAllFilesOfChunk(final long chunkNumber) {
        File[] chunkFiles = this.rootDirectory.listFiles((file, name) -> name.startsWith("" + chunkNumber));
        if (chunkFiles == null || chunkFiles.length <= 0) {
            // there are no files to delete
            return;
        }
        for (File chunkFile : chunkFiles) {
            chunkFile.delete();
        }
    }

}
