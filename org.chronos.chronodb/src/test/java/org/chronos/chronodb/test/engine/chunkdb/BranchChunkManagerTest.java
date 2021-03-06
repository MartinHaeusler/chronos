package org.chronos.chronodb.test.engine.chunkdb;

import org.chronos.chronodb.internal.api.Period;
import org.chronos.chronodb.internal.impl.IBranchMetadata;
import org.chronos.chronodb.internal.impl.engines.chunkdb.BranchChunkManager;
import org.chronos.chronodb.internal.impl.engines.chunkdb.BranchMetadataFile;
import org.chronos.chronodb.internal.impl.engines.chunkdb.ChronoChunk;
import org.chronos.chronodb.internal.impl.engines.chunkdb.ChunkedChronoDB;
import org.chronos.chronodb.internal.util.ChronosFileUtils;
import org.chronos.chronodb.test.base.ChronoDBUnitTest;
import org.chronos.common.exceptions.ChronosIOException;
import org.chronos.common.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

@Category(UnitTest.class)
public class BranchChunkManagerTest extends ChronoDBUnitTest {

    @Test
    public void canCreateChunkManager() {
        this.createBranchMetadataFile(this.getTestDirectory());
        BranchChunkManager cm = new BranchChunkManager(this.getTestDirectory());
        assertNotNull(cm);
    }

    @Test
    public void defaultChunkFileIsCreatedAutomatically() {
        this.createBranchMetadataFile(this.getTestDirectory());
        BranchChunkManager cm = new BranchChunkManager(this.getTestDirectory());
        assertNotNull(cm);
        File[] files = this.getTestDirectory().listFiles();
        assertEquals(4, files.length);
        File dataFile = new File(this.getTestDirectory(), "0." + ChronoChunk.CHUNK_FILE_EXTENSION);
        assertTrue(dataFile.exists());
        assertTrue(dataFile.isFile());
        File metaFile = new File(this.getTestDirectory(), "0." + ChronoChunk.META_FILE_EXTENSION);
        assertTrue(metaFile.exists());
        assertTrue(metaFile.isFile());
    }

    @Test
    public void canGetCorrectChunkForTimestampWhenOnlyOneChunkExists() {
        this.createBranchMetadataFile(this.getTestDirectory());
        BranchChunkManager cm = new BranchChunkManager(this.getTestDirectory());
        ChronoChunk chunk = cm.getChunkForTimestamp(System.currentTimeMillis());
        assertNotNull(chunk);
        assertEquals(0, chunk.getSequenceNumber());
        assertEquals(0, chunk.getMetaData().getValidFrom());
        assertEquals(Long.MAX_VALUE, chunk.getMetaData().getValidTo());
        assertTrue(chunk.getMetaData().getValidPeriod().isOpenEnded());
    }

    @Test
    public void canGetIndexFileForTimestamp() {
        this.createBranchMetadataFile(this.getTestDirectory());
        BranchChunkManager cm = new BranchChunkManager(this.getTestDirectory());
        long timestamp = System.currentTimeMillis();
        ChronoChunk chunk = cm.getChunkForTimestamp(timestamp);
        assertNotNull(chunk);
        assertFalse(chunk.hasIndexFile());
        chunk.createIndexFileIfNotExists();
        assertTrue(chunk.hasIndexFile());
        File indexFile = chunk.getIndexFile();
        assertNotNull(indexFile);
        assertTrue(indexFile.exists());
        assertTrue(indexFile.isFile());
        assertEquals("0." + ChronoChunk.INDEX_FILE_EXTENSION, indexFile.getName());
    }

    @Test
    public void canDoDataChunkRollover() {
        this.createBranchMetadataFile(this.getTestDirectory());
        BranchChunkManager cm = new BranchChunkManager(this.getTestDirectory());
        long timestamp = 5000;
        // do the rollover
        cm.terminateChunkAndCreateNewHeadRevision(timestamp,
                this.createFile("temp." + ChronoChunk.CHUNK_FILE_EXTENSION));
        File[] dataDirFiles = this.getTestDirectory().listFiles();
        // there should be 8 files in the directory now:
        //
        // - branchMetadata.properties
        // - 0.cchunk ("0.cchunk.db" does not exist yet because the chunk was never touched)
        // - 0.cmeta
        // - 0.cmeta.db
        // - 1.cchunk
        // - 1.cchunk.db (empty Tupl database)
        // - 1.cmeta
        // - 1.cmeta.db
        //
        assertEquals(8, dataDirFiles.length);
        File chunk0meta = new File(this.getTestDirectory(), "0." + ChronoChunk.META_FILE_EXTENSION);
        File chunk0data = new File(this.getTestDirectory(), "0." + ChronoChunk.CHUNK_FILE_EXTENSION);
        File chunk1meta = new File(this.getTestDirectory(), "1." + ChronoChunk.META_FILE_EXTENSION);
        File chunk1data = new File(this.getTestDirectory(), "1." + ChronoChunk.CHUNK_FILE_EXTENSION);
        assertTrue(ChronosFileUtils.isExistingFile(chunk0meta));
        assertTrue(ChronosFileUtils.isExistingFile(chunk0data));
        assertTrue(ChronosFileUtils.isExistingFile(chunk1meta));
        assertTrue(ChronosFileUtils.isExistingFile(chunk1data));
        // we should get two chunks now
        List<ChronoChunk> chunks = cm.getChunksForPeriod(Period.createOpenEndedRange(0));
        assertEquals(2, chunks.size());
        // try to access both chunks
        ChronoChunk chunk0 = cm.getChunkForTimestamp(0);
        assertEquals(0, chunk0.getSequenceNumber());
        ChronoChunk chunk1 = cm.getChunkForTimestamp(timestamp);
        assertEquals(chunk1, cm.getChunkForTimestamp(Long.MAX_VALUE));
        assertEquals(chunk1, cm.getChunkForHeadRevision());
        assertEquals(1, chunk1.getSequenceNumber());
    }

    @Test
    public void canDoMultipleRollovers() {
        this.createBranchMetadataFile(this.getTestDirectory());
        BranchChunkManager cm = new BranchChunkManager(this.getTestDirectory());
        ChronoChunk chunk0 = cm.getChunkForHeadRevision();
        long timestamp1 = 5000;
        long timestamp2 = 10000;
        long timestamp3 = 15000;
        ChronoChunk chunk1 = cm.terminateChunkAndCreateNewHeadRevision(timestamp1,
                this.createFile("temp." + ChronoChunk.CHUNK_FILE_EXTENSION));
        ChronoChunk chunk2 = cm.terminateChunkAndCreateNewHeadRevision(timestamp2,
                this.createFile("temp." + ChronoChunk.CHUNK_FILE_EXTENSION));
        ChronoChunk chunk3 = cm.terminateChunkAndCreateNewHeadRevision(timestamp3,
                this.createFile("temp." + ChronoChunk.CHUNK_FILE_EXTENSION));
        // we should have a total of 4 chunk files now
        List<ChronoChunk> chunks = cm.getChunksForPeriod(Period.createOpenEndedRange(0));
        assertEquals(4, chunks.size());
        // try to request a few timestamps and assert that the right chunks are coming back
        assertEquals(chunk0, cm.getChunkForTimestamp(0));
        assertEquals(chunk0, cm.getChunkForTimestamp(timestamp1 / 2));
        assertEquals(chunk1, cm.getChunkForTimestamp(timestamp1));
        assertEquals(chunk1, cm.getChunkForTimestamp(timestamp1 + 1));
        assertEquals(chunk1, cm.getChunkForTimestamp((timestamp1 + timestamp2) / 2));
        assertEquals(chunk2, cm.getChunkForTimestamp(timestamp2));
        assertEquals(chunk2, cm.getChunkForTimestamp(timestamp2 + 1));
        assertEquals(chunk2, cm.getChunkForTimestamp((timestamp2 + timestamp3) / 2));
        assertEquals(chunk3, cm.getChunkForTimestamp(timestamp3));
        assertEquals(chunk3, cm.getChunkForTimestamp(timestamp3 + 1));
        assertEquals(chunk3, cm.getChunkForTimestamp(timestamp3 + 1000));
        assertEquals(chunk3, cm.getChunkForTimestamp(Long.MAX_VALUE));
    }

    @Test
    public void canTolerateLossOfSingleIntermediateChunkMetaFile() {
        this.createBranchMetadataFile(this.getTestDirectory());
        BranchChunkManager cm = new BranchChunkManager(this.getTestDirectory());
        long timestamp1 = 5000;
        long timestamp2 = 10000;
        long timestamp3 = 15000;
        // roll over
        cm.terminateChunkAndCreateNewHeadRevision(timestamp1,
                this.createFile("temp." + ChronoChunk.CHUNK_FILE_EXTENSION));
        cm.terminateChunkAndCreateNewHeadRevision(timestamp2,
                this.createFile("temp." + ChronoChunk.CHUNK_FILE_EXTENSION));
        cm.terminateChunkAndCreateNewHeadRevision(timestamp3,
                this.createFile("temp." + ChronoChunk.CHUNK_FILE_EXTENSION));
        // we should have a total of 16 chunk files now:
        //
        // - branchMetadata.properties
        // - 0.cchunk ("0.cchunk.db" does not exist because the chunk was never touched)
        // - 0.cmeta
        // - 0.cmeta.db
        // - 1.cchunk
        // - 1.cchunk.db (empty Tupl database)
        // - 1.cmeta
        // - 1.cmeta.db
        // - 2.cchunk
        // - 2.cchunk.db (empty Tupl database)
        // - 2.cmeta
        // - 2.cmeta.db
        // - 3.cchunk
        // - 3.cchunk.db (empty Tupl database)
        // - 3.cmeta
        // - 3.cmeta.db
        //
        assertEquals(16, this.getTestDirectory().list().length);
        // get chunk #2
        File chunk2MetaFile = new File(this.getTestDirectory(), "1." + ChronoChunk.META_FILE_EXTENSION);
        assertTrue(chunk2MetaFile.exists());
        assertTrue(chunk2MetaFile.isFile());
        // delete it
        assertTrue(chunk2MetaFile.delete());
        // create a new chrono chunk manager
        cm = new BranchChunkManager(this.getTestDirectory());
        // request a timestamp in the range of chunk #2, and expect to get chunk #1 back
        ChronoChunk chunk = cm.getChunkForTimestamp((timestamp1 + timestamp2) / 2);
        assertNotNull(chunk);
        assertEquals(0, chunk.getSequenceNumber());
    }

    @Test
    public void canTolerateLossOfSingleIntermediateChunkDataFile() {
        this.createBranchMetadataFile(this.getTestDirectory());
        BranchChunkManager cm = new BranchChunkManager(this.getTestDirectory());
        long timestamp1 = 5000;
        long timestamp2 = 10000;
        long timestamp3 = 15000;
        // roll over
        cm.terminateChunkAndCreateNewHeadRevision(timestamp1,
                this.createFile("temp." + ChronoChunk.CHUNK_FILE_EXTENSION));
        cm.terminateChunkAndCreateNewHeadRevision(timestamp2,
                this.createFile("temp." + ChronoChunk.CHUNK_FILE_EXTENSION));
        cm.terminateChunkAndCreateNewHeadRevision(timestamp3,
                this.createFile("temp." + ChronoChunk.CHUNK_FILE_EXTENSION));
        // we should have a total of 16 chunk files now:
        //
        // - branchMetadata.properties
        // - 0.cchunk ("0.cchunk.db" does not exist because the chunk was never touched)
        // - 0.cmeta
        // - 0.cmeta.db
        // - 1.cchunk
        // - 1.cchunk.db (empty Tupl database)
        // - 1.cmeta
        // - 1.cmeta.db
        // - 2.cchunk
        // - 2.cchunk.db (empty Tupl database)
        // - 2.cmeta
        // - 2.cmeta.db
        // - 3.cchunk
        // - 3.cchunk.db (empty Tupl database)
        // - 3.cmeta
        // - 3.cmeta.db
        //
        assertEquals(16, this.getTestDirectory().list().length);
        // get chunk #2
        File chunk2DataFile = new File(this.getTestDirectory(), "1." + ChronoChunk.CHUNK_FILE_EXTENSION);
        assertTrue(chunk2DataFile.exists());
        assertTrue(chunk2DataFile.isFile());
        // delete it
        assertTrue(chunk2DataFile.delete());
        // create a new chrono chunk manager
        cm = new BranchChunkManager(this.getTestDirectory());
        // request a timestamp in the range of chunk #2, and expect to get chunk #1 back
        ChronoChunk chunk = cm.getChunkForTimestamp((timestamp1 + timestamp2) / 2);
        assertNotNull(chunk);
        assertEquals(0, chunk.getSequenceNumber());
    }

    @Test
    public void canTolerateLossOfHeadRevisionChunk() {
        this.createBranchMetadataFile(this.getTestDirectory());
        BranchChunkManager cm = new BranchChunkManager(this.getTestDirectory());
        long timestamp1 = 5000;
        long timestamp2 = 10000;
        long timestamp3 = 15000;
        // roll over
        cm.terminateChunkAndCreateNewHeadRevision(timestamp1,
                this.createFile("temp." + ChronoChunk.CHUNK_FILE_EXTENSION));
        cm.terminateChunkAndCreateNewHeadRevision(timestamp2,
                this.createFile("temp." + ChronoChunk.CHUNK_FILE_EXTENSION));
        cm.terminateChunkAndCreateNewHeadRevision(timestamp3,
                this.createFile("temp." + ChronoChunk.CHUNK_FILE_EXTENSION));
        // we should have a total of 16 chunk files now:
        //
        // - branchMetadata.properties
        // - 0.cchunk ("0.cchunk.db" does not exist because the chunk was never touched)
        // - 0.cmeta
        // - 0.cmeta.db
        // - 1.cchunk
        // - 1.cchunk.db (empty Tupl database)
        // - 1.cmeta
        // - 1.cmeta.db
        // - 2.cchunk
        // - 2.cchunk.db (empty Tupl database)
        // - 2.cmeta
        // - 2.cmeta.db
        // - 3.cchunk
        // - 3.cchunk.db (empty Tupl database)
        // - 3.cmeta
        // - 3.cmeta.db
        //
        assertEquals(16, this.getTestDirectory().list().length);
        // get the last chunk
        File dataChunk4 = new File(this.getTestDirectory(), "3." + ChronoChunk.META_FILE_EXTENSION);
        assertTrue(dataChunk4.exists());
        assertTrue(dataChunk4.isFile());
        // delete it
        assertTrue(dataChunk4.delete());
        // create a new chrono chunk manager
        cm = new BranchChunkManager(this.getTestDirectory());
        // request a timestamp in the range of chunk #4, and expect to get chunk #3 back, with it's upper bound set to
        // NOW
        ChronoChunk chunk = cm.getChunkForTimestamp(timestamp3 + 5000);
        assertNotNull(chunk);
        assertEquals(2, chunk.getSequenceNumber());
        assertTrue(chunk.getMetaData().getValidPeriod().isOpenEnded());
    }

    @Test
    public void canTolerateLossOfInitialChunkDataFile() {
        this.createBranchMetadataFile(this.getTestDirectory());
        BranchChunkManager cm = new BranchChunkManager(this.getTestDirectory());
        long timestamp1 = 5000;
        long timestamp2 = 10000;
        long timestamp3 = 15000;
        // roll over
        cm.terminateChunkAndCreateNewHeadRevision(timestamp1,
                this.createFile("temp." + ChronoChunk.CHUNK_FILE_EXTENSION));
        cm.terminateChunkAndCreateNewHeadRevision(timestamp2,
                this.createFile("temp." + ChronoChunk.CHUNK_FILE_EXTENSION));
        cm.terminateChunkAndCreateNewHeadRevision(timestamp3,
                this.createFile("temp." + ChronoChunk.CHUNK_FILE_EXTENSION));
        // we should have a total of 16 chunk files now:
        //
        // - branchMetadata.properties
        // - 0.cchunk ("0.cchunk.db" does not exist because the chunk was never touched)
        // - 0.cmeta
        // - 0.cmeta.db
        // - 1.cchunk
        // - 1.cchunk.db (empty Tupl database)
        // - 1.cmeta
        // - 1.cmeta.db
        // - 2.cchunk
        // - 2.cchunk.db (empty Tupl database)
        // - 2.cmeta
        // - 2.cmeta.db
        // - 3.cchunk
        // - 3.cchunk.db (empty Tupl database)
        // - 3.cmeta
        // - 3.cmeta.db
        //
        assertEquals(16, this.getTestDirectory().list().length);
        // get the first chunk
        File dataChunk1 = new File(this.getTestDirectory(), "0." + ChronoChunk.CHUNK_FILE_EXTENSION);
        assertTrue(dataChunk1.exists());
        assertTrue(dataChunk1.isFile());
        // delete it
        assertTrue(dataChunk1.delete());
        // create a new chrono chunk manager
        cm = new BranchChunkManager(this.getTestDirectory());
        // request a timestamp in the range of chunk #1
        ChronoChunk chunk = cm.getChunkForTimestamp(1);
        // we have no data for this range
        assertNull(chunk);
    }

    @Test
    public void canTolerateLossOfInitialChunkMetaFile() throws Exception {
        this.createBranchMetadataFile(this.getTestDirectory());
        BranchChunkManager cm = new BranchChunkManager(this.getTestDirectory());
        long timestamp1 = 5000;
        long timestamp2 = 10000;
        long timestamp3 = 15000;
        // roll over
        cm.terminateChunkAndCreateNewHeadRevision(timestamp1,
                this.createFile("temp." + ChronoChunk.CHUNK_FILE_EXTENSION));
        cm.terminateChunkAndCreateNewHeadRevision(timestamp2,
                this.createFile("temp." + ChronoChunk.CHUNK_FILE_EXTENSION));
        cm.terminateChunkAndCreateNewHeadRevision(timestamp3,
                this.createFile("temp." + ChronoChunk.CHUNK_FILE_EXTENSION));
        System.out.println(Arrays.toString(this.getTestDirectory().list()));
        // we should have a total of 16 chunk files now:
        //
        // - branchMetadata.properties
        // - 0.cchunk ("0.cchunk.db" does not exist because the chunk was never touched)
        // - 0.cmeta
        // - 0.cmeta.db
        // - 1.cchunk
        // - 1.cchunk.db (empty Tupl database)
        // - 1.cmeta
        // - 1.cmeta.db
        // - 2.cchunk
        // - 2.cchunk.db (empty Tupl database)
        // - 2.cmeta
        // - 2.cmeta.db
        // - 3.cchunk
        // - 3.cchunk.db (empty Tupl database)
        // - 3.cmeta
        // - 3.cmeta.db
        //
        assertEquals(16, this.getTestDirectory().list().length);
        // get the first chunk
        File dataChunk1 = new File(this.getTestDirectory(), "0." + ChronoChunk.META_FILE_EXTENSION);
        assertTrue(dataChunk1.exists());
        assertTrue(dataChunk1.isFile());
        // delete it
        assertTrue(dataChunk1.delete());
        // create a new chrono chunk manager
        cm = new BranchChunkManager(this.getTestDirectory());
        // request a timestamp in the range of chunk #1
        ChronoChunk chunk = cm.getChunkForTimestamp(1);
        // we have no data for this range
        assertNull(chunk);
    }

    // =================================================================================================================
    // HELPER METHODS
    // =================================================================================================================

    private File createFile(final String path) {
        try {
            File file = new File(this.getTestDirectory(), path);
            if (file.exists() == false) {
                boolean created = file.createNewFile();
                if (!created) {
                    throw new IOException("Unable to create file '" + file.getAbsolutePath() + "'!");
                }
            }
            return file;
        } catch (IOException ioe) {
            throw new ChronosIOException("Failed to create file", ioe);
        }
    }

    private void createBranchMetadataFile(final File directory) {
        try {
            File branchMetadataFile = new File(directory, ChunkedChronoDB.FILENAME__BRANCH_METADATA_FILE);
            branchMetadataFile.createNewFile();
            BranchMetadataFile.write(IBranchMetadata.createMasterBranchMetadata(), branchMetadataFile);
        } catch (IOException e) {
            throw new ChronosIOException("Failed to create branch metadata file", e);
        }
    }
}
