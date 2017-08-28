package org.chronos.benchmarks.chronodb.dump;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.chronos.benchmarks.util.RandomTemporalMatrixGenerator;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.DumpOption;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.common.logging.ChronoLogger;
import org.chronos.common.test.junit.categories.PerformanceTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * A simple benchmark for testing RAM consumption with the DB dump feature.
 *
 * <p>
 * The benchmark consists of the following steps:
 * <ol>
 * <li>Generate a random matrix and write it into a DB dump file.
 * <li>Read that dump file with ChronoDB.
 * <li>From the resulting DB, create a dump.
 * </ol>
 *
 * <p>
 * Since this test does not produce any output on its own, please use JVisualVM to monitor the memory consumption.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
@Category(PerformanceTest.class)
public class SimpleDumpBenchmark extends AllChronoDBBackendsTest {

	private static final int KEY_SET_SIZE = 10_000;
	private static final int ENTRY_SET_SIZE = 100_000;

	@Test
	public void runBenchmark() throws Exception {
		ChronoDB db = this.getChronoDB();
		ChronoLogger.log("--Xmx is set to " + FileUtils.byteCountToDisplaySize(Runtime.getRuntime().maxMemory()));
		ChronoLogger.log("Starting Benchmark in 5 seconds. Please startup JVisualVM Monitoring.");
		this.sleep(5 * 1000);
		// create the test file
		File dumpFile = new File(this.getTestDirectory(), "generated.chronodump");
		dumpFile.createNewFile();
		dumpFile.deleteOnExit();
		// prepare the generator
		RandomTemporalMatrixGenerator generator = new RandomTemporalMatrixGenerator(KEY_SET_SIZE, ENTRY_SET_SIZE);
		ChronoLogger.log("Generating initial dump file containing " + ENTRY_SET_SIZE + " entries.");
		generator.generate(dumpFile);
		ChronoLogger.log("Dump file generated successfully.");
		ChronoLogger.log("Reading dump file into DB instance of type [" + this.getChronoBackendName() + "].");
		db.readDump(dumpFile, DumpOption.batchSize(10_000));
		ChronoLogger.log("Successfully read dump file into DB.");
		// stream out the data into a new file
		File dumpFile2 = new File(this.getTestDirectory(), "output.chronodump");
		dumpFile2.createNewFile();
		dumpFile2.deleteOnExit();
		ChronoLogger.log("Writing ChronoDB dump into test file.");
		db.writeDump(dumpFile2);
		ChronoLogger.log("Dump file written successfully.");
		ChronoLogger.log("Original dump has " + FileUtils.byteCountToDisplaySize(dumpFile.length())
				+ ", output dump has " + FileUtils.byteCountToDisplaySize(dumpFile.length()));
	}
}
