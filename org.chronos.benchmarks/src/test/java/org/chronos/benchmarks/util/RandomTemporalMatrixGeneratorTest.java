package org.chronos.benchmarks.util;

import static org.junit.Assert.*;

import java.io.File;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.api.dump.ChronoDBDumpFormat;
import org.chronos.chronodb.internal.api.stream.ObjectInput;
import org.chronos.chronodb.internal.impl.dump.DumpOptions;
import org.chronos.chronodb.internal.impl.dump.entry.ChronoDBDumpEntry;
import org.chronos.chronodb.internal.impl.dump.meta.BranchDumpMetadata;
import org.chronos.chronodb.internal.impl.dump.meta.ChronoDBDumpMetadata;
import org.chronos.chronodb.test.base.ChronoDBUnitTest;
import org.chronos.common.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Sets;

@Category(UnitTest.class)
public class RandomTemporalMatrixGeneratorTest extends ChronoDBUnitTest {

	@Test
	public void runTest() {
		int numberOfKeys = 1000;
		int numberOfEntries = 10_000;
		// create the dump file
		RandomTemporalMatrixGenerator generator = new RandomTemporalMatrixGenerator(numberOfKeys, numberOfEntries);
		File dumpFile = new File(this.getTestDirectory(), "Test.chronodump");
		generator.generate(dumpFile);
		System.out.println("Dump file has " + FileUtils.byteCountToDisplaySize(dumpFile.length()));
		// set up the input stream
		try (ObjectInput input = ChronoDBDumpFormat.createInput(dumpFile, new DumpOptions())) {
			// fetch the first object. It should contain the db metadata
			assertTrue(input.hasNext());
			Object firstObject = input.next();
			assertNotNull(firstObject);
			assertTrue(firstObject instanceof ChronoDBDumpMetadata);
			ChronoDBDumpMetadata metadata = (ChronoDBDumpMetadata) firstObject;
			// we should always have the master branch in the metadata
			this.assertBranchMetadataContainsMasterBranch(metadata);
			// for the remaining entries, we simply count and assert that we receive the same amount that was generated.
			int entryCount = 0;
			Set<String> keySet = Sets.newHashSet();
			while (input.hasNext()) {
				ChronoDBDumpEntry<?> entry = (ChronoDBDumpEntry<?>) input.next();
				keySet.add(entry.getChronoIdentifier().getKey());
				entryCount++;
			}
			assertEquals(numberOfEntries, entryCount);
			assertTrue(keySet.size() <= numberOfKeys);
		}
	}

	private void assertBranchMetadataContainsMasterBranch(final ChronoDBDumpMetadata metadata) {
		Set<BranchDumpMetadata> branchDumpMetadata = metadata.getBranchDumpMetadata();
		Optional<BranchDumpMetadata> masterBranch = branchDumpMetadata.stream()
				.filter(branch -> this.isMasterBranchMetadata(branch)).findAny();
		assertTrue(masterBranch.isPresent());
	}

	private boolean isMasterBranchMetadata(final BranchDumpMetadata branch) {
		if (branch == null) {
			return false;
		}
		if (ChronoDBConstants.MASTER_BRANCH_IDENTIFIER.equals(branch.getName()) == false) {
			return false;
		}
		if (branch.getBranchingTimestamp() != 0L) {
			return false;
		}
		if (branch.getParentName() != null) {
			return false;
		}
		return true;
	}

}
