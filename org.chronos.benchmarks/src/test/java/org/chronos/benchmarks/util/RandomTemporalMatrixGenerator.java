package org.chronos.benchmarks.util;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.api.DumpOption;
import org.chronos.chronodb.api.dump.ChronoDBDumpFormat;
import org.chronos.chronodb.api.key.ChronoIdentifier;
import org.chronos.chronodb.internal.api.stream.ObjectOutput;
import org.chronos.chronodb.internal.impl.dump.DumpOptions;
import org.chronos.chronodb.internal.impl.dump.entry.ChronoDBDumpBinaryEntry;
import org.chronos.chronodb.internal.impl.dump.entry.ChronoDBDumpEntry;
import org.chronos.chronodb.internal.impl.dump.meta.BranchDumpMetadata;
import org.chronos.chronodb.internal.impl.dump.meta.ChronoDBDumpMetadata;
import org.chronos.chronodb.internal.impl.temporal.ChronoIdentifierImpl;
import org.chronos.chronodb.test.util.model.payload.NamedPayload;
import org.chronos.common.version.ChronosVersion;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class RandomTemporalMatrixGenerator {

	private int numberOfEntries;
	private int numberOfKeys;
	private long timestampMin;
	private long timestampMax;
	private int payloadSizeMin;
	private int payloadSizeMax;
	private long randomSeed;

	private Set<String> keys;

	public RandomTemporalMatrixGenerator(final int numberOfKeys, final int numberOfEntries) {
		this.numberOfKeys = numberOfKeys;
		this.numberOfEntries = numberOfEntries;
		this.payloadSizeMin = 1; // kilobytes
		this.payloadSizeMax = 1; // kilobytes
		this.timestampMin = 1;
		this.timestampMax = System.currentTimeMillis();
		this.randomSeed = System.currentTimeMillis();
	}

	// =================================================================================================================
	// GETTERS & SETTERS
	// =================================================================================================================

	public int getNumberOfEntries() {
		return this.numberOfEntries;
	}

	public void setNumberOfEntries(final int numberOfEntries) {
		this.numberOfEntries = numberOfEntries;
	}

	public int getNumberOfKeys() {
		return this.numberOfKeys;
	}

	public void setNumberOfKeys(final int numberOfKeys) {
		this.numberOfKeys = numberOfKeys;
		this.keys = null;
	}

	public void setKeys(final Set<String> keys) {
		this.keys = Sets.newHashSet(keys);
		this.numberOfKeys = this.keys.size();
	}

	public long getTimestampMin() {
		return this.timestampMin;
	}

	public void setTimestampMin(final long timestampMin) {
		this.timestampMin = timestampMin;
	}

	public long getTimestampMax() {
		return this.timestampMax;
	}

	public void setTimestampMax(final long timestampMax) {
		this.timestampMax = timestampMax;
	}

	public int getPayloadSizeMin() {
		return this.payloadSizeMin;
	}

	public void setPayloadSizeMin(final int payloadSizeMin) {
		this.payloadSizeMin = payloadSizeMin;
	}

	public int getPayloadSizeMax() {
		return this.payloadSizeMax;
	}

	public void setPayloadSizeMax(final int payloadSizeMax) {
		this.payloadSizeMax = payloadSizeMax;
	}

	public void setPayloadSize(final int payloadSize) {
		this.payloadSizeMin = payloadSize;
		this.payloadSizeMax = payloadSize;
	}

	public long getRandomSeed() {
		return this.randomSeed;
	}

	public void setRandomSeed(final long randomSeed) {
		this.randomSeed = randomSeed;
	}

	// =================================================================================================================
	// GENERATOR
	// =================================================================================================================

	public void generate(final File dumpFile) {
		// prepare the DB metadata
		ChronoDBDumpMetadata metadata = new ChronoDBDumpMetadata();
		metadata.setChronosVersion(ChronosVersion.getCurrentVersion());
		metadata.setCreationDate(new Date());
		metadata.getBranchDumpMetadata().add(BranchDumpMetadata.createMasterBranchMetadata());
		// set up the entry generator
		Iterator<ChronoDBDumpEntry<?>> iterator = new RandomEntryGenerator();
		// configure the dump options
		DumpOptions options = new DumpOptions(DumpOption.ENABLE_GZIP);
		// write everything to disk
		try (ObjectOutput output = ChronoDBDumpFormat.createOutput(dumpFile, options)) {
			// include the DB metadata in the output
			output.write(metadata);
			// add the entries to the output
			iterator.forEachRemaining(entry -> output.write(entry));
		} catch (Exception e) {
			throw new AssertionError("Failed to write DB dump!", e);
		}
	}

	private class RandomEntryGenerator implements Iterator<ChronoDBDumpEntry<?>> {

		private int generated = 0;
		private Random random = new Random(RandomTemporalMatrixGenerator.this.randomSeed);
		private List<String> keySet;
		private Kryo kryo;

		private RandomEntryGenerator() {
			this.keySet = Lists.newArrayList();
			if (RandomTemporalMatrixGenerator.this.keys != null) {
				this.keySet.addAll(RandomTemporalMatrixGenerator.this.keys);
			} else {
				// randomly generate the keyset
				for (int i = 0; i < RandomTemporalMatrixGenerator.this.numberOfKeys; i++) {
					String key = UUID.randomUUID().toString();
					this.keySet.add(key);
				}
			}
			this.kryo = new Kryo();
		}

		@Override
		public boolean hasNext() {
			return this.generated < RandomTemporalMatrixGenerator.this.numberOfEntries;
		}

		@Override
		public ChronoDBDumpEntry<?> next() {
			int keyIndex = this.random.nextInt(this.keySet.size());
			String key = this.keySet.get(keyIndex);

			int payloadSize = 0;
			int payloadMin = RandomTemporalMatrixGenerator.this.payloadSizeMin;
			int payloadMax = RandomTemporalMatrixGenerator.this.payloadSizeMax;
			if (payloadMin >= payloadMax) {
				payloadSize = payloadMin;
			} else {
				payloadSize = payloadMin + this.random.nextInt(payloadMax - payloadMin);
			}
			NamedPayload payload = NamedPayload.createKB(key, payloadSize);
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			Output output = new Output(baos);
			this.kryo.writeClassAndObject(output, payload);
			output.flush();
			output.close();
			byte[] value = baos.toByteArray();

			long timestamp = 0;
			long timestampMin = RandomTemporalMatrixGenerator.this.timestampMin;
			long timestampMax = RandomTemporalMatrixGenerator.this.timestampMax;
			if (timestampMin >= timestampMax) {
				timestamp = timestampMin;
			} else {
				timestamp = timestampMin + Math.round(this.random.nextDouble() * (timestampMax - timestampMin));
			}

			String branch = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER;
			String keyspace = ChronoDBConstants.DEFAULT_KEYSPACE_NAME;

			ChronoIdentifier identifier = new ChronoIdentifierImpl(branch, keyspace, key, timestamp);
			ChronoDBDumpEntry<?> entry = new ChronoDBDumpBinaryEntry(identifier, value);
			this.generated++;
			return entry;
		}

	}

}
