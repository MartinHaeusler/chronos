package org.chronos.benchmarks.util;

import static com.google.common.base.Preconditions.*;

import java.io.File;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;

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
import org.chronos.common.serialization.KryoManager;
import org.chronos.common.version.ChronosVersion;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

public class BenchmarkUtils {

	public static Set<String> randomKeySet(final long size) {
		Set<String> keySet = Sets.newHashSet();
		for (int i = 0; i < size; i++) {
			keySet.add(UUID.randomUUID().toString());
		}
		return keySet;
	}

	public static List<String> randomKeySetAsList(final long size) {
		List<String> keys = Lists.newArrayList();
		for (int i = 0; i < size; i++) {
			keys.add(UUID.randomUUID().toString());
		}
		return keys;
	}

	public static RandomDatasetBuilder createRandomDataset(final int entries, final int keySetSize) {
		checkArgument(entries > 0, "Precondition violation - argument 'entries' must be > 0!");
		checkArgument(keySetSize > 0, "Precondition violation - argument 'keySetSize' must be > 0!");
		checkArgument(entries >= keySetSize,
				"Precondition violation - argument 'entries' must be >= argument 'keySetSize'!");
		return new RandomDatasetBuilder(entries, keySetSize);
	}

	public static int randomBetween(final int lower, final int upper) {
		if (lower > upper) {
			throw new IllegalArgumentException("lower > upper: " + lower + " > " + upper);
		}
		if (lower == upper) {
			return lower;
		}
		double random = Math.random();
		return (int) (lower + Math.round((upper - lower) * random));
	}

	public static long randomBetween(final long lower, final long upper) {
		if (lower > upper) {
			throw new IllegalArgumentException("lower > upper: " + lower + " > " + upper);
		}
		if (lower == upper) {
			return lower;
		}
		double random = Math.random();
		return lower + Math.round((upper - lower) * random);
	}

	public static double lerp(final double lower, final double upper, final double percent) {
		return lower + percent * (upper - lower);
	}

	public static <T> List<T> generateValuesList(final Supplier<T> factory, final int size) {
		return generateValues(factory, Lists.newArrayList(), size);
	}

	public static <T> Set<T> generateValuesSet(final Supplier<T> factory, final int size) {
		return generateValues(factory, Sets.newHashSet(), size);
	}

	public static <T, C extends Collection<T>> C generateValues(final Supplier<T> factory, final C collection,
			final int size) {
		for (int i = 0; i < size; i++) {
			T element = factory.get();
			collection.add(element);
		}
		return collection;
	}

	public static <T> T getRandomEntryOf(final List<T> list) {
		if (list == null) {
			throw new NullPointerException("Precondition violation - argument 'list' must not be NULL!");
		}
		if (list.isEmpty()) {
			throw new NoSuchElementException("List is empty, cannot get random entry!");
		}
		int index = randomBetween(0, list.size() - 1);
		return list.get(index);
	}

	public static void writeDBDump(final File file, final Iterable<ChronoDBDumpEntry<?>> data) {
		checkNotNull(file, "Precondition violation - argument 'file' must not be NULL!");
		checkNotNull(data, "Precondition violation - argument 'data' must not be NULL!");
		writeDBDump(file, data.iterator());
	}

	public static void writeDBDump(final File file, final Iterator<ChronoDBDumpEntry<?>> data) {
		checkNotNull(file, "Precondition violation - argument 'file' must not be NULL!");
		checkNotNull(data, "Precondition violation - argument 'data' must not be NULL!");
		// prepare the DB metadata
		ChronoDBDumpMetadata metadata = new ChronoDBDumpMetadata();
		metadata.setChronosVersion(ChronosVersion.getCurrentVersion());
		metadata.setCreationDate(new Date());
		metadata.getBranchDumpMetadata().add(BranchDumpMetadata.createMasterBranchMetadata());
		writeDBDump(file, metadata, data);
	}

	public static void writeDBDump(final File file, final ChronoDBDumpMetadata metadata,
			final Iterable<ChronoDBDumpEntry<?>> data) {
		checkNotNull(file, "Precondition violation - argument 'file' must not be NULL!");
		checkNotNull(metadata, "Precondition violation - argument 'metadata' must not be NULL!");
		checkNotNull(data, "Precondition violation - argument 'data' must not be NULL!");
		writeDBDump(file, metadata, data.iterator());
	}

	public static void writeDBDump(final File file, final ChronoDBDumpMetadata metadata,
			final Iterator<ChronoDBDumpEntry<?>> data) {
		checkNotNull(file, "Precondition violation - argument 'file' must not be NULL!");
		checkNotNull(metadata, "Precondition violation - argument 'metadata' must not be NULL!");
		checkNotNull(data, "Precondition violation - argument 'data' must not be NULL!");
		// configure the dump options
		DumpOptions options = new DumpOptions(DumpOption.ENABLE_GZIP);
		// write everything to disk
		try (ObjectOutput output = ChronoDBDumpFormat.createOutput(file, options)) {
			// include the DB metadata in the output
			output.write(metadata);
			// add the entries to the output
			data.forEachRemaining(entry -> output.write(entry));
		} catch (Exception e) {
			throw new AssertionError("Failed to write DB dump!", e);
		}
	}

	public static class RandomDatasetBuilder {

		private final int entries;
		private final int keySetSize;

		private Function<Integer, String> keyGeneratorFunction;
		private Function<String, Object> valueGeneratorFunction;
		private Function<Object, byte[]> serializerFunction;
		private Function<String, String> branchSelectorFunction;
		private Function<String, String> keyspaceSelectorFunction;
		private long timestampFrom;
		private long timestampTo;

		public RandomDatasetBuilder(final int entries, final int keySetSize) {
			checkArgument(entries > 0, "Precondition violation - argument 'entries' must be > 0!");
			checkArgument(keySetSize > 0, "Precondition violation - argument 'keySetSize' must be > 0!");
			checkArgument(entries >= keySetSize,
					"Precondition violation - argument 'entries' must be >= argument 'keySetSize'!");
			this.entries = entries;
			this.keySetSize = keySetSize;

			// DEFAULT VALUES
			this.timestampFrom = 0;
			this.timestampTo = System.currentTimeMillis();
			// use the kryo manager for serialization
			this.serializerFunction = KryoManager::serialize;
			// use random UUIDs as keys
			this.keyGeneratorFunction = (keyIndex) -> UUID.randomUUID().toString();
			// use random doubles as values
			this.valueGeneratorFunction = (key) -> Math.random();
			// put all keys into the master branch
			this.branchSelectorFunction = (key) -> ChronoDBConstants.MASTER_BRANCH_IDENTIFIER;
			// put all keys into the default keyspace
			this.keyspaceSelectorFunction = (key) -> ChronoDBConstants.DEFAULT_KEYSPACE_NAME;
		}

		public RandomDatasetBuilder keyGenerator(final Function<Integer, String> keyGeneratorFunction) {
			checkNotNull(keyGeneratorFunction,
					"Precondition violation - argument 'keyGeneratorFunction' must not be NULL!");
			this.keyGeneratorFunction = keyGeneratorFunction;
			return this;
		}

		public RandomDatasetBuilder keyToValue(final Function<String, Object> valueGeneratorFunction) {
			checkNotNull(valueGeneratorFunction,
					"Precondition violation - argument 'valueGeneratorFunction' must not be NULL!");
			this.valueGeneratorFunction = valueGeneratorFunction;
			return this;
		}

		public RandomDatasetBuilder timestampRange(final long timestampFrom, final long timestampTo) {
			checkArgument(timestampFrom >= 0,
					"Precondition violation - argument 'timestampFrom' must not be negative!");
			checkArgument(timestampTo >= 0, "Precondition violation - argument 'timestampTo' must not be negative!");
			checkArgument(timestampFrom <= timestampTo,
					"Precondition violation - argument 'timestampFrom' must be <= argument 'timestampTo'!");
			this.timestampFrom = timestampFrom;
			this.timestampTo = timestampTo;
			return this;
		}

		public RandomDatasetBuilder serializer(final Function<Object, byte[]> serializer) {
			checkNotNull(serializer, "Precondition violation - argument 'serializer' must not be NULL!");
			this.serializerFunction = serializer;
			return this;
		}

		public RandomDatasetBuilder keyToBranch(final Function<String, String> keyToBranchFunction) {
			checkNotNull(keyToBranchFunction,
					"Precondition violation - argument 'keyToBranchFunction' must not be NULL!");
			this.branchSelectorFunction = keyToBranchFunction;
			return this;
		}

		public RandomDatasetBuilder keyToKeyspace(final Function<String, String> keyToKeyspaceFunction) {
			checkNotNull(keyToKeyspaceFunction,
					"Precondition violation - argument 'keyToKeyspaceFunction' must not be NULL!");
			this.keyspaceSelectorFunction = keyToKeyspaceFunction;
			return this;
		}

		public Set<ChronoDBDumpEntry<?>> build() {
			return this.build(Sets.newHashSet());
		}

		public List<ChronoDBDumpEntry<?>> buildList() {
			return this.build(Lists.newArrayList());
		}

		public <T extends Collection<ChronoDBDumpEntry<?>>> T build(final T collectionToFill) {
			// generate the key set
			Set<String> keyList = this.buildKeySet();
			SetMultimap<String, Long> keyToTimestamps = HashMultimap.create();
			// this iterator cycles through the key list indefinitly
			Iterator<String> keyCycleIterator = Iterators.cycle(keyList);
			for (int entryIndex = 0; entryIndex < this.entries; entryIndex++) {
				// get the key
				String key = keyCycleIterator.next();
				// generate a random timestamp that was not yet used for the given key
				long timestamp = randomBetween(this.timestampFrom, this.timestampTo);
				while (keyToTimestamps.containsEntry(key, timestamp)) {
					timestamp = randomBetween(this.timestampFrom, this.timestampTo);
				}
				// remember that we have this combination
				keyToTimestamps.put(key, timestamp);
				// choose the branch and keyspace
				String branch = this.branchSelectorFunction.apply(key);
				if (branch == null || branch.trim().isEmpty()) {
					branch = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER;
				}
				String keyspace = this.keyspaceSelectorFunction.apply(key);
				if (keyspace == null || keyspace.trim().isEmpty()) {
					branch = ChronoDBConstants.DEFAULT_KEYSPACE_NAME;
				}
				// create the identifier
				ChronoIdentifier identifier = new ChronoIdentifierImpl(branch, keyspace, key, timestamp);
				// create the value
				Object value = this.valueGeneratorFunction.apply(key);
				// serialize the value
				byte[] serializedValue = this.serializerFunction.apply(value);
				// create the entry
				ChronoDBDumpEntry<?> entry = new ChronoDBDumpBinaryEntry(identifier, serializedValue);
				// ... and add it to our set
				collectionToFill.add(entry);
			}
			return collectionToFill;
		}

		private Set<String> buildKeySet() {
			Set<String> keySet = Sets.newHashSet();
			for (int keyIndex = 0; keyIndex < this.keySetSize; keyIndex++) {
				String key = this.keyGeneratorFunction.apply(keyIndex);
				keySet.add(key);
			}
			return keySet;
		}
	}

}
