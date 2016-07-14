package org.chronos.chronodb.internal.impl.dump;

import static com.google.common.base.Preconditions.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;

import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.BranchManager;
import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.api.ChronoIndexer;
import org.chronos.chronodb.api.IndexManager;
import org.chronos.chronodb.api.SerializationManager;
import org.chronos.chronodb.api.dump.ChronoConverter;
import org.chronos.chronodb.api.dump.annotations.ChronosExternalizable;
import org.chronos.chronodb.api.exceptions.ChronoDBSerializationException;
import org.chronos.chronodb.api.key.ChronoIdentifier;
import org.chronos.chronodb.internal.api.BranchManagerInternal;
import org.chronos.chronodb.internal.api.ChronoDBInternal;
import org.chronos.chronodb.internal.api.stream.ChronoDBEntry;
import org.chronos.chronodb.internal.api.stream.CloseableIterator;
import org.chronos.chronodb.internal.api.stream.ObjectInput;
import org.chronos.chronodb.internal.api.stream.ObjectOutput;
import org.chronos.chronodb.internal.impl.BranchMetadata;
import org.chronos.chronodb.internal.impl.dump.base.ChronoDBDumpElement;
import org.chronos.chronodb.internal.impl.dump.entry.ChronoDBDumpBinaryEntry;
import org.chronos.chronodb.internal.impl.dump.entry.ChronoDBDumpEntry;
import org.chronos.chronodb.internal.impl.dump.entry.ChronoDBDumpPlainEntry;
import org.chronos.chronodb.internal.impl.dump.meta.BranchDumpMetadata;
import org.chronos.chronodb.internal.impl.dump.meta.ChronoDBDumpMetadata;
import org.chronos.chronodb.internal.impl.dump.meta.IndexerDumpMetadata;
import org.chronos.common.logging.ChronoLogger;
import org.chronos.common.util.ReflectionUtils;
import org.chronos.common.version.ChronosVersion;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;

public class ChronoDBDumpUtil {

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	public static void dumpDBContentsToOutput(final ChronoDBInternal db, final ObjectOutput output,
			final DumpOptions options) {
		checkNotNull(db, "Precondition violation - argument 'db' must not be NULL!");
		checkNotNull(output, "Precondition violation - argument 'output' must not be NULL!");
		checkNotNull(options, "Precondition violation - argument 'options' must not be NULL!");
		// stream out data using a sequence writer that fills a root array
		try {
			// calculate the metadata we need to write as the first object
			ChronoDBDumpMetadata dbMetadata = extractMetadata(db);
			// write the DB data into the dump file
			output.write(dbMetadata);
			// set up some caches and variables we are going to need later
			SerializationManager sm = db.getSerializationManager();
			boolean forceBinary = options.isForceBinaryEncodingEnabled();
			ConverterRegistry converters = new ConverterRegistry(options);
			// now, stream in the entries from the database
			try (CloseableIterator<ChronoDBEntry> entryStream = db.entryStream()) {
				while (entryStream.hasNext()) {
					ChronoDBEntry entry = entryStream.next();
					// convert the entry to the dump entry, depending on the settings
					ChronoDBDumpEntry<?> dumpEntry = null;
					if (forceBinary) {
						dumpEntry = convertToBinaryEntry(entry);
					} else {
						dumpEntry = convertToDumpEntry(entry, sm, converters);
					}
					// write our entry into the dump
					output.write(dumpEntry);
				}
			}
		} catch (Exception e) {
			ChronoLogger.logError("Failed to write Chronos DB Dump!", e);
		} finally {
			output.close();
		}
	}

	public static void readDumpContentsFromInput(final ChronoDBInternal db, final ObjectInput input,
			final DumpOptions options) {
		checkNotNull(db, "Precondition violation - argument 'db' must not be NULL!");
		checkNotNull(input, "Precondition violation - argument 'input' must not be NULL!");
		checkNotNull(options, "Precondition violation - argument 'options' must not be NULL!");
		try {
			// create the converter registry based on the options
			ConverterRegistry converters = new ConverterRegistry(options);
			// the first element should ALWAYS be the metadata
			ChronoDBDumpMetadata metadata = readMetadata(input);
			// with the metadata, we set up the branches
			createBranches(db, metadata);
			// load the elements
			loadEntries(db, input, converters, options);
			// set up the indexers
			setupIndexersAndReindex(db, metadata);
		} catch (Exception e) {
			ChronoLogger.logError("Failed to load DB dump!", e);
		}

	}

	public static ChronoConverter<?, ?> getAnnotatedConverter(final Object value) {
		ChronosExternalizable annotation = value.getClass().getAnnotation(ChronosExternalizable.class);
		if (annotation == null) {
			return null;
		}
		Class<? extends ChronoConverter<?, ?>> converterClass = annotation.converterClass();
		try {
			return ReflectionUtils.instantiate(converterClass);
		} catch (Exception e) {
			ChronoLogger.logWarning("Could not instantiate Plain-Text-Converter class '" + converterClass.getName()
					+ "' for annotated class '" + value.getClass().getName() + "'. Falling back to binary.");
			return null;
		}
	}

	public static boolean isWellKnownObject(final Object object) {
		if (object == null) {
			return true;
		}
		if (ReflectionUtils.isPrimitiveOrWrapperClass(object.getClass())) {
			// we support all primitives and their wrapper classes as well-known classes
			return true;
		}
		if (object instanceof String) {
			// we treat string as well-known class
			return true;
		}
		if (object.getClass().isEnum()) {
			// we treat enums as well-known elements
			return true;
		}
		if (object instanceof Collection<?>) {
			Collection<?> collection = (Collection<?>) object;
			// check if it's a well-known collection class
			if (isWellKnownCollectionClass(collection.getClass()) == false) {
				// nope, we don't know this collection class
				return false;
			}
			// make sure that all members are of well-known types
			for (Object element : collection) {
				if (isWellKnownObject(element) == false) {
					// a non-well-known element resides in the collection -> the object as a whole is not well-known
					return false;
				}
			}
			// we know the collection type, and all elements (recursively) -> the object is well-known
			return true;
		}
		if (object instanceof Map<?, ?>) {
			Map<?, ?> map = (Map<?, ?>) object;
			if (isWellKnownMapClass(map.getClass()) == false) {
				// we don't know this map class
				return false;
			}
			// make sure that all entries are of well-known types
			for (Entry<?, ?> entry : map.entrySet()) {
				if (isWellKnownObject(entry.getKey()) == false) {
					return false;
				}
				if (isWellKnownObject(entry.getValue()) == false) {
					return false;
				}
			}
			// we know the map type, and all elements (recursively) -> the object is well-known
			return true;
		}
		// in any other case, the object is no well-known type
		return false;
	}

	private static boolean isWellKnownCollectionClass(final Class<?> clazz) {
		if (clazz.equals(HashSet.class)) {
			return true;
		} else if (clazz.equals(ArrayList.class)) {
			return true;
		} else if (clazz.equals(LinkedList.class)) {
			return true;
		}
		// unknown collection type
		return false;
	}

	private static boolean isWellKnownMapClass(final Class<?> clazz) {
		if (clazz.equals(HashMap.class)) {
			return true;
		}
		// unknown map type
		return false;
	}

	// =====================================================================================================================
	// SERIALIZATION / DUMP WRITE METHODS
	// =====================================================================================================================

	private static ChronoDBDumpMetadata extractMetadata(final ChronoDBInternal db) {
		checkNotNull(db, "Precondition violation - argument 'db' must not be NULL!");
		ChronoDBDumpMetadata dbDumpMetadata = new ChronoDBDumpMetadata();
		dbDumpMetadata.setCreationDate(new Date());
		dbDumpMetadata.setChronosVersion(ChronosVersion.getCurrentVersion());
		// copy branch metadata
		BranchManager branchManager = db.getBranchManager();
		for (Branch branch : branchManager.getBranches()) {
			BranchDumpMetadata branchDump = new BranchDumpMetadata(branch);
			dbDumpMetadata.getBranchDumpMetadata().add(branchDump);
		}
		// copy indexer metadata
		IndexManager indexManager = db.getIndexManager();
		Map<String, Set<ChronoIndexer>> indexersByIndexName = indexManager.getIndexersByIndexName();
		for (Entry<String, Set<ChronoIndexer>> indexNameToIndexers : indexersByIndexName.entrySet()) {
			String indexName = indexNameToIndexers.getKey();
			Set<ChronoIndexer> indexers = indexNameToIndexers.getValue();
			for (ChronoIndexer indexer : indexers) {
				IndexerDumpMetadata indexDump = new IndexerDumpMetadata(indexName, indexer);
				dbDumpMetadata.getIndexerDumpMetadata().add(indexDump);
			}
		}
		return dbDumpMetadata;
	}

	private static ChronoDBDumpBinaryEntry convertToBinaryEntry(final ChronoDBEntry entry) {
		return new ChronoDBDumpBinaryEntry(entry.getIdentifier(), entry.getValue());
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static ChronoDBDumpEntry<?> convertToDumpEntry(final ChronoDBEntry entry,
			final SerializationManager serializationManager, final ConverterRegistry converters) {
		ChronoDBDumpEntry<?> dumpEntry;
		byte[] serializedValue = entry.getValue();
		if (serializedValue == null || serializedValue.length < 1) {
			// this entry is a deletion and has no value; it makes no difference if we
			// write it out as a serialized entry or a plain-text entry.
			dumpEntry = convertToBinaryEntry(entry);
		} else {
			// deserialize the value and check if it is plain-text enabled
			Object deserializedValue = serializationManager.deserialize(serializedValue);
			ChronoConverter converter = converters.getConverterForObject(deserializedValue);
			// if we have a plain text converter, use it
			Object externalRepresentation = null;
			if (converter != null) {
				try {
					externalRepresentation = converter.writeToOutput(deserializedValue);
					if (externalRepresentation == null) {
						ChronoLogger.logError("Plain text converter '" + converter.getClass()
								+ "' produced NULL! Falling back to binary.");
						// set the plain text to NULL, in case that it was empty
						externalRepresentation = null;
						converter = null;
					} else {
						// we successfully converted the entry. We can discard the converter
						// in the output if we used a default converter.
						if (converters.isDefaultConverter(converter)) {
							// it's a default converter, don't include it in the output
							converter = null;
						}

					}
				} catch (Exception e) {
					ChronoLogger.logError("Chrono converter '" + converter.getClass()
							+ "' produced an error! Falling back to binary. Exception is: " + e.toString());
				}
			} else {
				// we did not find an explicit converter; maybe it's a well-known object?
				if (isWellKnownObject(deserializedValue)) {
					// it's a well-known object, we can use it directly as external representation
					externalRepresentation = deserializedValue;
					converter = null;
				}
			}
			// see if the plain text conversion worked
			if (externalRepresentation != null) {
				// conversion was okay; use it
				dumpEntry = new ChronoDBDumpPlainEntry(entry.getIdentifier(), externalRepresentation, converter);
			} else {
				// plain text conversion failed, fall back to binary
				dumpEntry = convertToBinaryEntry(entry);
			}
		}
		return dumpEntry;
	}

	// =====================================================================================================================
	// DESERIALIZATION / DUMP READ METHODS
	// =====================================================================================================================

	private static ChronoDBDumpMetadata readMetadata(final ObjectInput input) {
		checkNotNull(input, "Precondition violation - argument 'input' must not be NULL!");
		if (input.hasNext() == false) {
			throw new ChronoDBSerializationException("Failed to read dump - metadata entry is missing!");
		}
		ChronoDBDumpElement element = (ChronoDBDumpElement) input.next();
		if (element instanceof ChronoDBDumpMetadata == false) {
			throw new ChronoDBSerializationException("Failed to read dump - metadata entry is missing!");
		}
		return (ChronoDBDumpMetadata) element;
	}

	private static void createBranches(final ChronoDBInternal db, final ChronoDBDumpMetadata metadata) {
		checkNotNull(db, "Precondition violation - argument 'db' must not be NULL!");
		checkNotNull(metadata, "Precondition violation - argument 'metadata' must not be NULL!");
		BranchManagerInternal branchManager = db.getBranchManager();
		Set<BranchDumpMetadata> branchDumpMetadata = metadata.getBranchDumpMetadata();
		// strategy note:
		// In order to achieve the loading of branch data, we need to feed the branches into
		// branchManager.loadBranchDataFromDump(...). However, the ordering matters in this case,
		// because we can only create a 'real' branch if the parent branch already exists in the
		// system. We therefore need to create a sorted list of branches such that the parent
		// branch is always loaded before the child branch. We do this by calculating a mapping
		// from parent to child branches, then recursively iterating over the structure using
		// a breadth-first-search, and insert the encountered branches into a list.
		BranchDumpMetadata masterDumpBranch = null;
		// branch name to sub-branches
		SetMultimap<String, BranchDumpMetadata> subBranches = HashMultimap.create();
		// branch name to metadata
		Map<String, BranchDumpMetadata> branchByName = Maps.newHashMap();
		for (BranchDumpMetadata branch : branchDumpMetadata) {
			if (branch.getParentName() == null) {
				// we have found the master branch
				if (branch.getName().equals(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER) == false) {
					// a branch without a parent that is not the master branch...?
					throw new IllegalStateException(
							"Found branch that has no parent, but is called '" + branch.getName()
									+ "' (expected name: '" + ChronoDBConstants.MASTER_BRANCH_IDENTIFIER + "')!");
				}
				if (masterDumpBranch != null) {
					// the master branch must be unique... how did this even happen?
					throw new IllegalStateException(
							"Found multiple branches without parent (only master branch may have no parent)! Encountered branches: '"
									+ masterDumpBranch.getName() + "', '" + branch.getName() + "'");
				}
				masterDumpBranch = branch;
			} else {
				// we are dealing with a regular branch
				subBranches.put(branch.getParentName(), branch);
			}
			// in any case, remember the branch by name in our map
			branchByName.put(branch.getName(), branch);
		}
		// convert to "real" branch objects
		BranchMetadata master = BranchMetadata.createMasterBranchMetadata();
		List<BranchMetadata> loadedBranches = Lists.newArrayList();
		Stack<BranchMetadata> branchesToVisit = new Stack<BranchMetadata>();
		// start the conversion at the master branch
		branchesToVisit.push(master);
		while (branchesToVisit.isEmpty() == false) {
			BranchMetadata currentBranch = branchesToVisit.pop();
			Set<BranchDumpMetadata> childDumpBranches = subBranches.get(currentBranch.getName());
			for (BranchDumpMetadata childDumpBranch : childDumpBranches) {
				String childBranchName = childDumpBranch.getName();
				long branchingTimestamp = childDumpBranch.getBranchingTimestamp();
				// create the child
				BranchMetadata childBranchMetadata = new BranchMetadata(childBranchName, currentBranch.getName(),
						branchingTimestamp);
				// remember to visit this child to create its children
				branchesToVisit.push(childBranchMetadata);
				// remember that we created this child
				loadedBranches.add(childBranchMetadata);
			}
		}
		// load the branch data into the DB system
		branchManager.loadBranchDataFromDump(loadedBranches);
	}

	private static void loadEntries(final ChronoDBInternal db, final ObjectInput input,
			final ConverterRegistry converters, final DumpOptions options) {
		checkNotNull(db, "Precondition violation - argument 'db' must not be NULL!");
		checkNotNull(input, "Precondition violation - argument 'input' must not be NULL!");
		checkNotNull(converters, "Precondition violation - argument 'converters' must not be NULL!");
		checkNotNull(options, "Precondition violation - argument 'options' must not be NULL!");
		SerializationManager sm = db.getSerializationManager();
		// this is our read batch. We fill it one by one, when it's full, we load that batch into the DB.
		List<ChronoDBEntry> readBatch = Lists.newArrayList();
		int batchSize = options.getBatchSize();
		while (input.hasNext()) {
			ChronoDBDumpElement element = (ChronoDBDumpElement) input.next();
			// this element should be an entry...
			if (element instanceof ChronoDBDumpEntry == false) {
				// hm... no idea what this could be.
				ChronoLogger.logError("Encountered unexpected element of type '" + element.getClass().getName()
						+ "', expected '" + ChronoDBDumpEntry.class.getName() + "'. Skipping this entry.");
				continue;
			}
			// cast down to the entry and check what it is
			ChronoDBDumpEntry<?> dumpEntry = (ChronoDBDumpEntry<?>) element;
			ChronoDBEntry entry = convertDumpEntryToDBEntry(dumpEntry, sm, converters);
			readBatch.add(entry);
			// check if we need to flush our read batch into the DB
			if (readBatch.size() >= batchSize) {
				ChronoLogger.log("Reading a batch of size " + batchSize);
				db.loadEntries(readBatch);
				readBatch.clear();
			}
		}
		// we are at the end of the input; flush the remaining buffer (if any)
		if (readBatch.isEmpty() == false) {
			db.loadEntries(readBatch);
			readBatch.clear();
		}
	}

	private static ChronoDBEntry convertDumpEntryToDBEntry(final ChronoDBDumpEntry<?> dumpEntry,
			final SerializationManager serializationManager, final ConverterRegistry converters) {
		checkNotNull(dumpEntry, "Precondition violation - argument 'dumpEntry' must not be NULL!");
		checkNotNull(serializationManager,
				"Precondition violation - argument 'serializationManager' must not be NULL!");
		checkNotNull(converters, "Precondition violation - argument 'converters' must not be NULL!");
		try {
			if (dumpEntry instanceof ChronoDBDumpBinaryEntry) {
				return convertBinaryDumpEntryToDBEntry(dumpEntry);
			} else if (dumpEntry instanceof ChronoDBDumpPlainEntry) {
				return convertPlainTextDumpEntryToDBEntry(dumpEntry, serializationManager, converters);
			} else {
				// can this even happen?
				return null;
			}
		} catch (Exception e) {
			ChronoLogger.logError("Failed to read dump entry!", e);
			return null;
		}
	}

	private static ChronoDBEntry convertBinaryDumpEntryToDBEntry(final ChronoDBDumpEntry<?> dumpEntry) {
		ChronoDBDumpBinaryEntry binaryEntry = (ChronoDBDumpBinaryEntry) dumpEntry;
		ChronoIdentifier identifier = binaryEntry.getChronoIdentifier();
		byte[] value = binaryEntry.getValue();
		return ChronoDBEntry.create(identifier, value);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static ChronoDBEntry convertPlainTextDumpEntryToDBEntry(final ChronoDBDumpEntry<?> dumpEntry,
			final SerializationManager serializationManager, final ConverterRegistry converters) throws Exception {
		ChronoDBDumpPlainEntry plainEntry = (ChronoDBDumpPlainEntry) dumpEntry;
		String converterClassName = plainEntry.getConverterClassName();
		ChronoConverter<?, ?> converter = null;
		if (converterClassName != null) {
			// look for the explicit converter
			converter = converters.getConverterByClassName(converterClassName);
			if (converter == null) {
				ChronoLogger.logError(
						"Failed to instantiate plain text converter '" + converterClassName + "'! Skipping entry!");
				return null;
			}
		}
		if (converter == null) {
			// check if we have a default converter
			converter = converters.getConverterForObject(plainEntry.getValue());
		}
		Object deserializedValue = null;
		if (converter != null) {
			// use the converter
			deserializedValue = ((ChronoConverter) converter).readFromInput(plainEntry.getValue());
		} else {
			// well-known objects don't have/need a converter.
			deserializedValue = plainEntry.getValue();
		}
		byte[] serializedValue = serializationManager.serialize(deserializedValue);
		return ChronoDBEntry.create(plainEntry.getChronoIdentifier(), serializedValue);
	}

	private static void setupIndexersAndReindex(final ChronoDBInternal db, final ChronoDBDumpMetadata metadata) {
		checkNotNull(db, "Precondition violation - argument 'db' must not be NULL!");
		checkNotNull(metadata, "Precondition violation - argument 'metadata' must not be NULL!");
		IndexManager indexManager = db.getIndexManager();
		Set<IndexerDumpMetadata> indexerDumpMetadata = metadata.getIndexerDumpMetadata();
		// insert the indexers, one by one
		for (IndexerDumpMetadata indexerMetadata : indexerDumpMetadata) {
			ChronoIndexer indexer = indexerMetadata.getIndexer();
			String name = indexerMetadata.getIndexName();
			if (indexer == null) {
				ChronoLogger.logError("Failed to reconstruct index '" + name + "' because indexer is unavailable"
						+ " - skipping it!");
				continue;
			}
			indexManager.addIndexer(name, indexer);
		}
		// reconstruct the index
		indexManager.reindexAll();
	}
}
