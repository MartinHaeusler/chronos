package org.chronos.chronodb.api.dump;

import static com.google.common.base.Preconditions.*;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.DumpOption;
import org.chronos.chronodb.api.exceptions.ChronoDBStorageBackendException;
import org.chronos.chronodb.api.key.ChronoIdentifier;
import org.chronos.chronodb.internal.api.stream.ObjectInput;
import org.chronos.chronodb.internal.api.stream.ObjectOutput;
import org.chronos.chronodb.internal.impl.dump.DumpOptions;
import org.chronos.chronodb.internal.impl.dump.entry.ChronoDBDumpBinaryEntry;
import org.chronos.chronodb.internal.impl.dump.entry.ChronoDBDumpPlainEntry;
import org.chronos.chronodb.internal.impl.dump.meta.BranchDumpMetadata;
import org.chronos.chronodb.internal.impl.dump.meta.ChronoDBDumpMetadata;
import org.chronos.chronodb.internal.impl.dump.meta.IndexerDumpMetadata;
import org.chronos.chronodb.internal.impl.temporal.ChronoIdentifierImpl;
import org.chronos.chronodb.internal.util.ChronosFileUtils;
import org.chronos.common.logging.ChronoLogger;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.thoughtworks.xstream.XStream;

/**
 * This class describes the file format for {@link ChronoDB#writeDump(File, DumpOption...) DB dumps}.
 *
 * <p>
 * This class serves two major purposes:
 * <ul>
 * <li>Offering constants and a default configuration for the file format
 * <li>Creating {@link ObjectInput input} and {@link ObjectOutput output} streams
 * </ul>
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public class ChronoDBDumpFormat {

	/** The default alias name for the {@link ChronoDBDumpMetadata} class. */
	public static String ALIAS_NAME__CHRONODB_METADATA = "dbmetadata";
	/** The default alias name for the {@link ChronoDBDumpPlainEntry} class. */
	public static String ALIAS_NAME__CHRONODB_PLAIN_ENTRY = "dbentryPlain";
	/** The default alias name for the {@link ChronoDBDumpBinaryEntry} class. */
	public static String ALIAS_NAME__CHRONODB_BINARY_ENTRY = "dbentryBinary";
	/** The default alias name for the {@link ChronoIdentifier} class. */
	public static String ALIAS_NAME__CHRONO_IDENTIFIER = "chronoIdentifier";
	/** The default alias name for the {@link BranchDumpMetadata} class. */
	public static String ALIAS_NAME__BRANCH_DUMP_METADATA = "branch";
	/** The default alias name for the {@link IndexerDumpMetadata} class. */
	public static String ALIAS_NAME__INDEXER_DUMP_METADATA = "indexer";

	/** The default alias definition for the {@link ChronoDBDumpMetadata} class. */
	public static Alias ALIAS__CHRONODB_METADATA = new Alias(ChronoDBDumpMetadata.class, ALIAS_NAME__CHRONODB_METADATA);
	/** The default alias definition for the {@link ChronoDBDumpPlainEntry} class. */
	public static Alias ALIAS__CHRONODB_PLAIN_ENTRY = new Alias(ChronoDBDumpPlainEntry.class,
			ALIAS_NAME__CHRONODB_PLAIN_ENTRY);
	/** The default alias definition for the {@link ChronoDBDumpBinaryEntry} class. */
	public static Alias ALIAS__CHRONODB_BINARY_ENTRY = new Alias(ChronoDBDumpBinaryEntry.class,
			ALIAS_NAME__CHRONODB_BINARY_ENTRY);
	/** The default alias definition for the {@link ChronoIdentifierImpl} class. */
	public static Alias ALIAS__CHRONO_IDENTIFIER = new Alias(ChronoIdentifierImpl.class, ALIAS_NAME__CHRONO_IDENTIFIER);
	/** The default alias definition for the {@link BranchDumpMetadata} class. */
	public static Alias ALIAS__CHRONO_DUMP_METADATA = new Alias(BranchDumpMetadata.class,
			ALIAS_NAME__BRANCH_DUMP_METADATA);
	/** The default alias definition for the {@link IndexerDumpMetadata} class. */
	public static Alias ALIAS__INDEXER_DUMP = new Alias(IndexerDumpMetadata.class, ALIAS_NAME__INDEXER_DUMP_METADATA);

	// =====================================================================================================================
	// PUBLIC API METHODS
	// =====================================================================================================================

	/**
	 * Returns the set of default aliases for DB dumps.
	 *
	 * @return the set of default aliases. May be empty, but never <code>null</code>.
	 */
	public static Set<Alias> getAliases() {
		Set<Alias> aliases = Sets.newHashSet();
		Field[] fields = ChronoDBDumpFormat.class.getDeclaredFields();
		for (Field field : fields) {
			if (Modifier.isStatic(field.getModifiers()) == false) {
				continue;
			}
			if (field.getType().equals(Alias.class) == false) {
				continue;
			}
			try {
				Alias value = (Alias) field.get(null);
				aliases.add(value);
			} catch (IllegalArgumentException | IllegalAccessException e) {
				ChronoLogger.logError("Failed to collect aliases", e);
			}
		}
		return aliases;
	}

	/**
	 * Returns the set of default aliases, as a map from alias name to "aliased" class.
	 *
	 * @return The mapping of default aliases (alias name to class). May be empty, but never <code>null</code>.
	 */
	public static Map<String, Class<?>> getAliasesAsMap() {
		Set<Alias> aliases = getAliases();
		Map<String, Class<?>> resultMap = Maps.newHashMap();
		for (Alias alias : aliases) {
			Class<?> previousValue = resultMap.put(alias.getAliasName(), alias.getType());
			if (previousValue != null) {
				throw new IllegalStateException("Multiple classes share the alias '" + alias.getAliasName() + "': "
						+ previousValue.getName() + ", " + alias.getType().getName());
			}
		}
		return resultMap;
	}

	/**
	 * Creates a default {@link ObjectOutput} for {@linkplain ChronoDB#writeDump(File, DumpOption...) DB dumping} .
	 *
	 * <p>
	 * It is <b>strongly</b> recommended (and in some cases required) to use the same options for writing and reading a
	 * dump file!
	 *
	 * @param outputFile
	 *            The file to store the dump data. Must not be <code>null</code>, must refer to a file. Will be created
	 *            if it does not exist. Will be overwritten without further notice.
	 * @param options
	 *            The options to use when writing the dump. Must not be <code>null</code>, may be empty.
	 * @return The object output. Must be closed explicitly by the caller. Never <code>null</code>.
	 *
	 * @throws ChronoDBStorageBackendException
	 *             When an I/O related error occurs.
	 */
	public static ObjectOutput createOutput(final File outputFile, final DumpOptions options) {
		checkNotNull(outputFile, "Precondition violation - argument 'outputFile' must not be NULL!");
		checkNotNull(options, "Precondition violation - argument 'options' must not be NULL!");
		if (outputFile.exists()) {
			checkArgument(outputFile.isFile(),
					"Precondition violation - argument 'outputFile' must be a file (not a directory)!");
		} else {
			try {
				outputFile.getParentFile().mkdirs();
				outputFile.createNewFile();
			} catch (IOException e) {
				throw new ChronoDBStorageBackendException(
						"Failed to create dump file in '" + outputFile.getAbsolutePath() + "'!", e);
			}
		}
		// initialize the stream
		XStream xStream = createXStream(options);
		try {
			OutputStream outputStream = new FileOutputStream(outputFile);
			if (options.isGZipEnabled()) {
				// wrap the output stream in a GZIP output stream
				outputStream = new GZIPOutputStream(outputStream);
			}
			// create the raw object output stream
			ObjectOutputStream oos = xStream.createObjectOutputStream(outputStream, "chronodump");
			// wrap it into an object output
			ObjectOutputStreamWrapper wrapper = new ObjectOutputStreamWrapper(oos);
			return wrapper;
		} catch (IOException e) {
			throw new ChronoDBStorageBackendException("Failed to open output stream for writing!", e);
		}
	}

	/**
	 * Creates a default {@link ObjectInput} for {@linkplain ChronoDB#readDump(File, DumpOption...) reading DB dump
	 * files}.
	 *
	 * <p>
	 * It is <b>strongly</b> recommended (and in some cases required) to use the same options for writing and reading a
	 * dump file!
	 *
	 * @param inputFile
	 *            The dump file to read. Must not be <code>null</code>. Must point to a file (not a directory). Must
	 *            point to an existing file.
	 * @param options
	 *            The options to use when reading the dump data. Must not be <code>null</code>.
	 * @return The object input. Must be closed explicitly by the caller. Never <code>null</code>.
	 *
	 * @throws ChronoDBStorageBackendException
	 *             When an I/O related error occurs.
	 */
	public static ObjectInput createInput(final File inputFile, final DumpOptions options) {
		checkNotNull(inputFile, "Precondition violation - argument 'inputFile' must not be NULL!");
		checkArgument(inputFile.exists(), "Precondition violation - argument 'inputFile' does not exist! Location: "
				+ inputFile.getAbsolutePath());
		checkArgument(inputFile.isFile(),
				"Precondition violation - argument 'inputFile' must be a File (is a Directory)!");
		checkNotNull(options, "Precondition violation - argument 'options' must not be NULL!");
		// initialize the xstream
		XStream xStream = createXStream(options);
		try {
			InputStream inputStream = new FileInputStream(inputFile);
			if (ChronosFileUtils.isGZipped(inputFile)) {
				// unzip the stream
				inputStream = new GZIPInputStream(inputStream);
			}
			// create the raw object input stream
			ObjectInputStream ois = xStream.createObjectInputStream(inputStream);
			// wrap it into an object input
			ObjectInputStreamWrapper wrapper = new ObjectInputStreamWrapper(ois);
			return wrapper;
		} catch (IOException e) {
			throw new ChronoDBStorageBackendException("Failed to open XStream for reading!", e);
		}
	}

	// =====================================================================================================================
	// INTERNAL HELPER METHODS
	// =====================================================================================================================

	/**
	 * Creates a new instance of {@link XStream} for reading or writing dump data.
	 *
	 * <p>
	 * It will be equipped with all {@linkplain #getAliases() default aliases}.
	 *
	 * @param options
	 *            The options to use for the stream. Must not be <code>null</code>.
	 * @return The new, pre-configured XStream instance. Never <code>null</code>.
	 */
	private static XStream createXStream(final DumpOptions options) {
		XStream xStream = new XStream();
		for (Alias alias : ChronoDBDumpFormat.getAliases()) {
			xStream.alias(alias.getAliasName(), alias.getType());
		}
		for (DumpOption.AliasOption aliasOption : options.getAliasOptions()) {
			Alias alias = aliasOption.getAlias();
			xStream.alias(alias.getAliasName(), alias.getType());
		}
		return xStream;
	}

	// =====================================================================================================================
	// PUBLIC INNER CLASSES
	// =====================================================================================================================

	/**
	 * An {@link Alias} is simply an alternative name for a {@link java.lang.Class} in the output file.
	 *
	 * <p>
	 * Aliases serve multiple purposes:
	 * <ul>
	 * <li>As an alias is typically shorter than a fully qualified class name, they make the output more compact.
	 * <li>Aliases typically have names that are easier to read for humans than class names.
	 * <li>As aliases prevent the qualified class name from leaking into a persistent file, classes can be moved to
	 * other Java packages without breaking existing dump files.
	 * </ul>
	 *
	 * <p>
	 * Please note that any class may have at most one alias! Aliases are immutable once created.
	 *
	 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
	 *
	 */
	public static class Alias {

		/** The {@link java.lang.Class} to provide an alias for. */
		private final Class<?> type;
		/** The alias name given to the class. */
		private final String aliasName;

		/**
		 * Creates a new alias.
		 *
		 * @param type
		 *            The class to give the alias name to. Must not be <code>null</code>.
		 * @param name
		 *            The alias name to assign to the class. Must not be <code>null</code>.
		 */
		public Alias(final Class<?> type, final String name) {
			checkNotNull(type, "Precondition violation - argument 'type' must not be NULL!");
			checkNotNull(name, "Precondition violation - argument 'name' must not be NULL!");
			this.type = type;
			this.aliasName = name;
		}

		/**
		 * Returns the alias name for the class.
		 *
		 * @return The alias name. Never <code>null</code>.
		 */
		public String getAliasName() {
			return this.aliasName;
		}

		/**
		 * Returns the class which receives an alias name by this instance.
		 *
		 * @return The class to be aliased. Never <code>null</code>.
		 */
		public Class<?> getType() {
			return this.type;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + (this.aliasName == null ? 0 : this.aliasName.hashCode());
			result = prime * result + (this.type == null ? 0 : this.type.hashCode());
			return result;
		}

		@Override
		public boolean equals(final Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (this.getClass() != obj.getClass()) {
				return false;
			}
			Alias other = (Alias) obj;
			if (this.aliasName == null) {
				if (other.aliasName != null) {
					return false;
				}
			} else if (!this.aliasName.equals(other.aliasName)) {
				return false;
			}
			if (this.type == null) {
				if (other.type != null) {
					return false;
				}
			} else if (!this.type.equals(other.type)) {
				return false;
			}
			return true;
		}

		@Override
		public String toString() {
			return "Alias [type=" + this.type + ", aliasName=" + this.aliasName + "]";
		}

	}

	// =====================================================================================================================
	// PRIVATE INNER CLASSES
	// =====================================================================================================================

	/**
	 * A simple wrapper for an {@link ObjectOutputStream} that implements the reduced {@link ObjectOutput} interface.
	 *
	 * <p>
	 * All calls to the {@link ObjectOutput} interface will be forwarded to the appropriate {@link ObjectOutputStream}
	 * methods.
	 *
	 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
	 *
	 */
	private static class ObjectOutputStreamWrapper implements ObjectOutput {

		/** The object output stream that is wrapped. */
		private final ObjectOutputStream oos;

		/**
		 * Creates a new instance that wraps the given stream.
		 *
		 * @param stream
		 *            The stream to wrap. Must not be <code>null</code>.
		 */
		public ObjectOutputStreamWrapper(final ObjectOutputStream stream) {
			checkNotNull(stream, "Precondition violation - argument 'stream' must not be NULL!");
			this.oos = stream;
		}

		@Override
		public void write(final Object object) {
			try {
				this.oos.writeObject(object);
			} catch (IOException e) {
				String clazz = "NULL";
				if (object != null) {
					clazz = object.getClass().getName();
				}
				throw new ChronoDBStorageBackendException("Failed to write object of type '" + clazz + "' to output!",
						e);
			}
		}

		@Override
		public void close() {
			try {
				this.oos.close();
			} catch (IOException e) {
				throw new ChronoDBStorageBackendException("Failed to close object output!", e);
			}
		}

	}

	/**
	 * A simple wrapper for an {@link ObjectInputStream} that implements the reduced {@link ObjectInput} interface.
	 *
	 * <p>
	 * All calls to the {@link ObjectInput} interface will be forwarded to the appropriate {@link ObjectInputStream}
	 * methods.
	 *
	 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
	 *
	 */
	private static class ObjectInputStreamWrapper implements ObjectInput {

		/** The object input stream that is being wrapped. */
		private final ObjectInputStream ois;
		/** As {@link ObjectInputStream} does not provide a {@link #hasNext()} method, we buffer one element here. */
		private Object next;
		/**
		 * Boolean flag that indicates if {@link #close()} has already been called (<code>true</codE>) or not (
		 * <code>false</code>).
		 */
		private boolean closed = false;

		/**
		 * Creates a new object input, wrapping the given {@link ObjectInputStream}.
		 *
		 * @param ois
		 *            The object input stream to wrap. Must not be <code>null</code>.
		 */
		public ObjectInputStreamWrapper(final ObjectInputStream ois) {
			checkNotNull(ois, "Precondition violation - argument 'ois' must not be NULL!");
			this.ois = ois;
			this.tryReadNext();
		}

		/**
		 * Fills the internal {@link next buffer} field to answer the {@link #hasNext()} method.
		 *
		 * <p>
		 * If {@link #next} is <code>null</code> after calling this method, the end of the wrapped input stream has been
		 * reached.
		 */
		private void tryReadNext() {
			try {
				this.next = this.ois.readObject();
			} catch (EOFException ex) {
				// end of stream was reached
				this.next = null;
			} catch (ClassNotFoundException | IOException e) {
				throw new ChronoDBStorageBackendException("Failed to read object from input!", e);
			}
		}

		@Override
		public Object next() {
			if (!this.hasNext()) {
				throw new NoSuchElementException();
			}
			Object next = this.next;
			this.tryReadNext();
			return next;
		}

		@Override
		public boolean hasNext() {
			if (this.isClosed()) {
				return false;
			}
			return this.next != null;
		}

		@Override
		public void close() {
			try {
				this.ois.close();
			} catch (IOException e) {
				throw new ChronoDBStorageBackendException("Failed to close object input stream!", e);
			} finally {
				this.closed = true;
			}
		}

		@Override
		public boolean isClosed() {
			return this.closed;
		}

	}
}
