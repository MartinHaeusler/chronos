package org.chronos.chronodb.api;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.api.dump.ChronoConverter;
import org.chronos.chronodb.api.dump.ChronoDBDumpFormat.Alias;
import org.chronos.chronodb.api.dump.annotations.ChronosExternalizable;

/**
 * This class acts as a factory for options for {@link ChronoDB#writeDump(java.io.File, DumpOption...)}.
 *
 * <p>
 * Some options (that don't need parameters) can be accessed via constants, others are produced via static factory methods.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
public abstract class DumpOption {

	// =====================================================================================================================
	// USER OPTIONS
	// =====================================================================================================================

	/** Enforces binary encoding for DB entry content. Does not apply to metadata. */
	public static final DumpOption FORCE_BINARY_ENCODING = new FlagOption("forceBinaryEncoding");

	/**
	 * Enables G-Zipping of the output file to preserve space on disk.
	 *
	 * <p>
	 * Unzipping the file will yield the internal plain text.
	 */
	public static final DumpOption ENABLE_GZIP = new FlagOption("enableGZip");

	/**
	 * Creates an alias for the given class in the output format.
	 *
	 * <p>
	 * Please note that this is a <b>HINT</b> for the output writer and input reader. It may or may not affect all occurrences.
	 *
	 * <p>
	 * <b>/!\ WARNING /!\</b><br>
	 * You <b>must</b> use the <b>same set of aliases</b> when reading a dump as you used when writing it, otherwise the dump will not be readable! Also, alias names <b>must not clash</b> and <b>must be unique</b>!
	 *
	 * <p>
	 * This has several uses:
	 * <ul>
	 * <li>It reduces the overall length of the dump by using shorter class names
	 * <li>It improves the human-readability of the dump
	 * <li>It makes the dump more resilient against package name changes (or classes moving to other packages)
	 * </ul>
	 *
	 * @param clazz
	 *            The class to create the alias for. Must not be <code>null</code>.
	 * @param alias
	 *            The alias to give to the class. must not be <code>null</code>.
	 * @return The alias option. Never <code>null</code>.
	 */
	public static DumpOption aliasHint(final Class<?> clazz, final String alias) {
		checkNotNull(clazz, "Precondition violation - argument 'clazz' must not be NULL!");
		checkNotNull(alias, "Precondition violation - argument 'alias' must not be NULL!");
		return new AliasOption(clazz, alias);
	}

	/**
	 * Registers a default converter for a given class.
	 *
	 * <p>
	 * Please note that which class to use as the first argument depends on the use case:
	 * <ul>
	 * <li>When writing a dump, <code>type</code> must be the internal class (i.e. the parameter type of the {@link ChronoConverter#writeToOutput(Object) writeToOutput} method).
	 * <li>When reading a dump, <code>type</code> must be the serialized class (i.e. the parameter type of the {@link ChronoConverter#readFromInput(Object) readFromInput} method).
	 * </ul>
	 *
	 * <p>
	 * Example:
	 *
	 * <pre>
	 *
	 * // we have a converter class like this (converts Person <-> PersonExt):
	 * public class PersonConverter implements ChronoConverter&lt;Person, PersonExt&gt; {...}
	 *
	 * // we write a dump using:
	 * DumpOption.defaultConverter(Person.class, new PersonConverter());
	 * // this will create a dump that contains no Person instances, but PersonExt instances.
	 *
	 * // ... if we want to read that dump, we use:
	 * DumpOption.defaultConverter(PersonExt.class, new PersonConverter());
	 * // this will find all PersonExt instances in the dump, and convert them to Person instances again.
	 *
	 * </pre>
	 *
	 * <p>
	 * As a general rule, a {@link ChronosExternalizable} annotation on a value class will override the default converter if specified, unless they are equal, in which case the default converter takes precedence. When writing a dump using default converters, the used converters will <b>not</b> be part of the output.
	 *
	 * <p>
	 * <b>/!\ WARNING /!\</b><br>
	 * You <b>must</b> use the <b>same set of default converters</b> when reading a dump as you used when writing it, otherwise the dump will not be readable!
	 *
	 * @param type
	 *            The class to associate the converter with. Must not be <code>null</code>.
	 * @param converter
	 *            The converter to use for instances of the model class. Must not be <code>null</code>.
	 * @return The option that adds the given default converter. Never <code>null</code>.
	 */
	public static DumpOption defaultConverter(final Class<?> type, final ChronoConverter<?, ?> converter) {
		checkNotNull(type, "Precondition violation - argument 'type' must not be NULL!");
		checkNotNull(converter, "Precondition violation - argument 'converter' must not be NULL!");
		return new DefaultConverterOption(type, converter);
	}

	/**
	 * Sets the batch size to use when reading elements from a dump.
	 *
	 * <p>
	 * Higher batch sizes consume more RAM, but are in general faster. Sensible values usually range from 1000 to 10000.
	 *
	 * @param batchSize
	 *            The batch size to use. Must be greater than or equal to 1.
	 * @return The option that sets the batch size. Never <code>null</code>.
	 */
	public static DumpOption batchSize(final int batchSize) {
		checkArgument(batchSize > 0,
				"Precondition violation - argument 'batchSize' must be strictly greater than zero!");
		return new IntOption("batchSize", batchSize);
	}

	// =====================================================================================================================
	// INNER CLASSES
	// =====================================================================================================================

	/**
	 * A {@link FlagOption} is the simplest kind of option. It has a name, and is either present or absent.
	 *
	 * <p>
	 * Flag options are uniquely identified by their name.
	 *
	 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
	 */
	private static class FlagOption extends DumpOption {

		/** The name of the flag. */
		private String name;

		/**
		 * Constructs a new flag option.
		 *
		 * @param name
		 *            The name for the flag. Must not be <code>null</code>.
		 */
		private FlagOption(final String name) {
			checkNotNull(name, "Precondition violation - argument 'name' must not be NULL!");
			this.name = name;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + (this.name == null ? 0 : this.name.hashCode());
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
			FlagOption other = (FlagOption) obj;
			if (this.name == null) {
				if (other.name != null) {
					return false;
				}
			} else if (!this.name.equals(other.name)) {
				return false;
			}
			return true;
		}

		@Override
		public String toString() {
			return "FlagOption [name=" + this.name + "]";
		}

	}

	/**
	 * An {@link AliasOption} allows to register alternative (usually shorter) names for classes that appear in a dump.
	 *
	 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
	 *
	 */
	public static class AliasOption extends DumpOption {

		/** The alias that is added by this option. */
		private Alias alias;

		/**
		 * Creates a new alias option.
		 *
		 * @param clazz
		 *            The Java {@link Class} to define the alias for. Must not be <code>null</code>.
		 * @param name
		 *            The alias name to use for the class. Must not be <code>null</code>.
		 */
		public AliasOption(final Class<?> clazz, final String name) {
			this.alias = new Alias(clazz, name);
		}

		/**
		 * Returns the alias that is provided by this option.
		 *
		 * @return The alias. Never <code>null</code>.
		 */
		public Alias getAlias() {
			return this.alias;
		}

		@Override
		public String toString() {
			return "AliasOption [alias=" + this.alias + "]";
		}

	}

	/**
	 * This option allows to add default {@link ChronoConverter}s to a dump process.
	 *
	 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
	 *
	 */
	public static class DefaultConverterOption extends DumpOption {

		/** The type to be converted. */
		private final Class<?> type;
		/** The converter to be applied to the {@link #type}. */
		private final ChronoConverter<?, ?> converter;

		/**
		 * Creates a new Default Converter Option.
		 *
		 * @param type
		 *            The Java {@link Class} that should be converted. Must not be <code>null</code>.
		 * @param converter
		 *            The converter to apply to instances of the given class. Must not be <code>null</code>.
		 */
		public DefaultConverterOption(final Class<?> type, final ChronoConverter<?, ?> converter) {
			checkNotNull(type, "Precondition violation - argument 'type' must not be NULL!");
			checkNotNull(converter, "Precondition violation - argument 'converter' must not be NULL!");
			this.type = type;
			this.converter = converter;
		}

		/**
		 * Returns the Java {@link Class} that should be converted.
		 *
		 * @return The class. Never <code>null</code>.
		 */
		public Class<?> getType() {
			return this.type;
		}

		/**
		 * Returns the converter to be applied to instances of the {@link #getType() class}.
		 *
		 * @return The converter. Never <code>null</code>.
		 */
		public ChronoConverter<?, ?> getConverter() {
			return this.converter;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + (this.converter == null ? 0 : this.converter.hashCode());
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
			DefaultConverterOption other = (DefaultConverterOption) obj;
			if (this.converter == null) {
				if (other.converter != null) {
					return false;
				}
			} else if (!this.converter.equals(other.converter)) {
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

	}

	/**
	 * An {@link IntOption} is a named container for an integer value.
	 *
	 * <p>
	 * IntOptions are uniquely identified by their name.
	 *
	 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
	 *
	 */
	public static class IntOption extends DumpOption {

		/** The name of this option. */
		private final String name;
		/** The integer value associated with this option. */
		private final int value;

		/**
		 * Constructs a new IntOption.
		 *
		 * @param name
		 *            The name to use for the option. Must not be <code>null</code>.
		 * @param value
		 *            The value to use for the option. Must not be <code>null</code>.
		 */
		public IntOption(final String name, final int value) {
			checkNotNull(name, "Precondition violation - argument 'name' must not be NULL!");
			this.name = name;
			this.value = value;
		}

		/**
		 * Returns the name of this option.
		 *
		 * @return The name. Never <code>null</code>.
		 */
		public String getName() {
			return this.name;
		}

		/**
		 * Returns the integer value associated with this option.
		 *
		 * @return The integer value.
		 */
		public int getValue() {
			return this.value;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + (this.name == null ? 0 : this.name.hashCode());
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
			IntOption other = (IntOption) obj;
			if (this.name == null) {
				if (other.name != null) {
					return false;
				}
			} else if (!this.name.equals(other.name)) {
				return false;
			}
			return true;
		}

	}

}
