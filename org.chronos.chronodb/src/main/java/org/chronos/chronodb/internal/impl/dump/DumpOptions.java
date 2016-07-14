package org.chronos.chronodb.internal.impl.dump;

import static com.google.common.base.Preconditions.*;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.chronos.chronodb.api.DumpOption;
import org.chronos.chronodb.api.DumpOption.IntOption;

import com.google.common.collect.Sets;

public class DumpOptions {

	// =====================================================================================================================
	// DEFAULTS
	// =====================================================================================================================

	public static final int DEFAULT_BATCH_SIZE = 1000;

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	private final Set<DumpOption> options = Sets.newHashSet();

	public DumpOptions(final DumpOption... options) {
		if (options != null && options.length > 0) {
			for (DumpOption option : options) {
				this.options.add(option);
			}
		}
	}

	public void enable(final DumpOption option) {
		checkNotNull(option, "Precondition violation - argument 'option' must not be NULL!");
		this.options.add(option);
	}

	public void disable(final DumpOption option) {
		checkNotNull(option, "Precondition violation - argument 'option' must not be NULL!");
		this.options.remove(option);
	}

	public boolean isGZipEnabled() {
		return this.isOptionEnabled(DumpOption.ENABLE_GZIP);
	}

	public boolean isForceBinaryEncodingEnabled() {
		return this.isOptionEnabled(DumpOption.FORCE_BINARY_ENCODING);
	}

	public boolean isOptionEnabled(final DumpOption option) {
		checkNotNull(option, "Precondition violation - argument 'option' must not be NULL!");
		return this.options.contains(option);
	}

	public Set<DumpOption.AliasOption> getAliasOptions() {
		return this.options.stream()
				// filter only the alias options
				.filter(option -> option instanceof DumpOption.AliasOption)
				// cast them to the correct type
				.map(option -> (DumpOption.AliasOption) option)
				// collect them in a set
				.collect(Collectors.toSet());
	}

	public Set<DumpOption.DefaultConverterOption> getDefaultConverterOptions() {
		return this.options.stream()
				// filter only the default converter options
				.filter(option -> option instanceof DumpOption.DefaultConverterOption)
				// cast them to the correct type
				.map(option -> (DumpOption.DefaultConverterOption) option)
				// collect them in a set
				.collect(Collectors.toSet());
	}

	public int getBatchSize() {
		Optional<IntOption> batchSize = this.options.stream().filter(option -> option instanceof IntOption)
				.map(option -> (IntOption) option).filter(option -> "batchSize".equals(option.getName())).findAny();
		if (batchSize.isPresent()) {
			return batchSize.get().getValue();
		} else {
			return DEFAULT_BATCH_SIZE;
		}
	}

	public DumpOption[] toArray() {
		return this.options.toArray(new DumpOption[this.options.size()]);
	}
}
