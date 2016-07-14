package org.chronos.chronodb.internal.impl.stream.entry;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.api.key.ChronoIdentifier;
import org.chronos.chronodb.internal.api.stream.ChronoDBEntry;

public class ChronoDBEntryImpl implements ChronoDBEntry {

	// =====================================================================================================================
	// STATIC FACTORY
	// =====================================================================================================================

	public static ChronoDBEntryImpl create(final ChronoIdentifier identifier, final byte[] value) {
		checkNotNull(identifier, "Precondition violation - argument 'identifier' must not be NULL!");
		return new ChronoDBEntryImpl(identifier, value);
	}

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	private final ChronoIdentifier identifier;
	private final byte[] value;

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	protected ChronoDBEntryImpl(final ChronoIdentifier identifier, final byte[] value) {
		checkNotNull(identifier, "Precondition violation - argument 'identifier' must not be NULL!");
		this.identifier = identifier;
		this.value = value;
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	@Override
	public ChronoIdentifier getIdentifier() {
		return this.identifier;
	}

	@Override
	public byte[] getValue() {
		return this.value;
	}

}
