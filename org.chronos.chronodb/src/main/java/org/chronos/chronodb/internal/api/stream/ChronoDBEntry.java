package org.chronos.chronodb.internal.api.stream;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.api.key.ChronoIdentifier;
import org.chronos.chronodb.internal.impl.stream.entry.ChronoDBEntryImpl;

public interface ChronoDBEntry {

	// =====================================================================================================================
	// STATIC FACTORY
	// =====================================================================================================================

	public static ChronoDBEntry create(final ChronoIdentifier identifier, final byte[] value) {
		checkNotNull(identifier, "Precondition violation - argument 'identifier' must not be NULL!");
		return ChronoDBEntryImpl.create(identifier, value);
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	public ChronoIdentifier getIdentifier();

	public byte[] getValue();

}
