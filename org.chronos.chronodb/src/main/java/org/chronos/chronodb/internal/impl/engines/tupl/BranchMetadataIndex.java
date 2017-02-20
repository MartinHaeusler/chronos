package org.chronos.chronodb.internal.impl.engines.tupl;

import static com.google.common.base.Preconditions.*;

import java.io.IOException;
import java.util.Set;

import org.chronos.chronodb.internal.impl.BranchMetadata;
import org.chronos.chronodb.internal.impl.tupl.TuplTransaction;
import org.chronos.common.exceptions.ChronosIOException;
import org.chronos.common.serialization.KryoManager;
import org.cojen.tupl.Cursor;

import com.google.common.collect.Sets;

public class BranchMetadataIndex {

	// =====================================================================================================================
	// CONSTANTS
	// =====================================================================================================================

	public static final String NAME = "BranchMetadata";

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	public static void insertOrUpdate(final TuplTransaction tx, final BranchMetadata metadata) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(metadata, "Precondition violation - argument 'metadata' must not be NULL!");
		tx.store(NAME, metadata.getName(), KryoManager.serialize(metadata));
	}

	public static BranchMetadata getMetadata(final TuplTransaction tx, final String name) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(name, "Precondition violation - argument 'name' must not be NULL!");
		byte[] loadedValue = tx.load(NAME, name);
		if (loadedValue == null) {
			return null;
		}
		return KryoManager.deserialize(loadedValue);
	}

	public static Set<BranchMetadata> values(final TuplTransaction tx) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		Set<BranchMetadata> resultSet = Sets.newHashSet();
		Cursor cursor = tx.newCursorOn(NAME);
		try {
			cursor.first();
			if (cursor.key() == null) {
				// index is empty, return empty result set
				return resultSet;
			}
			while (cursor.key() != null) {
				byte[] value = cursor.value();
				if (value != null) {
					BranchMetadata branchMetadata = KryoManager.deserialize(value);
					resultSet.add(branchMetadata);
				}
				cursor.next();
			}
			return resultSet;
		} catch (IOException ioe) {
			throw new ChronosIOException("Failed to load branch metadata! See root cause for details.", ioe);
		} finally {
			if (cursor != null) {
				cursor.reset();
			}
		}
	}

}
