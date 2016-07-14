package org.chronos.chronodb.internal.impl;

import static com.google.common.base.Preconditions.*;

import java.util.Set;

import org.chronos.chronodb.api.ChangeSetEntry;
import org.chronos.chronodb.api.PutOption;
import org.chronos.chronodb.api.key.QualifiedKey;

import com.google.common.collect.ImmutableSet;

public class ChangeSetEntryImpl implements ChangeSetEntry {

	// =================================================================================================================
	// STATIC FACTORY METHODS
	// =================================================================================================================

	public static ChangeSetEntry createChange(final QualifiedKey key, final Object newValue, final PutOption... options) {
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		checkNotNull(newValue, "Precondition violation - argument 'newValue' must not be NULL!");
		checkNotNull(options, "Precondition violation - argument 'options' must not be NULL!");
		return new ChangeSetEntryImpl(key, newValue, options);
	}

	public static ChangeSetEntry createDeletion(final QualifiedKey key) {
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		return new ChangeSetEntryImpl(key, null);
	}

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	private final QualifiedKey key;
	private final Object value;
	private final Set<PutOption> options;

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	protected ChangeSetEntryImpl(final QualifiedKey key, final Object value, final PutOption... options) {
		this.key = key;
		this.value = value;
		this.options = ImmutableSet.<PutOption> builder().add(options).build();
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	@Override
	public String getKey() {
		return this.key.getKey();
	}

	@Override
	public String getKeyspace() {
		return this.key.getKeyspace();
	}

	@Override
	public Object getValue() {
		return this.value;
	}

	public Set<PutOption> getOptions() {
		return this.options;
	}

	@Override
	public boolean isSet() {
		return this.value != null;
	}

	@Override
	public boolean isRemove() {
		return this.value == null;
	}

	// =================================================================================================================
	// HASH CODE & EQUALS
	// =================================================================================================================

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (this.key == null ? 0 : this.key.hashCode());
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
		ChangeSetEntryImpl other = (ChangeSetEntryImpl) obj;
		if (this.key == null) {
			if (other.key != null) {
				return false;
			}
		} else if (!this.key.equals(other.key)) {
			return false;
		}
		return true;
	}

}
