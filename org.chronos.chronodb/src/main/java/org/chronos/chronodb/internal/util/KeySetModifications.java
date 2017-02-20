package org.chronos.chronodb.internal.util;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Set;

import com.google.common.collect.Sets;

public class KeySetModifications {

	private final Set<String> additions;
	private final Set<String> removals;

	public KeySetModifications(Set<String> additions, Set<String> removals) {
		checkNotNull(additions, "Precondition violation - argument 'additions' must not be NULL!");
		checkNotNull(removals, "Precondition violation - argument 'removals' must not be NULL!");
		this.additions = Collections.unmodifiableSet(Sets.newHashSet(additions));
		this.removals = Collections.unmodifiableSet(Sets.newHashSet(removals));
	}

	public Set<String> getAdditions() {
		return this.additions;
	}

	public Set<String> getRemovals() {
		return this.removals;
	}

	public void apply(Set<String> baseSet) {
		baseSet.addAll(this.additions);
		baseSet.removeAll(this.removals);
	}

}
