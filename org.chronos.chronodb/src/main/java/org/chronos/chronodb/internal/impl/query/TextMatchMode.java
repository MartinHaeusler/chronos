package org.chronos.chronodb.internal.impl.query;

import org.chronos.chronodb.api.query.Condition;

/**
 * A simple enumeration that decides how to match a text query against the index.
 *
 * <p>
 * Usually used in conjunction with a {@link Condition}.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public enum TextMatchMode {

	/** Strict text matching (i.e. respecting upper- & lowercase differences). */
	STRICT,

	/** Case insensitive matching. */
	CASE_INSENSITIVE;

}
