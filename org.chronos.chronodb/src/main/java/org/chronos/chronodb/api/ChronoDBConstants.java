package org.chronos.chronodb.api;

import org.chronos.common.exceptions.NotInstantiableException;

/**
 * A static collection of constants used throughout the {@link ChronoDB} API.
 *
 * <p>
 * This class is merely a container for the constants and must not be instantiated.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public final class ChronoDBConstants {

	/** The identifier for the master branch. The master branch is predefined and always exists. */
	public static final String MASTER_BRANCH_IDENTIFIER = "master";

	/** The name for the default keyspace. The default keyspace is predefined and always exists. */
	public static final String DEFAULT_KEYSPACE_NAME = "default";

	private ChronoDBConstants() {
		throw new NotInstantiableException("This class must not be instantiated!");
	}
}
