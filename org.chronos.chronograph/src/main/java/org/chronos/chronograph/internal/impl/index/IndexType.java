package org.chronos.chronograph.internal.impl.index;

/**
 * An enumeration over index types.
 *
 * <p>
 * This enumeration is used in the construction of a graph index, indicating which values to index.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public enum IndexType {

	/** Type for indices that store {@link String} values. */
	STRING,
	/** Type for indices that store {@link Long} values. */
	LONG,
	/** Type for indices that store {@link Double} values. */
	DOUBLE;

}
