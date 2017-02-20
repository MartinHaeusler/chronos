package org.chronos.chronodb.api;

/**
 * A general multi-purpose enumeration for any kind of ordering.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
public enum Order {

	/** Represents ascending order, i.e. when iterating, every entry will be larger than the previous one. */
	ASCENDING,

	/** Represents descending order, i.e. when iterating, every entry will be smaller than the previous one. */
	DESCENDING

}
