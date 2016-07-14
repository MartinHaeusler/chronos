package org.chronos.chronodb.api;

/**
 * Put options are advanced options that can be set per {@link ChronoDBTransaction#put(String, Object, PutOption...)} command.
 *
 * <p>
 * Normal users should not have to concern themselves with these options.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public enum PutOption {

	/**
	 * [ADVANCED USERS ONLY] This option tells {@link ChronoDB} to skip the secondary indexing step for this value.
	 *
	 * <p>
	 * This can potentially break your secondary index and make it inconsistent! Use this option only as a performance optimization when you are <b>sure</b> that this put operation <b>would not have altered</b> the state of the secondary indexer.
	 */
	NO_INDEX;

	/** The default option to use. */
	public static final PutOption[] NONE = {};
}
