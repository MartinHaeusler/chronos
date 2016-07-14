package org.chronos.chronodb.api.builder.database;

import org.chronos.chronodb.api.ChronoDB;

/**
 * A builder for embedded instances of {@link ChronoDB}.
 *
 * <p>
 * Embedded instances work on the hard drive of the local machine.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface ChronoDBEmbeddedBuilder extends ChronoDBFinalizableBuilder<ChronoDBEmbeddedBuilder> {

	/**
	 * Enables the "drop on shutdown" feature (by default, it is off).
	 *
	 * <p>
	 * When this feature is enabled, after calling {@link ChronoDB#close()} on the produced {@link ChronoDB}, it will
	 * clear the working directory.
	 *
	 * <p>
	 * <b>WARNING:</b> If this feature is enabled, the <i>entire</i> work directory will be cleared, even files not
	 * managed by ChronoDB will be removed!
	 *
	 * @return <code>this</code> (for fluent method chaining)
	 */
	public ChronoDBEmbeddedBuilder withDropOnShutdown();

}
