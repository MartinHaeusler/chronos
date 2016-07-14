package org.chronos.chronodb.api.dump;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.dump.annotations.ChronosExternalizable;

/**
 * A basic converter interface.
 *
 * <p>
 * The intended use of this interface is to work as a converter between an internal (in-memory) and an external
 * (persistent) data format. It is mainly used in combination with the
 * {@linkplain ChronoDB#writeDump(java.io.File, org.chronos.chronodb.api.DumpOption...) dump API} and the
 * {@link ChronosExternalizable} annotation.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 * @param <I>
 *            The internal data format that can be converted to the external format by this converter.
 * @param <E>
 *            The external data format that can be converted to the internal format by this converter.
 */
public interface ChronoConverter<I, E> {

	/**
	 * Converts the given internal object into its external representation.
	 *
	 * @param value
	 *            The internal object to convert. May be <code>null</code>.
	 * @return The external representation of the object. May be <code>null</code>.
	 */
	public E writeToOutput(I value);

	/**
	 * Converts the given external object into its internal representation.
	 *
	 * @param value
	 *            The external object to convert. May be <code>null</code>.
	 * @return The internal representation of the object. May be <code>null</code>.
	 */
	public I readFromInput(E value);

}
