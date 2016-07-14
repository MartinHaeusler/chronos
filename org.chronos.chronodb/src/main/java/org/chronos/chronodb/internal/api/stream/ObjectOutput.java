package org.chronos.chronodb.internal.api.stream;

import java.io.ObjectOutputStream;

/**
 * An {@link ObjectOutput} is a minimal interface for writing arbitrary objects to a data sink.
 *
 * <p>
 * Please note that this interface is different from {@link ObjectOutputStream} in that it provides only a fraction of
 * the methods and functionality. This interface is used to keep the dependency towards the externalization mechanism to
 * a bare minimum.
 *
 * <p>
 * As this class extends {@link AutoCloseable}, instances of this class must be {@linkplain #close() closed} explicitly.
 * However, the Java 7 <code>try-with-resources</code> statement may be used for doing so.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface ObjectOutput extends AutoCloseable {

	/**
	 * Writes the given object to the output.
	 *
	 * @param object
	 *            The object to write. Some implementations may refuse to write the <code>null</code> object.
	 */
	public void write(Object object);

	/**
	 * Closes this object output.
	 *
	 * <p>
	 * Any further call to {@link #write(Object)} will fail with an appropriate exception.
	 */
	@Override
	public void close();

}
