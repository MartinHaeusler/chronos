package org.chronos.chronodb.internal.api.stream;

import java.io.ObjectInputStream;

/**
 * An {@link ObjectInput} is a minimal interface for reading arbitrary objects from a data source.
 *
 * <p>
 * Please note that this interface is different from {@link ObjectInputStream} in that it provides only a fraction of
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
public interface ObjectInput extends CloseableIterator<Object> {

}
