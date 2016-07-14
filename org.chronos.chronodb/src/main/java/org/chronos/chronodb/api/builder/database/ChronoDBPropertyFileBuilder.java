package org.chronos.chronodb.api.builder.database;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.internal.impl.ChronoDBConfigurationImpl;

/**
 * A builder for all kinds of instances of {@link ChronoDB}, based on a <code>*.properties</code> file.
 *
 * <p>
 * The resulting {@link ChronoDB} implementation is determined via the value of the
 * {@link ChronoDBConfigurationImpl#getBackendType()} property.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface ChronoDBPropertyFileBuilder extends ChronoDBFinalizableBuilder<ChronoDBPropertyFileBuilder> {

}
