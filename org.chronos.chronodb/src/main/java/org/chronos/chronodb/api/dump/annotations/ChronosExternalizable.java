package org.chronos.chronodb.api.dump.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.dump.ChronoConverter;

/**
 * A marker annotation that associates a class with a {@link ChronoConverter}.
 *
 * <p>
 * This annotation is primarily used in conjunction with the
 * {@linkplain ChronoDB#writeDump(java.io.File, org.chronos.chronodb.api.DumpOption...) dump API}.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface ChronosExternalizable {

	/** The converter class to use for the annotated class. */
	public Class<? extends ChronoConverter<?, ?>> converterClass();

}
