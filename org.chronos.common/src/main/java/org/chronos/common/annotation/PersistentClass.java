package org.chronos.common.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * This annotation indicates that the annotated class is persisted on disk.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
@Documented
@Retention(RetentionPolicy.SOURCE)
public @interface PersistentClass {

	public String value() default "";

}
