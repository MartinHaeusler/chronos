package org.chronos.common.configuration.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.chronos.common.configuration.Comparison;

@Documented
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(RequiredIfConditions.class)
public @interface RequiredIf {

	public String field();

	public Comparison comparison();

	public String compareValue() default "";

}
