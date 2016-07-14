package org.chronos.common.configuration;

import static com.google.common.base.Preconditions.*;

import org.chronos.common.configuration.annotation.IgnoredIf;
import org.chronos.common.configuration.annotation.RequiredIf;
import org.chronos.common.exceptions.UnknownEnumLiteralException;

public class Condition {

	private final String field;
	private final Comparison comparison;
	private final String value;

	public Condition(final RequiredIf requiredIf) {
		checkNotNull(requiredIf, "Precondition violation - argument 'requiredIf' must not be NULL!");
		this.field = requiredIf.field();
		this.comparison = requiredIf.comparison();
		this.value = requiredIf.compareValue();
	}

	public Condition(final IgnoredIf ignoredIf) {
		checkNotNull(ignoredIf, "Precondition violation - argument 'ignoredIf' must not be NULL!");
		this.field = ignoredIf.field();
		this.comparison = ignoredIf.comparison();
		this.value = ignoredIf.compareValue();
	}

	public boolean appliesTo(final AbstractConfiguration config) {
		checkNotNull(config, "Precondition violation - argument 'config' must not be NULL!");
		ParameterMetadata parameter = config.getMetadataOfField(this.field);
		Object value = parameter.getValue(config);
		switch (this.comparison) {
		case IS_SET:
			return value != null;
		case IS_NOT_SET:
			return value == null;
		case IS_SET_TO:
			return this.value.equals(value) || this.value.toString().equals(String.valueOf(value));
		case IS_NOT_SET_TO:
			return this.value.toString().equals(String.valueOf(value)) == false;
		default:
			throw new UnknownEnumLiteralException(this.comparison);
		}
	}
}
