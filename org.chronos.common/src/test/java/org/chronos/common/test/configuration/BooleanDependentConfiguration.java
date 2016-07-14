package org.chronos.common.test.configuration;

import org.chronos.common.configuration.AbstractConfiguration;
import org.chronos.common.configuration.Comparison;
import org.chronos.common.configuration.annotation.Namespace;
import org.chronos.common.configuration.annotation.Parameter;
import org.chronos.common.configuration.annotation.RequiredIf;

@Namespace("org.chronos.common.test")
public class BooleanDependentConfiguration extends AbstractConfiguration {

	@Parameter
	private Boolean bool;

	@Parameter
	@RequiredIf(field = "bool", comparison = Comparison.IS_SET_TO, compareValue = "true")
	private String string;

	public Boolean getBool() {
		return this.bool;
	}

	public String getString() {
		return this.string;
	}

}
