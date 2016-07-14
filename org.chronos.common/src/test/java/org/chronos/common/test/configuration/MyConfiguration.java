package org.chronos.common.test.configuration;

import static com.google.common.base.Preconditions.*;

import java.time.DayOfWeek;
import java.util.Collections;
import java.util.Set;

import org.chronos.common.configuration.AbstractConfiguration;
import org.chronos.common.configuration.Comparison;
import org.chronos.common.configuration.ParameterValueConverter;
import org.chronos.common.configuration.annotation.EnumFactoryMethod;
import org.chronos.common.configuration.annotation.IgnoredIf;
import org.chronos.common.configuration.annotation.Namespace;
import org.chronos.common.configuration.annotation.Parameter;
import org.chronos.common.configuration.annotation.RequiredIf;
import org.chronos.common.configuration.annotation.ValueAlias;
import org.chronos.common.configuration.annotation.ValueConverter;

import com.google.common.collect.Sets;

@Namespace("org.chronos.common.test")
public class MyConfiguration extends AbstractConfiguration {

	@Parameter
	private String name;

	@Parameter(key = "integer", optional = false)
	private int intValue = 42;

	@ValueAlias(alias = "mon", mapTo = "MONDAY")
	@ValueAlias(alias = "tue", mapTo = "TUESDAY")
	@ValueAlias(alias = "wed", mapTo = "WEDNESDAY")
	@ValueAlias(alias = "thu", mapTo = "THURSDAY")
	@ValueAlias(alias = "fri", mapTo = "FRIDAY")
	@ValueAlias(alias = "sat", mapTo = "SATURDAY")
	@ValueAlias(alias = "sun", mapTo = "SUNDAY")
	@Parameter(key = "org.chronos.common.test.day", optional = true)
	private DayOfWeek dayOfWeek;

	@Parameter
	@IgnoredIf(field = "dayOfWeek", comparison = Comparison.IS_SET_TO, compareValue = "SATURDAY")
	@IgnoredIf(field = "dayOfWeek", comparison = Comparison.IS_SET_TO, compareValue = "SUNDAY")
	private Double motivation;

	@Parameter()
	@RequiredIf(field = "dayOfWeek", comparison = Comparison.IS_SET_TO, compareValue = "MONDAY")
	private Boolean hangover;

	@Parameter(optional = true)
	@ValueConverter(CoordinateParser.class)
	private Coordinate coordinate;

	@Parameter(optional = true)
	@EnumFactoryMethod("fromString")
	private MyEnum myEnum;

	public MyConfiguration() {

	}

	public String getName() {
		return this.name;
	}

	public int getIntValue() {
		return this.intValue;
	}

	public DayOfWeek getDayOfWeek() {
		return this.dayOfWeek;
	}

	public double getMotivation() {
		return this.motivation;
	}

	public boolean isHangover() {
		return this.hangover;
	}

	public Coordinate getCoordinate() {
		return this.coordinate;
	}

	public MyEnum getMyEnum() {
		return this.myEnum;
	}

	public static class Coordinate {

		private String x;
		private String y;

		public Coordinate(final String x, final String y) {
			this.x = x;
			this.y = y;
		}

		public String getX() {
			return this.x;
		}

		public String getY() {
			return this.y;
		}

	}

	public static class CoordinateParser implements ParameterValueConverter {

		@Override
		public Object convert(final Object rawParameter) {
			String string = String.valueOf(rawParameter);
			char separator = ';';
			int separatorIndex = string.indexOf(separator);
			String x = string.substring(1, separatorIndex);
			String y = string.substring(separatorIndex + 1, string.length() - 1);
			return new Coordinate(x, y);
		}

	}

	public static enum MyEnum {

		ONE("one", "1"), TWO("2", "two"), THREE("3", "three");

		private final String primaryName;
		private final Set<String> aliases;
		private final Set<String> allNames;

		private MyEnum(final String primaryName, final String... aliases) {
			checkNotNull(primaryName, "Precondition violation - argument 'primaryName' must not be NULL!");
			this.primaryName = primaryName;
			Set<String> myAliases = Sets.newHashSet();
			if (aliases != null && aliases.length > 0) {
				for (String alias : aliases) {
					myAliases.add(alias);
				}
			}
			this.aliases = Collections.unmodifiableSet(myAliases);
			Set<String> myNames = Sets.newHashSet();
			myNames.add(primaryName);
			myNames.addAll(this.aliases);
			this.allNames = Collections.unmodifiableSet(myNames);
		}

		@Override
		public String toString() {
			return this.primaryName;
		}

		public static MyEnum fromString(final String stringValue) {
			checkNotNull(stringValue, "Precondition violation - argument 'stringValue' must not be NULL!");
			String token = stringValue.toLowerCase().trim();
			if (token.isEmpty()) {
				throw new IllegalArgumentException("Cannot parse DuplicateVersionEliminationMode from empty string!");
			}
			for (MyEnum literal : MyEnum.values()) {
				for (String name : literal.allNames) {
					if (name.equalsIgnoreCase(token)) {
						return literal;
					}
				}
			}
			throw new IllegalArgumentException("Unknown MyEnum: '" + token + "'!");
		}

	}

}
