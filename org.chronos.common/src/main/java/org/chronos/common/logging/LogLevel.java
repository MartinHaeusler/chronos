package org.chronos.common.logging;

import static com.google.common.base.Preconditions.*;

public enum LogLevel {

	TRACE(1), DEBUG(2), INFO(3), WARN(4), ERROR(5);

	private final int level;

	private LogLevel(final int level) {
		checkArgument(level >= 0, "LogLevel can not be negative!");
		this.level = level;
	}

	public boolean isGreaterThan(final LogLevel other) {
		return this.level > other.level;
	}

	public boolean isLessThan(final LogLevel other) {
		return this.level < other.level;
	}

	public boolean isGreaterThanOrEqualTo(final LogLevel other) {
		return this.level >= other.level;
	}

	public boolean isLessThanOrEqualTo(final LogLevel other) {
		return this.level <= other.level;
	}
}
