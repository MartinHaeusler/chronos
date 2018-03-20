package org.chronos.common.base;

import org.chronos.common.logging.LogLevel;

/**
 * Chrono Compiler Configuration.
 *
 * <p>
 * This class contains constants that are compiled into Chronos, but can be tweaked for specific builds.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public class CCC {

	public static final LogLevel MIN_LOG_LEVEL = LogLevel.WARN;

	public static final boolean TRACE_ENABLED = MIN_LOG_LEVEL.isGreaterThanOrEqualTo(LogLevel.TRACE);

	public static final boolean DEBUG_ENABLED = MIN_LOG_LEVEL.isGreaterThanOrEqualTo(LogLevel.DEBUG);

	public static final boolean INFO_ENABLED = MIN_LOG_LEVEL.isGreaterThanOrEqualTo(LogLevel.INFO);

	public static final boolean WARN_ENABLED = MIN_LOG_LEVEL.isGreaterThanOrEqualTo(LogLevel.WARN);

	public static final boolean ERROR_ENABLED = MIN_LOG_LEVEL.isGreaterThanOrEqualTo(LogLevel.ERROR);
}
