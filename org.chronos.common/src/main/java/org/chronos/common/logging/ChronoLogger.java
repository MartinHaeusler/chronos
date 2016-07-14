package org.chronos.common.logging;

import java.util.HashMap;
import java.util.Map;

import org.chronos.common.base.CCC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a thin wrapper around a logging facility (<code>org.slf4j</code>).<br>
 * It is recommended to import this class using a <code>static</code> import as such:<br>
 * <br>
 *
 * <code>
 * import static org.chronos.common.logging.ChronoLogger.*;
 * </code><br>
 * <br>
 *
 * Afterwards, you can access the static methods provided by this class without qualifying them. For example:<br>
 * <br>
 *
 * <code>
 * log("This is an information");<br>
 * logWarning("This is a warning");<br>
 * logError("This is an error");<br>
 * </code><br>
 * <br>
 *
 * Also, you can log exceptions directly as such:<br>
 * <br>
 *
 * <code>
 * try {<br>
 *    &nbsp;&nbsp;&nbsp;&nbsp;// do some unsafe work here...<br>
 * } catch (Exception e) {<br>
 *    &nbsp;&nbsp;&nbsp;&nbsp;// log the exception directly<br>
 *    &nbsp;&nbsp;&nbsp;&nbsp;logError(e);<br>
 * }
 *
 * </code>
 *
 * @author martin.haeusler@student.uibk.ac.at -- Initial Contribution and API
 *
 */
public class ChronoLogger {

	/** The map of all maintained loggers. */
	private static final Map<String, Logger> LOGGERS = new HashMap<String, Logger>();

	/**
	 * Returns a logger for the given name.<br>
	 * The retrieved logger is a <code>java.util.logging.</code>{@link Logger} that does <b>not</b> listen to its parent
	 * handlers, and has a custom {@link ConsoleLogHandler} attached that works based on a custom {@link LogFormatter}.
	 *
	 * @param name
	 *            The name of the logger to retrieve
	 * @return The logger for this name.
	 */
	private static Logger getLogger(final String name) {
		if (LOGGERS.containsKey(name)) {
			return LOGGERS.get(name);
		}
		Logger newLogger = LoggerFactory.getLogger(name);
		// newLogger.setUseParentHandlers(false);
		// Handler logHandler = new ConsoleLogHandler();
		// logHandler.setFormatter(LogFormatter.INSTANCE);
		// newLogger.addHandler(logHandler);
		LOGGERS.put(name, newLogger);
		return newLogger;
	}

	/**
	 * Internal method for logging purposes. Must not be called by clients directly.
	 *
	 * @param level
	 *            The log level to use
	 * @param message
	 *            The message to log
	 * @param error
	 *            Only valid for {@link LogLevel.ERROR} messages. Contains an optional throwable. May be
	 *            <code>null</code>.
	 */
	private static void logInternal(final LogLevel level, final String message, final Throwable error) {
		StackTraceElement caller = Thread.currentThread().getStackTrace()[3];
		String callerClass = caller.getClassName();
		// System.out.println("CALLER CLASS: '" + callerClass + "'");
		Logger logger = getLogger(callerClass);
		switch (level) {
		case TRACE:
			logger.trace(message);
			break;
		case INFO:
			logger.info(message);
			break;
		case DEBUG:
			logger.debug(message);
			break;
		case WARN:
			logger.warn(message);
			break;
		case ERROR:
			if (error == null) {
				// no throwable given, only log the message
				logger.error(message);
			} else {
				// throwable is given, log message and throwable
				logger.error(message, error);
			}
			break;
		default:
			throw new IllegalArgumentException("Unknown LogLevel: " + level);
		}

	}

	/**
	 * Logs an information message to the console.<br>
	 * Fully equivalent to:<br>
	 * <br>
	 *
	 * <code>
	 * logInfo(message);
	 * </code>
	 *
	 * @param message
	 *            The message to log. If <code>null</code>, the empty string will be used instead.
	 */
	public static void log(final String message) {
		if (CCC.MIN_LOG_LEVEL.isGreaterThan(LogLevel.INFO)) {
			// don't log
			return;
		}
		String actualMessage = message;
		if (actualMessage == null) {
			actualMessage = "";
		}
		logInternal(LogLevel.INFO, actualMessage, null);
	}

	/**
	 * Logs a trace message to the console.
	 *
	 * @param message
	 *            The message to log. If <code>null</code>, the empty string will be used instead.
	 */
	public static void logTrace(final String message) {
		if (CCC.MIN_LOG_LEVEL.isGreaterThan(LogLevel.TRACE)) {
			// don't log
			return;
		}
		String actualMessage = message;
		if (actualMessage == null) {
			actualMessage = "";
		}
		logInternal(LogLevel.TRACE, actualMessage, null);
	}

	/**
	 * Logs an information message to the console.
	 *
	 * @param message
	 *            The message to log. If <code>null</code>, the empty string will be used instead.
	 */
	public static void logInfo(final String message) {
		if (CCC.MIN_LOG_LEVEL.isGreaterThan(LogLevel.INFO)) {
			// don't log
			return;
		}
		String actualMessage = message;
		if (actualMessage == null) {
			actualMessage = "";
		}
		logInternal(LogLevel.INFO, actualMessage, null);
	}

	/**
	 * Logs a debug message to the console.
	 *
	 * @param message
	 *            The message to log. If <code>null</code>, the empty string will be used instead.
	 */
	public static void logDebug(final String message) {
		if (CCC.MIN_LOG_LEVEL.isGreaterThan(LogLevel.DEBUG)) {
			// don't log
			return;
		}
		String actualMessage = message;
		if (actualMessage == null) {
			actualMessage = "";
		}
		logInternal(LogLevel.DEBUG, actualMessage, null);
	}

	/**
	 * Logs a warning message to the console.
	 *
	 * @param message
	 *            The message to log. If <code>null</code>, the empty string will be used instead.
	 */
	public static void logWarning(final String message) {
		if (CCC.MIN_LOG_LEVEL.isGreaterThan(LogLevel.WARN)) {
			// don't log
			return;
		}
		String actualMessage = message;
		if (actualMessage == null) {
			actualMessage = "";
		}
		logInternal(LogLevel.WARN, actualMessage, null);
	}

	/**
	 * Logs an error message to the console.<br>
	 * <b>NOTE</b>: By default, messages classified as ERROR will appear on <code>System.err</code> instead of
	 * <code>System.out</code>.
	 *
	 * @param message
	 *            The message to log. If <code>null</code>, the empty string will be used instead.
	 */
	public static void logError(final String message) {
		if (CCC.MIN_LOG_LEVEL.isGreaterThan(LogLevel.ERROR)) {
			// don't log
			return;
		}
		String actualMessage = message;
		if (actualMessage == null) {
			actualMessage = "";
		}
		logInternal(LogLevel.ERROR, actualMessage, null);
	}

	/**
	 * Logs an exception to the console.<br>
	 * <b>NOTE</b>: By default, such messages will appear on <code>System.err</code> instead of <code>System.out</code>.
	 *
	 * @param message
	 *            A textual message describing the error
	 *
	 * @param throwable
	 *            The exception to log. May be <code>null</code>.
	 */
	public static void logError(final String message, final Throwable throwable) {
		if (CCC.MIN_LOG_LEVEL.isGreaterThan(LogLevel.ERROR)) {
			// don't log
			return;
		}
		if (throwable == null) {
			return;
		}
		logInternal(LogLevel.ERROR, message, throwable);
	}

}
