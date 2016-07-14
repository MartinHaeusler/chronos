package org.chronos.common.test.utils;

import java.util.Map;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class Measure {

	private static final Logger log = LoggerFactory.getLogger(Measure.class);

	private static final Map<String, Long> idToStartTime = Maps.newHashMap();

	public static void startTimeMeasure(final String id) {
		if (id == null) {
			throw new IllegalArgumentException("Precondition violation - argument 'id' must not be NULL!");
		}
		long startTime = System.nanoTime();
		idToStartTime.put(id, startTime);
	}

	public static long endTimeMeasure(final String id) {
		long endTime = System.nanoTime();
		if (id == null) {
			throw new IllegalArgumentException("Precondition violation - argument 'id' must not be NULL!");
		}
		Long startTime = idToStartTime.remove(id);
		if (startTime == null) {
			throw new IllegalStateException("A timer for '" + id + "' does not exist!");
		}
		long runtime = (endTime - startTime) / 1000000;
		log.debug("Time for action [" + id + "]: " + runtime + "ms.");
		return runtime;
	}

	public static TimeStatistics multipleTimes(final int times, final boolean writeLog, final Runnable task) {
		TimeStatistics statistics = new TimeStatistics();
		for (int run = 0; run < times; run++) {
			statistics.beginRun();
			task.run();
			statistics.endRun();
		}
		if (writeLog) {
			statistics.log();
		}
		return statistics;
	}

	public static TimeStatistics multipleTimes(final int times, final boolean writeLog, final Consumer<Integer> task) {
		TimeStatistics statistics = new TimeStatistics();
		for (int run = 0; run < times; run++) {
			statistics.beginRun();
			task.accept(run);
			statistics.endRun();
		}
		if (writeLog) {
			statistics.log();
		}
		return statistics;
	}

}
