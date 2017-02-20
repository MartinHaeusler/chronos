package org.chronos.common.test.utils;

import java.util.List;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeStatistics {

	private static final Logger log = LoggerFactory.getLogger(TimeStatistics.class);

	private final Statistic statistics;

	private Long currentRunStartTime;

	public TimeStatistics() {
		this.statistics = new Statistic();
	}

	public TimeStatistics(final Statistic statistic) {
		this.statistics = statistic;
	}

	public void beginRun() {
		if (this.currentRunStartTime != null) {
			throw new IllegalStateException("A run is already going on!");
		}
		this.currentRunStartTime = System.nanoTime();
	}

	public long endRun() {
		long runEndTime = System.nanoTime();
		if (this.currentRunStartTime == null) {
			throw new IllegalArgumentException("There is no run to end!");
		}
		// convert nanoseconds to milliseconds
		long runtime = (runEndTime - this.currentRunStartTime) / 1_000_000;
		this.currentRunStartTime = null;
		this.statistics.addSample(runtime);
		return runtime;
	}

	public double getAverageRuntime() {
		return this.statistics.getAverage();
	}

	public double getVariance() {
		return this.statistics.getVariance();
	}

	public double getStandardDeviation() {
		return this.statistics.getStandardDeviation();
	}

	public long getTotalTime() {
		return Math.round(this.statistics.getSum());
	}

	public int getNumberOfRuns() {
		return this.statistics.getSampleSize();
	}

	public long getLongestRuntime() {
		return Math.round(this.statistics.getMax());
	}

	public long getShortestRuntime() {
		return Math.round(this.statistics.getMin());
	}

	public long getQ1() throws NoSuchElementException {
		return Math.round(this.statistics.getQ1());
	}

	public long getMedian() {
		return Math.round(this.statistics.getMedian());
	}

	public long getQ2() {
		return Math.round(this.statistics.getQ2());
	}

	public long getQ3() {
		return Math.round(this.statistics.getQ3());
	}

	public List<Double> getRuntimes() {
		return this.statistics.getSamples();
	}

	public String toFullString() {
		StringBuilder builder = new StringBuilder();
		builder.append("TimeStatistics[");
		builder.append("Runs: ");
		builder.append(this.getNumberOfRuns());
		builder.append(", ");
		builder.append("Total Time: ");
		builder.append(this.getTotalTime());
		builder.append("ms, ");
		builder.append("Shortest Time: ");
		builder.append(this.getShortestRuntime());
		builder.append("ms, ");
		builder.append("Longest Time: ");
		builder.append(this.getLongestRuntime());
		builder.append("ms, ");
		builder.append("Average Time: ");
		builder.append(String.format("%1.3f", this.getAverageRuntime()));
		builder.append("ms, ");
		builder.append("Variance: ");
		builder.append(String.format("%1.3f", this.getVariance()));
		builder.append("ms, ");
		builder.append("Standard Deviation: ");
		builder.append(String.format("%1.3f", this.getStandardDeviation()));
		builder.append("ms, ");
		builder.append("Q1: ");
		builder.append(this.getQ1());
		builder.append("ms, ");
		builder.append("Q2 (median): ");
		builder.append(this.getQ2());
		builder.append("ms, ");
		builder.append("Q3: ");
		builder.append(this.getQ3());
		builder.append("ms ");
		builder.append("]");
		return builder.toString();
	}

	public String toCSV() {
		StringBuilder builder = new StringBuilder();
		builder.append("TimeStatistics[");
		builder.append("Runs;");
		builder.append("Total Time (ms);");
		builder.append("Shortest Time (ms); ");
		builder.append("Longest Time (ms); ");
		builder.append("Average Time (ms); ");
		builder.append("Variance (ms); ");
		builder.append("Standard Deviation (ms); ");
		builder.append("Q1 (ms); ");
		builder.append("Q2 (median) (ms); ");
		builder.append("Q3 (ms); ");
		builder.append(" ;; ");
		builder.append(this.getNumberOfRuns());
		builder.append("; ");
		builder.append(this.getTotalTime());
		builder.append("; ");
		builder.append(this.getShortestRuntime());
		builder.append("; ");
		builder.append(this.getLongestRuntime());
		builder.append("; ");
		builder.append(String.format("%1.3f", this.getAverageRuntime()));
		builder.append("; ");
		builder.append(String.format("%1.3f", this.getVariance()));
		builder.append("; ");
		builder.append(String.format("%1.3f", this.getStandardDeviation()));
		builder.append("; ");
		builder.append(this.getQ1());
		builder.append("; ");
		builder.append(this.getQ2());
		builder.append("; ");
		builder.append(this.getQ3());
		builder.append("; ");
		builder.append("]");
		return builder.toString();
	}

	public void log() {
		log.info(this.toFullString());
	}
}
