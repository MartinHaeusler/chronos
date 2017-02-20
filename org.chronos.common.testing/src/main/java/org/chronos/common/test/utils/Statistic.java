package org.chronos.common.test.utils;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.collect.Lists;

/**
 * This is a thread-safe list of statistic samples that is capable of providing standard statistic information.
 *
 * <p>
 * The "samples" are stored as <code>double</code> values. The meaning of each sample is decided by the application that uses this class. Please note that this class is intended only for samples which are greater than zero.
 *
 * <p>
 * This class is completely thread-safe. All operations internally make use of a {@link ReadWriteLock} for synchronization.
 *
 * <p>
 * Instances of this class can be created using the constructor. No additional handling is required.
 *
 * <p>
 * Usage example:
 *
 * <pre>
 * Statistics statistics = new Statistics();
 * statistics.addSample(123);
 * statistics.addSample(456);
 * // ... add more ...
 *
 * // retrieve the statistic information
 * double average = statistics.getAverage();
 * double variance = statistics.getVariance();
 * double standardDeviation = statistics.getStandardDeviation();
 * </pre>
 *
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public class Statistic {

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	/** The read-write lock used for access synchronization on the samples */
	private ReadWriteLock lock = new ReentrantReadWriteLock(true);
	/** The list of samples. A plain list data structure without special features. */
	private List<Double> samples = Lists.newArrayList();

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	/**
	 * Adds the given sample to this statistic.
	 *
	 * @param sample
	 *            The sample to add. Must be greater than zero.
	 */
	public void addSample(final double sample) {
		checkArgument(sample >= 0, "Precondition violation - argument 'sample' must be >= 0 (value: " + sample + ")!");
		this.lock.writeLock().lock();
		try {
			this.samples.add(sample);
		} finally {
			this.lock.writeLock().unlock();
		}
	}

	/**
	 * Returns an immutable list containing the current samples.
	 *
	 * @return An immutable list with the current samples. May be empty, but never <code>null</code>.
	 */
	public List<Double> getSamples() {
		this.lock.readLock().lock();
		try {
			List<Double> resultList = Lists.newArrayList(this.samples);
			return Collections.unmodifiableList(resultList);
		} finally {
			this.lock.readLock().unlock();
		}
	}

	/**
	 * Returns the average of all samples.
	 *
	 * @return The average value.
	 */
	public double getAverage() {
		this.lock.readLock().lock();
		try {
			double sum = this.getSum();
			int sampleSize = this.getSampleSize();
			return sum / sampleSize;
		} finally {
			this.lock.readLock().unlock();
		}
	}

	/**
	 * Returns the variance of all samples.
	 *
	 * @return The variance.
	 */
	public double getVariance() {
		this.lock.readLock().lock();
		try {
			double avg = this.getAverage();
			int sampleSize = this.getSampleSize();
			double variance = this.samples.parallelStream().mapToDouble(time -> Math.pow(time - avg, 2)).sum()
					/ sampleSize;
			return variance;
		} finally {
			this.lock.readLock().unlock();
		}
	}

	/**
	 * Returns the standard deviation of all samples.
	 *
	 * @return The standard deviation.
	 */
	public double getStandardDeviation() {
		this.lock.readLock().lock();
		try {
			return Math.sqrt(this.getVariance());
		} finally {
			this.lock.readLock().unlock();
		}
	}

	/**
	 * Returns the sum of all samples.
	 *
	 * @return The sum.
	 */
	public double getSum() {
		this.lock.readLock().lock();
		try {
			return this.samples.parallelStream().mapToDouble(l -> l).sum();
		} finally {
			this.lock.readLock().unlock();
		}
	}

	/**
	 * Returns the sample size, i.e. the number of samples in this statistic.
	 *
	 * @return The sample size.
	 */
	public int getSampleSize() {
		this.lock.readLock().lock();
		try {
			return this.samples.size();
		} finally {
			this.lock.readLock().unlock();
		}
	}

	/**
	 * Returns the largest sample in this statistic.
	 *
	 * @return The largest sample.
	 *
	 * @throws NoSuchElementException
	 *             Thrown if this statistic is empty and therefore has no maximum sample.
	 */
	public double getMax() throws NoSuchElementException {
		this.assertNotEmpty();
		this.lock.readLock().lock();
		try {
			return this.samples.stream().mapToDouble(v -> v).max().getAsDouble();
		} finally {
			this.lock.readLock().unlock();
		}
	}

	/**
	 * Returns the smallest sample in this statistic.
	 *
	 * @return The smallest sample.
	 *
	 * @throws NoSuchElementException
	 *             Thrown if this statistic is empty and therefore has no minimum sample.
	 */
	public double getMin() throws NoSuchElementException {
		this.assertNotEmpty();
		this.lock.readLock().lock();
		try {
			return this.samples.stream().mapToDouble(v -> v).min().getAsDouble();
		} finally {
			this.lock.readLock().unlock();
		}
	}

	/**
	 * Checks if this statistic is empty, i.e. contains no samples.
	 *
	 * @return <code>true</code> if this statistic has no samples, <code>false</code> if there is at least one sample.
	 */
	public boolean isEmpty() {
		this.lock.readLock().lock();
		try {
			return this.samples.isEmpty();
		} finally {
			this.lock.readLock().unlock();
		}
	}

	/**
	 * Gets the first quartile of the statistic.
	 *
	 * @return The first quartile.
	 *
	 * @throws NoSuchElementException
	 *             Thrown if this statistic is empty and therefore has no first quartile.
	 */
	public double getQ1() throws NoSuchElementException {
		this.assertNotEmpty();
		return this.getPercentile(25);
	}

	/**
	 * Gets the median (second quartile) of the statistic.
	 *
	 * @return The median.
	 *
	 * @throws NoSuchElementException
	 *             Thrown if this statistic is empty and therefore has no median.
	 */
	public double getMedian() {
		this.assertNotEmpty();
		return this.getQ2();
	}

	/**
	 * Gets the second quartile of the statistic.
	 *
	 * @return The second quartile.
	 *
	 * @throws NoSuchElementException
	 *             Thrown if this statistic is empty and therefore has no second quartile.
	 */
	public double getQ2() {
		this.assertNotEmpty();
		return this.getPercentile(50);
	}

	/**
	 * Gets the third quartile of the statistic.
	 *
	 * @return The third quartile.
	 *
	 * @throws NoSuchElementException
	 *             Thrown if this statistic is empty and therefore has no third quartile.
	 */
	public double getQ3() {
		this.assertNotEmpty();
		return this.getPercentile(75);
	}

	private double getPercentile(final double percent) {
		this.lock.readLock().lock();
		try {
			List<Double> sorted = Lists.newArrayList(this.samples);
			Collections.sort(sorted);
			int index = (int) Math.round(sorted.size() * percent / 100);
			index = Math.min(index, sorted.size() - 1);
			return sorted.get(index);
		} finally {
			this.lock.readLock().unlock();
		}
	}

	private void assertNotEmpty() {
		if (this.isEmpty()) {
			throw new NoSuchElementException("Statistic contains no data points!");
		}
	}

}
