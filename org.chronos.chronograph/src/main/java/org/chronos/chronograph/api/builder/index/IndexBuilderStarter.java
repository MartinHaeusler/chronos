package org.chronos.chronograph.api.builder.index;

import org.chronos.chronograph.api.index.ChronoGraphIndexManager;

/**
 * The first step of the fluent builder API for creating graph indices.
 *
 * <p>
 * You can get an instance of this class by calling {@link ChronoGraphIndexManager#create()}.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
public interface IndexBuilderStarter {

	/**
	 * Creates an index where individual values are strings.
	 *
	 * @return The builder, for method chaining. Never <code>null</code>.
	 */
	public ElementTypeChoiceIndexBuilder stringIndex();

	/**
	 * Creates an index where individual values are longs.
	 *
	 * <p>
	 * Besides {@link Long}, there are also several other classes that work with this type of index (conversion to {@link Long} takes place automatically):
	 * <ul>
	 * <li>{@link Byte}
	 * <li>{@link Short}
	 * <li>{@link Integer}
	 * </ul>
	 *
	 * @return The builder, for method chaining. Never <code>null</code>.
	 */
	public ElementTypeChoiceIndexBuilder longIndex();

	/**
	 * Creates an index where individual values are doubles.
	 *
	 * <p>
	 * Besides {@link Double}, this index is also compatible with values of type {@link Float}, which will automatically be converted to {@link Double}.
	 *
	 * @return The builder, for method chaining. Never <code>null</code>.
	 */
	public ElementTypeChoiceIndexBuilder doubleIndex();

}
