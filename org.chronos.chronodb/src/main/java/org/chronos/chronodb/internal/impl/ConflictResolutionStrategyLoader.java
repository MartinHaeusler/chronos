package org.chronos.chronodb.internal.impl;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;

import org.chronos.chronodb.api.conflict.ConflictResolutionStrategy;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;

public class ConflictResolutionStrategyLoader {

	/**
	 * Loads a {@link ConflictResolutionStrategy} instance with the given name.
	 *
	 * <p>
	 * This is a direct implementation of the semantics specified in
	 * {@link ChronoDBConfiguration#COMMIT_CONFLICT_RESOLUTION_STRATEGY}. Please refer to its JavaDocs for details.
	 *
	 * @param name
	 *            The name of the strategy to load. If <code>null</code> or whitespace-only string is passed,
	 *            {@link ConflictResolutionStrategy#DO_NOT_MERGE} will be returned as a default. Can be the name of a
	 *            standard strategy, or the fully qualified name of a custom class that implements the
	 *            {@link ConflictResolutionStrategy} interface and has a default constructor.
	 * 
	 * @return The loaded conflict resolution strategy. Never <code>null</code>.
	 */
	public static ConflictResolutionStrategy load(final String name) {
		if (name == null || name.trim().isEmpty()) {
			// use the default
			return ConflictResolutionStrategy.DO_NOT_MERGE;
		}
		String className = name.trim();
		// check the predefined strategies
		if (className.equals("DO_NOT_MERGE")) {
			return ConflictResolutionStrategy.DO_NOT_MERGE;
		} else if (className.equals("OVERWRITE_WITH_SOURCE")) {
			return ConflictResolutionStrategy.OVERWRITE_WITH_SOURCE;
		} else if (className.equals("OVERWRITE_WITH_TARGET")) {
			return ConflictResolutionStrategy.OVERWRITE_WITH_TARGET;
		}
		// no predefined strategy matched. Try to find the custom class
		Class<?> strategyClass = null;
		try {
			strategyClass = Class.forName(className);
		} catch (Exception e) {
			throw new IllegalArgumentException(
					"The parameter " + ChronoDBConfiguration.COMMIT_CONFLICT_RESOLUTION_STRATEGY + " was set to '"
							+ className + "' which is neither a predefined strategy nor the qualified name of a class!",
					e);
		}
		if (ConflictResolutionStrategy.class.isAssignableFrom(strategyClass) == false) {
			throw new IllegalArgumentException("The parameter "
					+ ChronoDBConfiguration.COMMIT_CONFLICT_RESOLUTION_STRATEGY + " was set to '" + className
					+ "' which refers to a fully qualified class name, but that class does not implement the required interface '"
					+ ConflictResolutionStrategy.class.getName() + "'!");
		}
		if (strategyClass.isInterface() || Modifier.isAbstract(strategyClass.getModifiers())) {
			throw new IllegalArgumentException("The parameter "
					+ ChronoDBConfiguration.COMMIT_CONFLICT_RESOLUTION_STRATEGY + " was set to '" + className
					+ "' which refers to a fully qualified class name, but that class is either abstract or an interface!");
		}
		try {
			Constructor<?> constructor = strategyClass.getConstructor();
			ConflictResolutionStrategy strategy = (ConflictResolutionStrategy) constructor.newInstance();
			return strategy;
		} catch (Exception e) {
			throw new IllegalArgumentException("The parameter "
					+ ChronoDBConfiguration.COMMIT_CONFLICT_RESOLUTION_STRATEGY + " was set to '" + className
					+ "' which refers to a fully qualified class name, but that class could not be instantiated. Does it have a default (no-argument) contructor?",
					e);
		}

	}

}
