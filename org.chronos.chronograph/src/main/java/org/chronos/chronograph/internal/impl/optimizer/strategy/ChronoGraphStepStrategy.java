package org.chronos.chronograph.internal.impl.optimizer.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.chronos.chronograph.internal.impl.optimizer.step.ChronoGraphStep;

public class ChronoGraphStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
		implements TraversalStrategy.ProviderOptimizationStrategy {

	// =====================================================================================================================
	// SINGLETON IMPLEMENTATION
	// =====================================================================================================================

	private static final ChronoGraphStepStrategy INSTANCE;

	public static ChronoGraphStepStrategy getInstance() {
		return INSTANCE;
	}

	static {
		INSTANCE = new ChronoGraphStepStrategy();
	}

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	/**
	 * This constructor is private on purpose.
	 *
	 * <p>
	 * Please use {@link #getInstance()} to retrieve the singleton instance of this class.
	 */
	private ChronoGraphStepStrategy() {
	}

	// =====================================================================================================================
	// TINKERPOP API
	// =====================================================================================================================

	@Override
	@SuppressWarnings("unchecked")
	public void apply(final Traversal.Admin<?, ?> traversal) {
		// first of all, get all graph steps in our traversal
		TraversalHelper.getStepsOfClass(GraphStep.class, traversal).forEach(originalGraphStep -> {
			// create a ChronoGraphStep for each original step (it wraps the original step)
			final ChronoGraphStep<?, ?> chronoGraphStep = new ChronoGraphStep<>(originalGraphStep);
			// replace the original step with the ChronoGraphStep in the traversal
			TraversalHelper.replaceStep(originalGraphStep, (Step<?, ?>) chronoGraphStep, traversal);
			// now we collapse all "has('property','value')" gremlin steps into one ChronoGraphStep
			// note: in the terminology here, a "Has Container" is a "has"-step in gremlin!
			Step<?, ?> currentStep = chronoGraphStep.getNextStep();
			while (currentStep instanceof HasContainerHolder) {
				// the following step is a "has" step. Add its conditions to our current step
				((HasContainerHolder) currentStep).getHasContainers().forEach(chronoGraphStep::addHasContainer);
				// we also "inherit" all labels from the next step
				currentStep.getLabels().forEach(chronoGraphStep::addLabel);
				// remove the step. It is obsolete because all 'has' conditions are now part of our own step
				traversal.removeStep(currentStep);
				// continue with the next step
				currentStep = currentStep.getNextStep();
			}
		});
	}

}
