package org.chronos.chronosphere.impl.query;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.chronos.chronosphere.api.query.EObjectQueryStepBuilder;
import org.chronos.chronosphere.api.query.NumericQueryStepBuilder;
import org.chronos.chronosphere.api.query.QueryStepBuilder;
import org.chronos.chronosphere.api.query.QueryStepBuilderInternal;
import org.chronos.chronosphere.api.query.UntypedQueryStepBuilder;
import org.eclipse.emf.ecore.EObject;

public class ObjectQueryStepBuilderImpl<S, E> extends AbstractQueryStepBuilder<S, E>
		implements UntypedQueryStepBuilder<S, E> {

	public ObjectQueryStepBuilderImpl(final QueryStepBuilderInternal<S, ?> previous,
			final GraphTraversal<?, E> traversal) {
		super(previous, traversal);
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	@Override
	public EObjectQueryStepBuilder<S> asEObject() {
		this.assertModificationsAllowed();
		GraphTraversal<?, EObject> traversal = this.getTraversal().filter(traverser -> {
			Object value = traverser.get();
			if (value == null) {
				return false;
			}
			if (value instanceof EObject == false) {
				return false;
			}
			return true;
		}).map(traverser -> (EObject) traverser.get());
		return new EObjectQueryStepBuilderImpl<S>(this, traversal);
	}

	@Override
	public QueryStepBuilder<S, Boolean> asBoolean() {
		this.assertModificationsAllowed();
		return new ObjectQueryStepBuilderImpl<S, Boolean>(this, this.castTraversalTo(Boolean.class));
	}

	@Override
	public NumericQueryStepBuilder<S, Byte> asByte() {
		this.assertModificationsAllowed();
		return new NumericQueryStepBuilderImpl<S, Byte>(this, this.castTraversalToNumeric(Number::byteValue));
	}

	@Override
	public NumericQueryStepBuilder<S, Short> asShort() {
		this.assertModificationsAllowed();
		return new NumericQueryStepBuilderImpl<S, Short>(this, this.castTraversalToNumeric(Number::shortValue));
	}

	@Override
	public QueryStepBuilder<S, Character> asCharacter() {
		this.assertModificationsAllowed();
		return new ObjectQueryStepBuilderImpl<S, Character>(this, this.castTraversalTo(Character.class));
	}

	@Override
	public NumericQueryStepBuilder<S, Integer> asInteger() {
		this.assertModificationsAllowed();
		return new NumericQueryStepBuilderImpl<S, Integer>(this, this.castTraversalToNumeric(Number::intValue));
	}

	@Override
	public NumericQueryStepBuilder<S, Long> asLong() {
		this.assertModificationsAllowed();
		return new NumericQueryStepBuilderImpl<S, Long>(this, this.castTraversalToNumeric(Number::longValue));
	}

	@Override
	public NumericQueryStepBuilder<S, Float> asFloat() {
		this.assertModificationsAllowed();
		return new NumericQueryStepBuilderImpl<S, Float>(this, this.castTraversalToNumeric(Number::floatValue));
	}

	@Override
	public NumericQueryStepBuilder<S, Double> asDouble() {
		this.assertModificationsAllowed();
		return new NumericQueryStepBuilderImpl<S, Double>(this, this.castTraversalToNumeric(Number::doubleValue));
	}

}
