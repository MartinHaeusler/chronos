package org.chronos.chronograph.internal.impl.structure.graph;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.GraphVariableHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.internal.ChronoGraphConstants;
import org.chronos.chronograph.internal.impl.transaction.GraphTransactionContext;

/**
 * Implementation of the Gremlin {@Link Graph.Variables} interface.
 *
 * <p>
 * This implementation persists the graph variables alongside the actual graph data, i.e.
 * <code>graph.tx().commit()</code> will also commit the graph variables, and <code>graph.tx().rollback()</code> will
 * remove any changes.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public class ChronoGraphVariables implements Graph.Variables {

	private final ChronoGraph graph;

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	public ChronoGraphVariables(final ChronoGraph graph) {
		checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
		this.graph = graph;
	}

	// =====================================================================================================================
	// TINKERPOP API
	// =====================================================================================================================

	@Override
	public Set<String> keys() {
		this.readWrite();
		GraphTransactionContext context = this.getContext();
		ChronoDBTransaction tx = this.getTransaction().getBackingDBTransaction();
		Set<String> keySet = tx.keySet(ChronoGraphConstants.KEYSPACE_VARIABLES);
		keySet.addAll(context.getModifiedVariables());
		keySet.removeAll(context.getRemovedVariables());
		return Collections.unmodifiableSet(keySet);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <R> Optional<R> get(final String key) {
		this.readWrite();
		if (this.keys().contains(key) == false) {
			// the key set doesn't contain the variable, return empty
			return Optional.empty();
		}
		GraphTransactionContext context = this.getContext();
		if (context.isVariableModified(key)) {
			// return the modified value
			return Optional.ofNullable((R) context.getModifiedVariableValue(key));
		} else {
			// query the db for the original value
			ChronoDBTransaction tx = this.getTransaction().getBackingDBTransaction();
			return Optional.ofNullable((R) tx.get(ChronoGraphConstants.KEYSPACE_VARIABLES, key));
		}
	}

	@Override
	public void remove(final String key) {
		this.readWrite();
		if (this.keys().contains(key) == false) {
			return;
		}
		GraphTransactionContext context = this.getContext();
		context.removeVariable(key);
	}

	@Override
	public void set(final String key, final Object value) {
		GraphVariableHelper.validateVariable(key, value);
		this.readWrite();
		GraphTransactionContext context = this.getContext();
		context.setVariableValue(key, value);
	}

	@Override
	public String toString() {
		this.readWrite();
		return StringFactory.graphVariablesString(this);
	}

	// =====================================================================================================================
	// INTERNAL HELPER METHODS
	// =====================================================================================================================

	protected ChronoGraphTransaction getTransaction() {
		return this.graph.tx().getCurrentTransaction();
	}

	protected void readWrite() {
		this.graph.tx().readWrite();
	}

	protected GraphTransactionContext getContext() {
		return this.getTransaction().getContext();
	}

}
