package org.chronos.chronodb.internal.impl.query.optimizer;

import org.chronos.chronodb.api.query.Condition;
import org.chronos.chronodb.internal.api.query.ChronoDBQuery;
import org.chronos.chronodb.internal.api.query.QueryOptimizer;
import org.chronos.chronodb.internal.impl.query.TextMatchMode;
import org.chronos.chronodb.internal.impl.query.parser.ast.BinaryOperatorElement;
import org.chronos.chronodb.internal.impl.query.parser.ast.BinaryQueryOperator;
import org.chronos.chronodb.internal.impl.query.parser.ast.ChronoDBQueryImpl;
import org.chronos.chronodb.internal.impl.query.parser.ast.NotElement;
import org.chronos.chronodb.internal.impl.query.parser.ast.QueryElement;
import org.chronos.chronodb.internal.impl.query.parser.ast.WhereElement;
import org.chronos.common.exceptions.UnknownEnumLiteralException;

public class StandardQueryOptimizer implements QueryOptimizer {

	@Override
	public ChronoDBQuery optimize(final ChronoDBQuery query) {
		QueryElement rootElement = query.getRootElement();
		QueryElement optimizedAST = this.pushNegationInside(rootElement);
		ChronoDBQueryImpl optimizedQuery = new ChronoDBQueryImpl(query.getKeyspace(), optimizedAST);
		return optimizedQuery;
	}

	private QueryElement pushNegationInside(final QueryElement original) {
		if (original instanceof NotElement) {
			// case 1: our AST element is a negation; push it down
			QueryElement child = ((NotElement) original).getChild();
			// drop the negation and return the negated child
			return this.getNegated(child);
		} else {
			// case 2: our AST element is not a negation; continue searching recursively
			if (original instanceof WhereElement) {
				// reached a where during scan without encountering a negation. Reuse it.
				return original;
			} else if (original instanceof BinaryOperatorElement) {
				// search recursively for negations in both child elements
				BinaryOperatorElement binaryOperatorElement = (BinaryOperatorElement) original;
				QueryElement left = this.pushNegationInside(binaryOperatorElement.getLeftChild());
				QueryElement right = this.pushNegationInside(binaryOperatorElement.getRightChild());
				// create a new binary element with the optimized children
				return new BinaryOperatorElement(left, binaryOperatorElement.getOperator(), right);
			} else {
				throw new IllegalArgumentException(
						"Encountered unknown subclass of QueryElement: '" + original.getClass().getName() + "'!");
			}
		}
	}

	private QueryElement getNegated(final QueryElement original) {
		if (original instanceof WhereElement) {
			// negate the condition, eliminate the parent "not"
			String indexName = ((WhereElement) original).getIndexName();
			Condition condition = ((WhereElement) original).getCondition();
			TextMatchMode matchMode = ((WhereElement) original).getMatchMode();
			String comparisonValue = ((WhereElement) original).getComparisonValue();
			WhereElement negatedWhere = new WhereElement(indexName, condition.getNegated(), matchMode, comparisonValue);
			return negatedWhere;
		} else if (original instanceof NotElement) {
			// double negation elimination: use what's inside the inner "not" and discard both not operators
			QueryElement child = ((NotElement) original).getChild();
			return this.pushNegationInside(child);
		} else if (original instanceof BinaryOperatorElement) {
			BinaryOperatorElement originalBinary = (BinaryOperatorElement) original;
			// invert the operator and push the negation into both children
			QueryElement negatedLeft = this.getNegated(originalBinary.getLeftChild());
			QueryElement negatedRight = this.getNegated(originalBinary.getRightChild());
			BinaryQueryOperator negatedOperator = null;
			switch (originalBinary.getOperator()) {
			case AND:
				negatedOperator = BinaryQueryOperator.OR;
				break;
			case OR:
				negatedOperator = BinaryQueryOperator.AND;
				break;
			default:
				throw new UnknownEnumLiteralException("Encountered unknown literal of BinaryOperatorElement: '"
						+ originalBinary.getOperator() + "'!");
			}
			return new BinaryOperatorElement(negatedLeft, negatedOperator, negatedRight);
		} else {
			throw new IllegalArgumentException(
					"Encountered unknown subclass of QueryElement: '" + original.getClass().getName() + "'!");
		}
	}
}
