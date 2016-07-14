package org.chronos.chronodb.internal.impl.builder.query;

import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

import org.chronos.chronodb.internal.api.query.QueryTokenStream;
import org.chronos.chronodb.internal.impl.query.parser.token.QueryToken;

import com.google.common.collect.Lists;

public class StandardQueryTokenStream implements QueryTokenStream {

	private final List<QueryToken> elements;

	private int currentIndex;

	public StandardQueryTokenStream(final List<QueryToken> elements) {
		this.elements = Collections.unmodifiableList(Lists.newArrayList(elements));
		this.currentIndex = 0;
	}

	@Override
	public QueryToken getToken() {
		QueryToken token = this.lookAhead();
		this.currentIndex++;
		return token;
	}

	@Override
	public QueryToken lookAhead() {
		if (this.hasNextToken() == false) {
			throw new NoSuchElementException("No more tokens available; stream is empty!");
		}
		return this.elements.get(this.currentIndex);
	}

	@Override
	public boolean hasNextToken() {
		if (this.currentIndex >= this.elements.size()) {
			return false;
		}
		return true;
	}

	@Override
	public boolean isAtStartOfInput() {
		return this.currentIndex == 0;
	}

}
