package org.chronos.chronodb.internal.impl.stream;

import static com.google.common.base.Preconditions.*;

import java.util.function.Function;

import org.chronos.chronodb.internal.api.stream.CloseableIterator;

public class TransformingCloseableIterator<E, F> extends AbstractCloseableIterator<F> {

	private final CloseableIterator<E> iterator;
	private final Function<? super E, ? extends F> function;

	public TransformingCloseableIterator(final CloseableIterator<E> iterator,
			final Function<? super E, ? extends F> function) {
		checkNotNull(function, "Precondition violation - argument 'function' must not be NULL!");
		this.iterator = iterator;
		this.function = function;
	}

	@Override
	public F next() {
		return this.function.apply(this.iterator.next());
	}

	@Override
	protected boolean hasNextInternal() {
		return this.iterator.hasNext();
	}

	@Override
	protected void closeInternal() {
		this.iterator.close();
	}

}
