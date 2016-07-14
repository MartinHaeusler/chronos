package org.chronos.chronodb.internal.impl.stream;

import org.chronos.chronodb.internal.api.stream.CloseableIterator;

public abstract class AbstractCloseableIterator<T> implements CloseableIterator<T> {

	private boolean closed;

	@Override
	public final boolean hasNext() {
		if (this.closed) {
			return false;
		}
		return this.hasNextInternal();
	}

	@Override
	public final void close() {
		if (this.isClosed()) {
			return;
		}
		this.closeInternal();
		this.closed = true;
	}

	@Override
	public final boolean isClosed() {
		return this.closed;
	}

	protected abstract boolean hasNextInternal();

	protected abstract void closeInternal();

}
