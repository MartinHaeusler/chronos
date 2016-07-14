package org.chronos.chronodb.internal.impl.stream;

import static com.google.common.base.Preconditions.*;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.chronos.chronodb.internal.api.stream.CloseableIterator;

public class ConcatenatedCloseableIterator<T> extends AbstractCloseableIterator<T> {

	private final Iterator<CloseableIterator<T>> iterator;
	private CloseableIterator<T> currentIterator;

	public ConcatenatedCloseableIterator(final Iterator<CloseableIterator<T>> iterator) {
		checkNotNull(iterator, "Precondition violation - argument 'iterator' must not be NULL!");
		this.iterator = iterator;
		if (iterator.hasNext()) {
			this.currentIterator = iterator.next();
		} else {
			this.currentIterator = null;
		}
	}

	@Override
	protected boolean hasNextInternal() {
		if (this.currentIterator != null && this.currentIterator.hasNext()) {
			// there are remaining entries in our current iterator
			return true;
		}
		// the current stream is empty, move to the next stream
		boolean foundNextIterator = this.moveToNextIterator();
		if (foundNextIterator) {
			// we have loaded the next non-empty iterator
			return true;
		} else {
			// there are no more iterators...
			return false;
		}
	}

	@Override
	public T next() {
		if (this.hasNext() == false) {
			throw new NoSuchElementException();
		}
		return this.currentIterator.next();
	}

	@Override
	protected void closeInternal() {
		if (this.currentIterator != null) {
			this.currentIterator.close();
		}
	}

	// =====================================================================================================================
	// INTERNAL HELPER METHODS
	// =====================================================================================================================

	private boolean moveToNextIterator() {
		// close the current iterator (if any)
		if (this.currentIterator != null) {
			this.currentIterator.close();
		}
		while (true) {
			if (this.iterator.hasNext() == false) {
				// no more streams avaliable
				this.currentIterator = null;
				return false;
			}
			CloseableIterator<T> nextIterator = this.iterator.next();
			if (nextIterator.hasNext() == false) {
				// iterator is empty; close it and move to the next
				nextIterator.close();
			} else {
				// this is our next iterator
				this.currentIterator = nextIterator;
				return true;
			}
		}
	}
}