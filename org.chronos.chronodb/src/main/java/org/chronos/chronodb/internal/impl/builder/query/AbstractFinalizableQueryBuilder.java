package org.chronos.chronodb.internal.impl.builder.query;

import java.util.Iterator;
import java.util.Map.Entry;

import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.builder.query.QueryBuilderFinalizer;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.internal.api.ChronoDBInternal;
import org.chronos.chronodb.internal.api.query.ChronoDBQuery;
import org.chronos.chronodb.internal.util.ImmutableMapEntry;

public abstract class AbstractFinalizableQueryBuilder implements QueryBuilderFinalizer {

	@Override
	public Iterator<QualifiedKey> getKeys() {
		ChronoDBQuery query = this.getQuery();
		// evaluate the query
		Branch branch = this.getBranch();
		long timestamp = this.getTx().getTimestamp();
		return this.getOwningDB().getIndexManager().evaluate(timestamp, branch, query);
	}

	@Override
	public Iterator<Entry<QualifiedKey, Object>> getQualifiedResult() {
		ChronoDBQuery query = this.getQuery();
		// evaluate the query
		Branch branch = this.getBranch();
		long timestamp = this.getTx().getTimestamp();
		Iterator<QualifiedKey> keyIterator = this.getOwningDB().getIndexManager().evaluate(timestamp, branch, query);
		return new QualifiedResultIterator(keyIterator);
	}

	@Override
	public Iterator<Entry<String, Object>> getResult() {
		ChronoDBQuery query = this.getQuery();
		// evaluate the query
		Branch branch = this.getBranch();
		long timestamp = this.getTx().getTimestamp();
		Iterator<QualifiedKey> keyIterator = this.getOwningDB().getIndexManager().evaluate(timestamp, branch, query);
		return new UnqualifiedResultIterator(keyIterator);
	}

	@Override
	public Iterator<Object> getValues() {
		ChronoDBQuery query = this.getQuery();
		// evaluate the query
		Branch branch = this.getBranch();
		long timestamp = this.getTx().getTimestamp();
		Iterator<QualifiedKey> keyIterator = this.getOwningDB().getIndexManager().evaluate(timestamp, branch, query);
		return new ValuesResultIterator(keyIterator);
	}

	protected Branch getBranch() {
		String branchName = this.getTx().getBranchName();
		Branch branch = this.getOwningDB().getBranchManager().getBranch(branchName);
		return branch;
	}

	protected abstract ChronoDBQuery getQuery();

	protected abstract ChronoDBInternal getOwningDB();

	protected abstract ChronoDBTransaction getTx();

	private class QualifiedResultIterator implements Iterator<Entry<QualifiedKey, Object>> {

		private final Iterator<QualifiedKey> keysIterator;

		public QualifiedResultIterator(final Iterator<QualifiedKey> keysIterator) {
			this.keysIterator = keysIterator;
		}

		@Override
		public boolean hasNext() {
			return this.keysIterator.hasNext();
		}

		@Override
		public Entry<QualifiedKey, Object> next() {
			QualifiedKey qKey = this.keysIterator.next();
			Object value = AbstractFinalizableQueryBuilder.this.getTx().get(qKey.getKeyspace(), qKey.getKey());
			Entry<QualifiedKey, Object> entry = ImmutableMapEntry.create(qKey, value);
			return entry;
		}
	}

	private class UnqualifiedResultIterator implements Iterator<Entry<String, Object>> {

		private final Iterator<QualifiedKey> keysIterator;

		public UnqualifiedResultIterator(final Iterator<QualifiedKey> keysIterator) {
			this.keysIterator = keysIterator;
		}

		@Override
		public boolean hasNext() {
			return this.keysIterator.hasNext();
		}

		@Override
		public Entry<String, Object> next() {
			QualifiedKey qKey = this.keysIterator.next();
			String keyspace = qKey.getKeyspace();
			String key = qKey.getKey();
			Object value = AbstractFinalizableQueryBuilder.this.getTx().get(keyspace, key);
			Entry<String, Object> entry = ImmutableMapEntry.create(key, value);
			return entry;
		}
	}

	private class ValuesResultIterator implements Iterator<Object> {

		private final Iterator<QualifiedKey> keysIterator;

		public ValuesResultIterator(final Iterator<QualifiedKey> keysIterator) {
			this.keysIterator = keysIterator;
		}

		@Override
		public boolean hasNext() {
			return this.keysIterator.hasNext();
		}

		@Override
		public Object next() {
			QualifiedKey qKey = this.keysIterator.next();
			Object value = AbstractFinalizableQueryBuilder.this.getTx().get(qKey.getKeyspace(), qKey.getKey());
			return value;
		}

	}
}