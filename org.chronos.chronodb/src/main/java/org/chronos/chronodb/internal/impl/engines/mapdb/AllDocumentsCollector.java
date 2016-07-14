package org.chronos.chronodb.internal.impl.engines.mapdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.SimpleCollector;

import com.google.common.collect.Lists;

public class AllDocumentsCollector extends SimpleCollector {

	private final List<Document> documents;
	private LeafReader currentReader;

	public AllDocumentsCollector() {
		this.documents = Lists.newArrayList();
	}

	public AllDocumentsCollector(final int numDocs) {
		this.documents = new ArrayList<>(numDocs);
	}

	public List<Document> getDocuments() {
		return Collections.unmodifiableList(this.documents);
	}

	@Override
	protected void doSetNextReader(final LeafReaderContext context) {
		this.currentReader = context.reader();
	}

	@Override
	public void collect(final int doc) throws IOException {
		this.documents.add(this.currentReader.document(doc));
	}

	@Override
	public boolean needsScores() {
		return false;
	}

}
