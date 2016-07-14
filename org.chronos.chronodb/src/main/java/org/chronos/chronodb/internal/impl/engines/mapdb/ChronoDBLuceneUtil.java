package org.chronos.chronodb.internal.impl.engines.mapdb;

import static com.google.common.base.Preconditions.*;

import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.internal.api.index.ChronoIndexDocument;
import org.chronos.chronodb.internal.impl.index.ChronoIndexDocumentImpl;

import com.google.common.collect.Sets;

public class ChronoDBLuceneUtil {

	public static final String DOCUMENT_FIELD_ID = "id";
	public static final String DOCUMENT_FIELD_KEYSPACE = "keyspace";
	public static final String DOCUMENT_FIELD_KEY = "key";
	public static final String DOCUMENT_FIELD_INDEX_NAME = "indexName";
	public static final String DOCUMENT_FIELD_INDEXED_VALUE = "indexedValue";
	public static final String DOCUMENT_FIELD_INDEXED_VALUE_CI = "indexedValueCI";
	public static final String DOCUMENT_FIELD_VALID_FROM = "validFrom";
	public static final String DOCUMENT_FIELD_VALID_TO = "validTo";
	public static final String DOCUMENT_FIELD_BRANCH = "branch";

	public static ChronoIndexDocument convertLuceneDocumentToChronoDocument(final Document luceneDocument) {
		checkNotNull(luceneDocument, "Precondition violation - argument 'luceneDocument' must not be NULL!");
		String id = luceneDocument.get(DOCUMENT_FIELD_ID);
		String indexName = luceneDocument.get(DOCUMENT_FIELD_INDEX_NAME);
		String branch = luceneDocument.get(DOCUMENT_FIELD_BRANCH);
		if (branch == null) {
			branch = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER;
		}
		String keyspace = luceneDocument.get(DOCUMENT_FIELD_KEYSPACE);
		String key = luceneDocument.get(DOCUMENT_FIELD_KEY);
		String indexedValue = luceneDocument.get(DOCUMENT_FIELD_INDEXED_VALUE);
		String indexedValueCaseInsensitive = luceneDocument.get(DOCUMENT_FIELD_INDEXED_VALUE_CI);
		long validFrom = (long) luceneDocument.getField(DOCUMENT_FIELD_VALID_FROM).numericValue();
		long validTo = (long) luceneDocument.getField(DOCUMENT_FIELD_VALID_TO).numericValue();
		return new ChronoIndexDocumentImpl(id, indexName, branch, keyspace, key, indexedValue,
				indexedValueCaseInsensitive, validFrom, validTo);
	}

	public static Document convertChronoDocumentToLuceneDocument(final ChronoIndexDocument chronoDocument) {
		checkNotNull(chronoDocument, "Precondition violation - argument 'chronoDocument' must not be NULL!");
		Document document = new Document();
		document.add(new StringField(DOCUMENT_FIELD_ID, chronoDocument.getDocumentId(), Store.YES));
		document.add(new StringField(DOCUMENT_FIELD_BRANCH, chronoDocument.getBranch(), Store.YES));
		document.add(new StringField(DOCUMENT_FIELD_KEYSPACE, chronoDocument.getKeyspace(), Store.YES));
		document.add(new StringField(DOCUMENT_FIELD_KEY, chronoDocument.getKey(), Store.YES));
		document.add(new StringField(DOCUMENT_FIELD_INDEX_NAME, chronoDocument.getIndexName(), Store.YES));
		document.add(new StringField(DOCUMENT_FIELD_INDEXED_VALUE, chronoDocument.getIndexedValue(), Store.YES));
		document.add(new StringField(DOCUMENT_FIELD_INDEXED_VALUE_CI, chronoDocument.getIndexedValueCaseInsensitive(),
				Store.YES));
		document.add(new LongField(DOCUMENT_FIELD_VALID_FROM, chronoDocument.getValidFromTimestamp(), Store.YES));
		document.add(new LongField(DOCUMENT_FIELD_VALID_TO, chronoDocument.getValidToTimestamp(), Store.YES));
		return document;
	}

	public static Set<ChronoIndexDocument> convertLuceneDocumentsToChronoDocuments(
			final Iterable<Document> luceneDocuments) {
		checkNotNull(luceneDocuments, "Precondition violation - argument 'luceneDocuments' must not be NULL!");
		Set<ChronoIndexDocument> chronoDocuments = Sets.newHashSet();
		for (Document luceneDocument : luceneDocuments) {
			ChronoIndexDocument chronoDocument = ChronoDBLuceneUtil
					.convertLuceneDocumentToChronoDocument(luceneDocument);
			chronoDocuments.add(chronoDocument);
		}
		return chronoDocuments;
	}
}
