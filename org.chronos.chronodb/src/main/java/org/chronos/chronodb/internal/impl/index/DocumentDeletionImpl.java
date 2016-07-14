package org.chronos.chronodb.internal.impl.index;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.internal.api.index.ChronoIndexDocument;
import org.chronos.chronodb.internal.api.index.DocumentDeletion;

public class DocumentDeletionImpl implements DocumentDeletion {

	private final ChronoIndexDocument documentToDelete;

	public DocumentDeletionImpl(final ChronoIndexDocument documentToDelete) {
		checkNotNull(documentToDelete, "Precondition violation - argument 'documentToDelete' must not be NULL!");
		this.documentToDelete = documentToDelete;
	}

	@Override
	public ChronoIndexDocument getDocumentToDelete() {
		return this.documentToDelete;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (this.documentToDelete == null ? 0 : this.documentToDelete.hashCode());
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (obj instanceof DocumentDeletion == false) {
			return false;
		}
		DocumentDeletion other = (DocumentDeletion) obj;
		if (this.documentToDelete == null) {
			if (other.getDocumentToDelete() != null) {
				return false;
			}
		} else if (!this.documentToDelete.equals(other.getDocumentToDelete())) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("DELETE(");
		builder.append(this.documentToDelete.getIndexName());
		builder.append("->");
		builder.append(this.documentToDelete.getKeyspace());
		builder.append("->");
		builder.append(this.documentToDelete.getKey());
		builder.append("->");
		builder.append(this.documentToDelete.getIndexedValue());
		builder.append(")");
		return builder.toString();
	}
}
