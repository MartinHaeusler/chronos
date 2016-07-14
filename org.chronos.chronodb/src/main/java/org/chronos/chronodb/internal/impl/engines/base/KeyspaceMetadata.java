package org.chronos.chronodb.internal.impl.engines.base;

import static com.google.common.base.Preconditions.*;

import java.io.Serializable;

public class KeyspaceMetadata implements Serializable {

	private String keyspaceName;
	private String matrixTableName;
	private long creationTimestamp;

	protected KeyspaceMetadata() {
		// default constructor for serialization
	}

	public KeyspaceMetadata(final String keyspaceName, final String matrixTableName, final long creationTimestamp) {
		checkNotNull(keyspaceName, "Precondition violation - argument 'keyspaceName' must not be NULL!");
		checkNotNull(matrixTableName, "Precondition violation - argument 'matrixTableName' must not be NULL!");
		checkArgument(creationTimestamp >= 0, "Precondition violation - argument 'creationTimestamp' must not be negative!");
		this.keyspaceName = keyspaceName;
		this.matrixTableName = matrixTableName;
		this.creationTimestamp = creationTimestamp;
	}

	public String getKeyspaceName() {
		return this.keyspaceName;
	}

	public String getMatrixTableName() {
		return this.matrixTableName;
	}

	public long getCreationTimestamp() {
		return this.creationTimestamp;
	}

}
