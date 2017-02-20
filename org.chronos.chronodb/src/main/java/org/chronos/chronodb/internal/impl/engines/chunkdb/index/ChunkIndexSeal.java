package org.chronos.chronodb.internal.impl.engines.chunkdb.index;

import java.util.UUID;

public class ChunkIndexSeal {

	private String sealId;

	public ChunkIndexSeal() {
		this.sealId = UUID.randomUUID().toString();
	}

	public String getId() {
		return this.sealId;
	}

}
