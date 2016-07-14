package org.chronos.chronodb.internal.impl.engines.base;

import static com.google.common.base.Preconditions.*;

public class WriteAheadLogToken {

	private long nowTimestampBeforeCommit;
	private long nowTimestampAfterCommit;

	protected WriteAheadLogToken() {
		// no-args for serialization
	}

	public WriteAheadLogToken(final long nowTimestampBeforeCommit, final long nowTimestampAfterCommit) {
		checkArgument(nowTimestampBeforeCommit >= 0, "Precondition violation - argument 'nowTimestampBeforeCommit' must not be negative!");
		checkArgument(nowTimestampAfterCommit >= 0, "Precondition violation - argument 'nowTimestampAfterCommit' must not be negative!");
		checkArgument(nowTimestampAfterCommit > nowTimestampBeforeCommit, "Precondition violation - argument 'nowTimestampAfterCommit' must be strictly greater than 'nowTimestampBeforeCommit'!");
		this.nowTimestampBeforeCommit = nowTimestampBeforeCommit;
		this.nowTimestampAfterCommit = nowTimestampAfterCommit;
	}

	public long getNowTimestampBeforeCommit() {
		return this.nowTimestampBeforeCommit;
	}

	public long getNowTimestampAfterCommit() {
		return this.nowTimestampAfterCommit;
	}
}
