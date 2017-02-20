package org.chronos.chronodb.test.util;

import java.util.function.Consumer;

import org.chronos.chronodb.api.ChronoDBTransaction;

public class KillSwitchCollection {

	private Consumer<ChronoDBTransaction> onBeforePrimaryIndexUpdate;
	private Consumer<ChronoDBTransaction> onBeforeSecondaryIndexUpdate;
	private Consumer<ChronoDBTransaction> onBeforeMetadataUpdate;
	private Consumer<ChronoDBTransaction> onBeforeCacheUpdate;
	private Consumer<ChronoDBTransaction> onBeforeNowTimestampUpdate;
	private Consumer<ChronoDBTransaction> onBeforeTransactionCommitted;

	public Consumer<ChronoDBTransaction> getOnBeforePrimaryIndexUpdate() {
		return this.onBeforePrimaryIndexUpdate;
	}

	public void setOnBeforePrimaryIndexUpdate(final Consumer<ChronoDBTransaction> onBeforePrimaryIndexUpdate) {
		this.onBeforePrimaryIndexUpdate = onBeforePrimaryIndexUpdate;
	}

	public Consumer<ChronoDBTransaction> getOnBeforeSecondaryIndexUpdate() {
		return this.onBeforeSecondaryIndexUpdate;
	}

	public void setOnBeforeSecondaryIndexUpdate(final Consumer<ChronoDBTransaction> onBeforeSecondaryIndexUpdate) {
		this.onBeforeSecondaryIndexUpdate = onBeforeSecondaryIndexUpdate;
	}

	public Consumer<ChronoDBTransaction> getOnBeforeMetadataUpdate() {
		return this.onBeforeMetadataUpdate;
	}

	public void setOnBeforeMetadataUpdate(final Consumer<ChronoDBTransaction> onBeforeMetadataUpdate) {
		this.onBeforeMetadataUpdate = onBeforeMetadataUpdate;
	}

	public Consumer<ChronoDBTransaction> getOnBeforeCacheUpdate() {
		return this.onBeforeCacheUpdate;
	}

	public void setOnBeforeCacheUpdate(final Consumer<ChronoDBTransaction> onBeforeCacheUpdate) {
		this.onBeforeCacheUpdate = onBeforeCacheUpdate;
	}

	public Consumer<ChronoDBTransaction> getOnBeforeNowTimestampUpdate() {
		return this.onBeforeNowTimestampUpdate;
	}

	public void setOnBeforeNowTimestampUpdate(final Consumer<ChronoDBTransaction> onBeforeNowTimestampUpdate) {
		this.onBeforeNowTimestampUpdate = onBeforeNowTimestampUpdate;
	}

	public Consumer<ChronoDBTransaction> getOnBeforeTransactionCommitted() {
		return this.onBeforeTransactionCommitted;
	}

	public void setOnBeforeTransactionCommitted(final Consumer<ChronoDBTransaction> onBeforeTransactionCommitted) {
		this.onBeforeTransactionCommitted = onBeforeTransactionCommitted;
	}

}