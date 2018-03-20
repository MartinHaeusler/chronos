package org.chronos.chronodb.internal.impl.builder.transaction;

import static com.google.common.base.Preconditions.*;

import java.util.Date;

import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.DuplicateVersionEliminationMode;
import org.chronos.chronodb.api.builder.transaction.ChronoDBTransactionBuilder;
import org.chronos.chronodb.api.conflict.ConflictResolutionStrategy;
import org.chronos.chronodb.internal.api.ChronoDBInternal;
import org.chronos.chronodb.internal.api.MutableTransactionConfiguration;
import org.chronos.chronodb.internal.impl.DefaultTransactionConfiguration;

public class DefaultTransactionBuilder implements ChronoDBTransactionBuilder {

	private final ChronoDBInternal owningDB;
	private final MutableTransactionConfiguration configuration;

	public DefaultTransactionBuilder(final ChronoDBInternal owningDB) {
		checkNotNull(owningDB, "Precondition violation - argument 'owningDB' must not be NULL!");
		this.owningDB = owningDB;
		this.configuration = new DefaultTransactionConfiguration();
		// by default, we inherit some settings from the owning database
		this.configuration
				.setConflictResolutionStrategy(this.owningDB.getConfiguration().getConflictResolutionStrategy());
		this.configuration.setDuplicateVersionEliminationMode(
				this.owningDB.getConfiguration().getDuplicateVersionEliminationMode());
	}

	@Override
	public ChronoDBTransaction build() {
		this.configuration.freeze();
		return this.owningDB.tx(this.configuration);
	}

	@Override
	public ChronoDBTransactionBuilder readOnly() {
		this.configuration.setReadOnly(true);
		return this;
	}

	@Override
	public ChronoDBTransactionBuilder threadSafe() {
		this.configuration.setThreadSafe(true);
		return this;
	}

	@Override
	public ChronoDBTransactionBuilder atDate(final Date date) {
		checkNotNull(date, "Precondition violation - argument 'date' must not be NULL!");
		this.configuration.setTimestamp(date.getTime());
		return this;
	}

	@Override
	public ChronoDBTransactionBuilder atTimestamp(final long timestamp) {
		checkArgument(timestamp >= 0, "Precondition Violation - argument 'timestamp' must not be negative!");
		this.configuration.setTimestamp(timestamp);
		return this;
	}

	@Override
	public ChronoDBTransactionBuilder onBranch(final String branchName) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		this.configuration.setBranch(branchName);
		return this;
	}

	@Override
	public ChronoDBTransactionBuilder withConflictResolutionStrategy(final ConflictResolutionStrategy strategy) {
		checkNotNull(strategy, "Precondition violation - argument 'strategy' must not be NULL!");
		this.configuration.setConflictResolutionStrategy(strategy);
		return this;
	}

	@Override
	public ChronoDBTransactionBuilder withDuplicateVersionEliminationMode(final DuplicateVersionEliminationMode mode) {
		checkNotNull(mode, "Precondition violation - argument 'mode' must not be NULL!");
		this.configuration.setDuplicateVersionEliminationMode(mode);
		return this;
	}

}
