package org.chronos.chronodb.api;

public interface BranchHeadStatistics {

	public long getTotalNumberOfEntries();

	public long getNumberOfEntriesInHead();

	public long getNumberOfEntriesInHistory();

	public double getHeadHistoryRatio();

}
