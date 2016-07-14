// package org.chronos.chronodb.internal.impl.index;
//
// import static com.google.common.base.Preconditions.*;
//
// import java.util.Collections;
// import java.util.Iterator;
// import java.util.List;
// import java.util.Map;
// import java.util.Map.Entry;
// import java.util.Set;
// import java.util.SortedMap;
//
// import org.apache.commons.lang3.tuple.Pair;
// import org.chronos.chronodb.api.key.ChronoIdentifier;
//
// import com.google.common.collect.HashMultimap;
// import com.google.common.collect.Iterators;
// import com.google.common.collect.Lists;
// import com.google.common.collect.Maps;
// import com.google.common.collect.SetMultimap;
//
// public class IndexerWorkload {
//
// // =================================================================================================================
// // STATIC FACTORY
// // =================================================================================================================
//
// public static IndexerWorkload build(final Map<ChronoIdentifier, Object> identifierToValue) {
// checkNotNull(identifierToValue, "Precondition violation - argument 'identifierToValue' must not be NULL!");
// IndexerWorkload workload = new IndexerWorkload();
// workload.initialize(identifierToValue);
// return workload;
// }
//
// // =================================================================================================================
// // FIELDS
// // =================================================================================================================
//
// /**
// * This mapping represents the contents of this workload, in a structured fashion.
// *
// * <p>
// * The individual parameters of the mapping have the following semantics:
// *
// * <pre>
// * Timestamp -> Branch Name -> Keyspace Name -> (Identifier, value object)
// * </pre>
// *
// * <p>
// * The first parameter is a {@link SortedMap}, because timestamps have to be treated in ascending order (index
// * building is incremental).
// *
// */
// private final SortedMap<Long, Map<String, SetMultimap<String, Pair<ChronoIdentifier, Object>>>> contents;
//
// // =================================================================================================================
// // CONSTRUCTOR
// // =================================================================================================================
//
// private IndexerWorkload() {
// this.contents = Maps.newTreeMap();
// }
//
// private void initialize(final Map<ChronoIdentifier, Object> identifierToValue) {
// checkNotNull(identifierToValue, "Precondition violation - argument 'identifierToValue' must not be NULL!");
// for (Entry<ChronoIdentifier, Object> entry : identifierToValue.entrySet()) {
// ChronoIdentifier identifier = entry.getKey();
// Object value = entry.getValue();
// long timestamp = identifier.getTimestamp();
// String branch = identifier.getBranchName();
// String keyspace = identifier.getKeyspace();
// // try to get the contents for our timestamp at hand
// Map<String, SetMultimap<String, Pair<ChronoIdentifier, Object>>> branchToKeyspaceToEntries = this.contents
// .get(timestamp);
// // if this map doesn't exist yet, add it
// if (branchToKeyspaceToEntries == null) {
// branchToKeyspaceToEntries = Maps.newHashMap();
// this.contents.put(timestamp, branchToKeyspaceToEntries);
// }
// // then, try to get the inner map for the branch
// SetMultimap<String, Pair<ChronoIdentifier, Object>> keyspaceToEntries = branchToKeyspaceToEntries
// .get(branch);
// // if this map doesn't exist yet, add it
// if (keyspaceToEntries == null) {
// keyspaceToEntries = HashMultimap.create();
// branchToKeyspaceToEntries.put(branch, keyspaceToEntries);
// }
// // finally, add the identifier-value pair to the map, keyed against the keyspace name
// keyspaceToEntries.put(keyspace, Pair.of(identifier, value));
// }
// }
//
// public Set<Long> getTimestampsAscending() {
// return Collections.unmodifiableSet(this.contents.keySet());
// }
//
// public Set<String> getAffectedBranchesAtTimestamp(final long timestamp) {
// checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
// Map<String, ?> branchNameToContents = this.contents.get(timestamp);
// if (branchNameToContents.isEmpty()) {
// return Collections.emptySet();
// } else {
// return Collections.unmodifiableSet(branchNameToContents.keySet());
// }
// }
//
// public Set<String> getKeyspacesAtTimestampInBranch(final long timestamp, final String branch) {
// checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
// Map<String, SetMultimap<String, Pair<ChronoIdentifier, Object>>> branchMap = this.contents.get(timestamp);
// if (branchMap == null) {
// return Collections.emptySet();
// }
// SetMultimap<String, Pair<ChronoIdentifier, Object>> multimap = branchMap.get(branch);
// if (multimap == null) {
// return Collections.emptySet();
// } else {
// return Collections.unmodifiableSet(multimap.keySet());
// }
// }
//
// public Set<Pair<ChronoIdentifier, Object>> getEntriesToIndex(final long timestamp, final String branch,
// final String keyspace) {
// checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
// checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
// Map<String, SetMultimap<String, Pair<ChronoIdentifier, Object>>> branchMap = this.contents.get(timestamp);
// if (branchMap == null) {
// return Collections.emptySet();
// }
// SetMultimap<String, Pair<ChronoIdentifier, Object>> keyspaceMap = branchMap.get(branch);
// if (keyspaceMap == null) {
// return Collections.emptySet();
// }
// return Collections.unmodifiableSet(keyspaceMap.get(keyspace));
// }
//
// public Iterator<Pair<ChronoIdentifier, Object>> iterator() {
// List<Iterator<Pair<ChronoIdentifier, Object>>> iterators = Lists.newArrayList();
// // iterate over the timestamps in the workload, in ascending order
// for (long timestamp : this.getTimestampsAscending()) {
// // for each timestamp, iterate over the affected branches
// for (String branchName : this.getAffectedBranchesAtTimestamp(timestamp)) {
// // for each timestamp, iterate over the affected keyspaces
// for (String keyspace : this.getKeyspacesAtTimestampInBranch(timestamp, branchName)) {
// Set<Pair<ChronoIdentifier, Object>> entriesToIndex = this.getEntriesToIndex(timestamp, branchName,
// keyspace);
// iterators.add(entriesToIndex.iterator());
// }
// }
// }
// Iterator<Pair<ChronoIdentifier, Object>> concatenatedIterator = Iterators.concat(iterators.iterator());
// return concatenatedIterator;
// }
//
// @Override
// public String toString() {
// StringBuilder builder = new StringBuilder();
// builder.append("IndexerWorkload[");
// String separator = "";
// for (long timestamp : this.getTimestampsAscending()) {
// for (String branch : this.getAffectedBranchesAtTimestamp(timestamp)) {
// for (String keyspace : this.getKeyspacesAtTimestampInBranch(timestamp, branch)) {
// Set<Pair<ChronoIdentifier, Object>> entriesToIndex = this.getEntriesToIndex(timestamp, branch,
// keyspace);
// for (Pair<ChronoIdentifier, Object> entry : entriesToIndex) {
// ChronoIdentifier identifier = entry.getKey();
// Object value = entry.getValue();
// builder.append(separator);
// separator = ", ";
// builder.append(identifier.getTimestamp());
// builder.append("->");
// builder.append(identifier.getBranchName());
// builder.append("->");
// builder.append(identifier.getKeyspace());
// builder.append("->");
// builder.append(identifier.getKey());
// builder.append(":");
// builder.append(value);
// }
// }
// }
// }
// builder.append("]");
// return builder.toString();
//
// }
//
// }
