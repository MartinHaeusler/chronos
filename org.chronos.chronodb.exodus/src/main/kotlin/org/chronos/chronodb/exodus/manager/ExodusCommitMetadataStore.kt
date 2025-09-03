package org.chronos.chronodb.exodus.manager

import com.google.common.collect.Iterators
import jetbrains.exodus.ByteIterable
import org.apache.commons.lang3.tuple.Pair
import org.chronos.chronodb.api.Branch
import org.chronos.chronodb.api.Order
import org.chronos.chronodb.api.SerializationManager
import org.chronos.chronodb.exodus.kotlin.ext.*
import org.chronos.chronodb.exodus.layout.ChronoDBStoreLayout
import org.chronos.chronodb.exodus.manager.chunk.BranchChunkManager
import org.chronos.chronodb.exodus.manager.chunk.ChronoChunk
import org.chronos.chronodb.exodus.manager.chunk.GlobalChunkManager
import org.chronos.chronodb.internal.api.CommitMetadataStore
import org.chronos.chronodb.internal.api.Period
import org.chronos.chronodb.internal.impl.engines.base.ChronosInternalCommitMetadata
import org.chronos.chronodb.internal.util.NavigableMapUtils
import java.util.*
import kotlin.math.max

class ExodusCommitMetadataStore : CommitMetadataStore {

    private val globalChunkManager: GlobalChunkManager
    private val branchChunkManager: BranchChunkManager
    private val serializationManager: SerializationManager
    private val branch: Branch

    constructor(globalChunkManager: GlobalChunkManager, serializationManager: SerializationManager, branch: Branch) {
        this.globalChunkManager = globalChunkManager
        this.serializationManager = serializationManager
        this.branch = branch
        this.branchChunkManager = globalChunkManager.getOrCreateChunkManagerForBranch(branch)
    }


    override fun getCommitTimestampsBetween(from: Long, to: Long, order: Order, includeSystemInternalCommits: Boolean): Iterator<Long> {
        requireNonNegative(from, "from")
        requireNonNegative(to, "to")
        if (from > to) {
            // as per specification, we return an empty iterator
            // if the requested time period is invalid.
            return Collections.emptyIterator()
        }
        val chunks = getChunks(from, to, order)
        // note: all of the following operations are lazy, so we only open the chunks that
        // are actually necessary (which is why we return an iterator here)
        return chunks.asSequence()
            .flatMap { getCommitTimestampsBetweenInChunk(it, from, to, order, includeSystemInternalCommits).asSequence() }
            .iterator()
    }

    override fun getCommitMetadataBetween(from: Long, to: Long, order: Order, includeSystemInternalCommits: Boolean): Iterator<Map.Entry<Long, Any>> {
        requireNonNegative(from, "from")
        requireNonNegative(to, "to")
        if (from > to) {
            // as per specification, an invalid interval produces an empty iterator
            return Collections.emptyIterator()
        }
        val chunks = getChunks(from, to, order)
        return chunks.asSequence()
            .flatMap { getCommitMetadataBetweenInChunk(it, from, to, order, includeSystemInternalCommits).asSequence() }
            .iterator()
    }


    override fun getCommitTimestampsPaged(minTimestamp: Long, maxTimestamp: Long, pageSize: Int, pageIndex: Int, order: Order, includeSystemInternalCommits: Boolean): Iterator<Long> {
        requireNonNegative(minTimestamp, "minTimestamp")
        requireNonNegative(maxTimestamp, "maxTimestamp")
        if (minTimestamp > maxTimestamp) {
            // as per specification, an invalid interval produces an empty iterator.
            return Collections.emptyIterator()
        }
        requireNonNegative(pageSize, "pageSize")
        requireNonNegative(pageIndex, "pageIndex")
        return this.getCommitTimestampsBetween(minTimestamp, maxTimestamp, order, includeSystemInternalCommits).asSequence()
            // skip the first n pages (each has m entries)
            .drop(pageSize * pageIndex)
            // from the remaining elements, take only one page (of m entries)
            .take(pageSize)
            .iterator()
    }

    override fun getCommitMetadataPaged(minTimestamp: Long, maxTimestamp: Long, pageSize: Int, pageIndex: Int, order: Order, includeSystemInternalCommits: Boolean): Iterator<Map.Entry<Long, Any>> {
        requireNonNegative(minTimestamp, "minTimestamp")
        requireNonNegative(maxTimestamp, "maxTimestamp")
        if (minTimestamp > maxTimestamp) {
            // according to the specification, an invalid time range produces an empty iterator.
            return Collections.emptyIterator()
        }
        requireNonNegative(pageSize, "pageSize")
        requireNonNegative(pageIndex, "pageIndex")
        return this.getCommitMetadataBetween(minTimestamp, maxTimestamp, order, includeSystemInternalCommits).asSequence()
            // skip the first n pages (each has m entries)
            .drop(pageSize * pageIndex)
            // from the remaining elements, take only one page (of m entries)
            .take(pageSize)
            .iterator()
    }

    override fun getCommitMetadataAround(timestamp: Long, count: Int, includeSystemInternalCommits: Boolean): List<Map.Entry<Long, Any>> {
        // get the lower entries (potentially twice as many as we need)
        val lowerCommits = this.getCommitMetadataPaged(0, timestamp, count, 0, Order.DESCENDING, includeSystemInternalCommits).asSequence().toList()
        val higherCommits = this.getCommitMetadataPaged(timestamp + 1, Long.MAX_VALUE, count, 0, Order.DESCENDING, includeSystemInternalCommits).asSequence().toList()
        val map = TreeMap<Long, Any>()
        lowerCommits.forEach { map[it.key] = it.value }
        higherCommits.forEach { map[it.key] = it.value }
        return NavigableMapUtils.entriesAround(map, timestamp, count).sortedByDescending { it.key }
    }

    override fun getCommitMetadataBefore(timestamp: Long, count: Int, includeSystemInternalCommits: Boolean): List<Map.Entry<Long, Any>> {
        requireNonNegative(timestamp, "timestamp")
        requireNonNegative(count, "count")
        // getCommitMetadataBetween has inclusive bounds, but this method uses an exclusive bound
        val upperBound = max(timestamp - 1, 0)
        return this.getCommitMetadataBetween(0, upperBound, Order.DESCENDING, includeSystemInternalCommits).asSequence()
            .take(count)
            .toList()
    }

    override fun getCommitMetadataAfter(timestamp: Long, count: Int, includeSystemInternalCommits: Boolean): List<Map.Entry<Long, Any>> {
        requireNonNegative(timestamp, "timestamp")
        requireNonNegative(count, "count")
        // getCommitMetadataBetween has inclusive bounds, but this method uses an exclusive bound
        val lowerBound = max(timestamp + 1, 0)
        return this.getCommitMetadataBetween(lowerBound, Long.MAX_VALUE, Order.ASCENDING, includeSystemInternalCommits).asSequence()
            .take(count)
            .toList().reversed()
    }

    override fun getCommitTimestampsAround(timestamp: Long, count: Int, includeSystemInternalCommits: Boolean): List<Long> {
        requireNonNegative(timestamp, "timestamp")
        requireNonNegative(count, "count")
        return this.getCommitMetadataAround(timestamp, count, includeSystemInternalCommits).asSequence().map { it.key }.toList()
    }

    override fun getCommitTimestampsBefore(timestamp: Long, count: Int, includeSystemInternalCommits: Boolean): List<Long> {
        requireNonNegative(timestamp, "timestamp")
        requireNonNegative(count, "count")
        // getCommitTimestampsBetween has inclusive bounds. This method has an exclusive upper bound.
        val upperLimit = max(timestamp - 1, 0)
        return this.getCommitTimestampsBetween(0, upperLimit, Order.DESCENDING, includeSystemInternalCommits).asSequence()
            .take(count)
            .toList()
    }

    override fun getCommitTimestampsAfter(timestamp: Long, count: Int, includeSystemInternalCommits: Boolean): List<Long> {
        requireNonNegative(timestamp, "timestamp")
        requireNonNegative(count, "count")
        // getCommitTimestampsBetween has inclusive bounds. This method has an exclusive lower bound.
        val lowerLimit = max(timestamp + 1, 0)
        return this.getCommitTimestampsBetween(lowerLimit, Long.MAX_VALUE, Order.ASCENDING, includeSystemInternalCommits).asSequence()
            .take(count)
            .toList().reversed()
    }

    override fun countCommitTimestampsBetween(from: Long, to: Long, includeSystemInternalCommits: Boolean): Int {
        requireNonNegative(from, "from")
        requireNonNegative(to, "to")
        if (from > to) {
            // as per the specification, if the range is invalid,
            // we return zero.
            return 0
        }
        return Iterators.size(this.getCommitTimestampsBetween(from, to, Order.DESCENDING, includeSystemInternalCommits))
    }

    override fun countCommitTimestamps(includeSystemInternalCommits: Boolean): Int {
        // TODO: it's a bit unfortunate that we have to open ALL chunks just for this...
        val chunks = this.getChunks(0, Long.MAX_VALUE, Order.DESCENDING)
        return chunks.asSequence().map { chunk -> this.countCommitsInChunk(chunk, includeSystemInternalCommits) }.sum()
    }

    override fun put(commitTimestamp: Long, commitMetadata: Any?) {
        requireNonNegative(commitTimestamp, "commitTimestamp")
        val chunk = this.branchChunkManager.getChunkForTimestamp(commitTimestamp)
            ?: throw IllegalStateException("Cannot insert commit at timestamp ${commitTimestamp} - the branch ${this.branch.name} has no chunk for this timestamp!")

        val byteValue = this.serializationManager.serialize(commitMetadata).toByteIterable()
        this.globalChunkManager.openReadWriteTransactionOn(chunk).use { tx ->
            tx.put(ChronoDBStoreLayout.STORE_NAME__COMMIT_METADATA, commitTimestamp.toByteIterable(), byteValue)
            tx.commit()
        }
    }

    override fun get(commitTimestamp: Long): Any? {
        requireNonNegative(commitTimestamp, "commitTimestamp")
        val chunk = this.branchChunkManager.getChunkForTimestamp(commitTimestamp)
        if (chunk == null) {
            return null
        }
        val metadataBytes = this.globalChunkManager.openReadOnlyTransactionOn(chunk).use { tx ->
            return@use tx.get(ChronoDBStoreLayout.STORE_NAME__COMMIT_METADATA, commitTimestamp.toByteIterable())
        }
        if (metadataBytes == null) {
            return null
        }
        val byteArray = metadataBytes.toByteArray()
        return this.serializationManager.deserialize(byteArray)
    }

    override fun rollbackToTimestamp(timestamp: Long) {
        requireNonNegative(timestamp, "timestamp")
        val chunks = this.getChunks(timestamp, Long.MAX_VALUE, Order.DESCENDING)
        if (chunks.isEmpty()) {
            throw IllegalStateException("There is no chunk to perform the rollback on branch ${this.branch.name} to timestamp ${timestamp}!")
        }
        if (chunks.size > 1) {
            throw IllegalStateException("Cannot perform rollback on branch ${this.branch.name} - the target timestamp ${timestamp} is not part of the HEAD chunk!")
        }
        chunks.forEach { chunk ->
            this.globalChunkManager.openReadWriteTransactionOn(chunk).use { tx ->
                val keysToDelete = mutableListOf<ByteIterable>()
                tx.openCursorOn(ChronoDBStoreLayout.STORE_NAME__COMMIT_METADATA).use { cursor ->
                    if (cursor.last) {
                        while (cursor.key.parseAsLong() > timestamp) {
                            keysToDelete.add(cursor.key)
                            if (!cursor.prev) {
                                break
                            }
                        }
                    }
                }
                keysToDelete.forEach { tx.delete(ChronoDBStoreLayout.STORE_NAME__COMMIT_METADATA, it) }
                tx.commit()
            }
        }
    }

    override fun getOwningBranch(): Branch {
        return this.branch
    }

    override fun purge(commitTimestamp: Long): Boolean {
        requireNonNegative(commitTimestamp, "commitTimestamp")
        val chunk = this.branchChunkManager.getChunkForTimestamp(commitTimestamp)
            ?: return false // we have no chunk here, so we also can't have a commit to purge.
        return this.globalChunkManager.openReadWriteTransactionOn(chunk).use { tx ->
            val deleted = tx.delete(ChronoDBStoreLayout.STORE_NAME__COMMIT_METADATA, commitTimestamp.toByteIterable())
            tx.commit()
            return@use deleted
        }
    }

    // =================================================================================================================
    // HELPER METHODS
    // =================================================================================================================

    private fun getChunks(from: Long, to: Long, order: Order): List<ChronoChunk> {
        val list = this.branchChunkManager.getChunksForPeriod(inclusiveRange(from, to))
        if (order == Order.DESCENDING) {
            return list.asReversed()
        } else {
            return list
        }
    }

    private fun inclusiveRange(from: Long, to: Long): Period {
        val upperBound = if (to < Long.MAX_VALUE) {
            to + 1
        } else {
            Long.MAX_VALUE
        }
        return Period.createRange(from, upperBound)
    }

    private fun consumeCommitsAscending(chunk: ChronoChunk, from: Long, to: Long, consumer: (Long, ByteIterable) -> Unit) {
        this.globalChunkManager.openReadOnlyTransactionOn(chunk).use { tx ->
            tx.openCursorOn(ChronoDBStoreLayout.STORE_NAME__COMMIT_METADATA).use { cursor ->
                cursor.ceilKey(from.toByteIterable())
                    ?: return@use // there is no key equal/greater than "from" -> nothing to do

                // we need to check the "to" bound here, because we may "overshoot"
                // the "to" bound with the "ceilKey" operation.
                val startTimestamp = cursor.key.parseAsLong()
                if (startTimestamp > to) {
                    return@use // the first key which is greater/equal "from" is greater than "to" -> nothing to do
                }

                // report the current key to the consumer
                consumer(startTimestamp, cursor.value)

                // iterate through the cursor until we either run out of keys or
                // we find a key which is greater than "to".
                while (cursor.next) {
                    val key = cursor.key.parseAsLong()
                    if (key > to) {
                        break
                    } else {
                        consumer(key, cursor.value)
                    }
                }
            }
        }
    }

    private fun getCommitTimestampsBetweenInChunk(chunk: ChronoChunk, from: Long, to: Long, order: Order, includeSystemInternalCommits: Boolean): Iterator<Long> {
        val timestamps = mutableListOf<Long>()
        this.consumeCommitsAscending(chunk, from, to) { key, value ->
            if (includeSystemInternalCommits || this.isUserCommitMetadata(value)) {
                timestamps.add(key)
            }
        }
        if (order == Order.DESCENDING) {
            timestamps.reverse()
        }
        return timestamps.iterator()
    }

    private fun getCommitMetadataBetweenInChunk(chunk: ChronoChunk, from: Long, to: Long, order: Order, includeSystemInternalCommits: Boolean): Iterator<Map.Entry<Long, Any>> {
        val commits = mutableListOf<Map.Entry<Long, Any>>()
        this.consumeCommitsAscending(chunk, from, to) { key, value ->
            val metadata = this.serializationManager.deserialize(value.toByteArray())
            if (includeSystemInternalCommits || metadata !is ChronosInternalCommitMetadata) {
                commits.add(Pair.of(key, metadata))
            }
        }
        if (order == Order.DESCENDING) {
            commits.reverse()
        }
        return commits.iterator()
    }

    private fun countCommitsInChunk(chunk: ChronoChunk, includeSystemInternalCommits: Boolean): Int {
        return this.globalChunkManager.openReadOnlyTransactionOn(chunk).use { tx ->
            if (includeSystemInternalCommits) {
                tx.storeSize(ChronoDBStoreLayout.STORE_NAME__COMMIT_METADATA).toInt()
            } else {
                tx.openCursorOn(ChronoDBStoreLayout.STORE_NAME__COMMIT_METADATA).use { cursor ->
                    var count = 0
                    while (cursor.next) {
                        if (this.isUserCommitMetadata(cursor.value)) {
                            count++
                        }
                    }
                    count
                }
            }
        }
    }

    private fun isUserCommitMetadata(value: ByteIterable): Boolean {
        val byteArray = value.toByteArray()
        try {
            val metadata = this.serializationManager.deserialize(byteArray)
            return metadata !is ChronosInternalCommitMetadata
        } catch (_: Throwable) {
            // could not deserialize. Whatever it is, it is NOT an internal metadata object.
            return false
        }
    }

}
