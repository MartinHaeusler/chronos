package org.chronos.chronodb.internal.impl.engines.mapdb;

import static com.google.common.base.Preconditions.*;

import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.locks.ReadWriteLock;

import org.mapdb.Atomic.Boolean;
import org.mapdb.Atomic.Integer;
import org.mapdb.Atomic.Long;
import org.mapdb.Atomic.Var;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DB.BTreeMapMaker;
import org.mapdb.DB.BTreeSetMaker;
import org.mapdb.DB.HTreeMapMaker;
import org.mapdb.DB.HTreeSetMaker;
import org.mapdb.Engine;
import org.mapdb.Fun.Function1;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

/**
 * A very thin wrapper around {@link DB} that implements {@link AutoCloseable} for convenience.
 *
 * <p>
 * All other methods are just forwarded to the wrapped instance.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public class MapDBTransaction implements AutoCloseable {

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	private final DB tx;

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	public MapDBTransaction(final DB mapDBTx) {
		checkNotNull(mapDBTx, "Precondition violation - argument 'mapDBTx' must not be NULL!");
		this.tx = mapDBTx;
	}

	// =================================================================================================================
	// IMPLEMENTATION OF [ AUTO CLOSABLE ]
	// =================================================================================================================

	@Override
	public void close() {
		// for now, do nothing here. The reason is that we only use a single "transaction" on the MapDB backend.
		// Closing this transaction would also close the database.
	}

	// =================================================================================================================
	// FORWARDING METHODS FROM [ DB ]
	// =================================================================================================================

	public <A> A catGet(final String name, final A init) {
		return this.tx.catGet(name, init);
	}

	public <A> A catGet(final String name) {
		return this.tx.catGet(name);
	}

	public <A> A catPut(final String name, final A value) {
		return this.tx.catPut(name, value);
	}

	public <A> A catPut(final String name, final A value, final A retValueIfNull) {
		return this.tx.catPut(name, value, retValueIfNull);
	}

	public Long atomicLongCreate(final String name, final long initValue) {
		return this.tx.atomicLongCreate(name, initValue);
	}

	public Long atomicLong(final String name) {
		return this.tx.atomicLong(name);
	}

	public Integer atomicIntegerCreate(final String name, final int initValue) {
		return this.tx.atomicIntegerCreate(name, initValue);
	}

	public Integer atomicInteger(final String name) {
		return this.tx.atomicInteger(name);
	}

	public Boolean atomicBooleanCreate(final String name, final boolean initValue) {
		return this.tx.atomicBooleanCreate(name, initValue);
	}

	public Boolean atomicBoolean(final String name) {
		return this.tx.atomicBoolean(name);
	}

	public void checkShouldCreate(final String name) {
		this.tx.checkShouldCreate(name);
	}

	public org.mapdb.Atomic.String atomicStringCreate(final String name, final String initValue) {
		return this.tx.atomicStringCreate(name, initValue);
	}

	public org.mapdb.Atomic.String atomicString(final String name) {
		return this.tx.atomicString(name);
	}

	public <E> Var<E> atomicVarCreate(final String name, final E initValue, final Serializer<E> serializer) {
		return this.tx.atomicVarCreate(name, initValue, serializer);
	}

	public <E> Var<E> atomicVar(final String name) {
		return this.tx.atomicVar(name);
	}

	public <E> Var<E> atomicVar(final String name, final Serializer<E> serializer) {
		return this.tx.atomicVar(name, serializer);
	}

	public <E> E get(final String name) {
		return this.tx.get(name);
	}

	public boolean exists(final String name) {
		return this.tx.exists(name);
	}

	public void delete(final String name) {
		this.tx.delete(name);
	}

	public Map<String, Object> getAll() {
		return this.tx.getAll();
	}

	public void checkNameNotExists(final String name) {
		this.tx.checkNameNotExists(name);
	}

	public void checkNotClosed() {
		this.tx.checkNotClosed();
	}

	public void commit() {
		this.tx.commit();
	}

	public void compact() {
		this.tx.compact();
	}

	public void checkType(final String type, final String expected) {
		this.tx.checkType(type, expected);
	}

	public ReadWriteLock consistencyLock() {
		return this.tx.consistencyLock();
	}

	public String getNameForObject(final Object obj) {
		return this.tx.getNameForObject(obj);
	}

	public <K, V> HTreeMap<K, V> hashMap(final String name) {
		return this.tx.hashMap(name);
	}

	public <K, V> HTreeMap<K, V> hashMap(final String name, final Serializer<K> keySerializer,
			final Serializer<V> valueSerializer, final Function1<V, K> valueCreator) {
		return this.tx.hashMap(name, keySerializer, valueSerializer, valueCreator);
	}

	public HTreeMapMaker hashMapCreate(final String name) {
		return this.tx.hashMapCreate(name);
	}

	public <K> Set<K> hashSet(final String name) {
		return this.tx.hashSet(name);
	}

	public <K> Set<K> hashSet(final String name, final Serializer<K> serializer) {
		return this.tx.hashSet(name, serializer);
	}

	public SortedMap<String, Object> getCatalog() {
		return this.tx.getCatalog();
	}

	public Object getFromWeakCollection(final String name) {
		return this.tx.getFromWeakCollection(name);
	}

	@SuppressWarnings("rawtypes")
	public Serializer getDefaultSerializer() {
		return this.tx.getDefaultSerializer();
	}

	public Engine getEngine() {
		return this.tx.getEngine();
	}

	public void metricsLog() {
		this.tx.metricsLog();
	}

	public Map<String, java.lang.Long> metricsGet() {
		return this.tx.metricsGet();
	}

	public <V> V namedPut(final String name, final Object ret) {
		return this.tx.namedPut(name, ret);
	}

	public HTreeSetMaker hashSetCreate(final String name) {
		return this.tx.hashSetCreate(name);
	}

	public <K, V> BTreeMap<K, V> treeMap(final String name) {
		return this.tx.treeMap(name);
	}

	public <K, V> BTreeMap<K, V> treeMap(final String name, final Serializer<K> keySerializer,
			final Serializer<V> valueSerializer) {
		return this.tx.treeMap(name, keySerializer, valueSerializer);
	}

	@SuppressWarnings("rawtypes")
	public <K, V> BTreeMap<K, V> treeMap(final String name, final BTreeKeySerializer keySerializer,
			final Serializer<V> valueSerializer) {
		return this.tx.treeMap(name, keySerializer, valueSerializer);
	}

	public BTreeMapMaker treeMapCreate(final String name) {
		return this.tx.treeMapCreate(name);
	}

	public <K> NavigableSet<K> treeSet(final String name) {
		return this.tx.treeSet(name);
	}

	@SuppressWarnings("rawtypes")
	public <K> NavigableSet<K> treeSet(final String name, final Serializer serializer) {
		return this.tx.treeSet(name, serializer);
	}

	@SuppressWarnings("rawtypes")
	public <K> NavigableSet<K> treeSet(final String name, final BTreeKeySerializer serializer) {
		return this.tx.treeSet(name, serializer);
	}

	public BTreeSetMaker treeSetCreate(final String name) {
		return this.tx.treeSetCreate(name);
	}

	public <K> NavigableSet<K> treeSetCreate(final BTreeSetMaker m) {
		return this.tx.treeSetCreate(m);
	}

	public void rename(final String oldName, final String newName) {
		this.tx.rename(oldName, newName);
	}

	public boolean isClosed() {
		return this.tx.isClosed();
	}

	public void rollback() {
		this.tx.rollback();
	}

	public DB snapshot() {
		return this.tx.snapshot();
	}

	@Override
	public String toString() {
		return this.tx.toString();
	}

}
