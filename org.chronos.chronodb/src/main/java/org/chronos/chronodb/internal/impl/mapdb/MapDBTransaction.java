package org.chronos.chronodb.internal.impl.mapdb;

import static com.google.common.base.Preconditions.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.locks.ReadWriteLock;

import org.chronos.common.exceptions.ChronosException;
import org.mapdb.Atomic.Boolean;
import org.mapdb.Atomic.Integer;
import org.mapdb.Atomic.Long;
import org.mapdb.Atomic.Var;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.DB;
import org.mapdb.DB.BTreeMapMaker;
import org.mapdb.DB.BTreeSetMaker;
import org.mapdb.DB.HTreeMapMaker;
import org.mapdb.DB.HTreeSetMaker;
import org.mapdb.Engine;
import org.mapdb.Fun.Function1;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.mapdb.StoreWAL;

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
	private boolean txClosed = false;

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
		this.assertNoUncommittedData();
		this.txClosed = true;
		// note: we do not perform a 'commit' here because we have no data to write (by definition).
		// note: we also do not perform a 'rollback' here because nothing has changed.
	}

	// =================================================================================================================
	// FORWARDING METHODS FROM [ DB ]
	// =================================================================================================================

	public <A> A catGet(final String name, final A init) {
		this.assertNotClosed();
		return this.tx.catGet(name, init);
	}

	public <A> A catGet(final String name) {
		this.assertNotClosed();
		return this.tx.catGet(name);
	}

	public <A> A catPut(final String name, final A value) {
		this.assertNotClosed();
		return this.tx.catPut(name, value);
	}

	public <A> A catPut(final String name, final A value, final A retValueIfNull) {
		this.assertNotClosed();
		return this.tx.catPut(name, value, retValueIfNull);
	}

	public Long atomicLongCreate(final String name, final long initValue) {
		this.assertNotClosed();
		return this.tx.atomicLongCreate(name, initValue);
	}

	public Long atomicLong(final String name) {
		this.assertNotClosed();
		return this.tx.atomicLong(name);
	}

	public Integer atomicIntegerCreate(final String name, final int initValue) {
		this.assertNotClosed();
		return this.tx.atomicIntegerCreate(name, initValue);
	}

	public Integer atomicInteger(final String name) {
		this.assertNotClosed();
		return this.tx.atomicInteger(name);
	}

	public Boolean atomicBooleanCreate(final String name, final boolean initValue) {
		this.assertNotClosed();
		return this.tx.atomicBooleanCreate(name, initValue);
	}

	public Boolean atomicBoolean(final String name) {
		this.assertNotClosed();
		return this.tx.atomicBoolean(name);
	}

	public void checkShouldCreate(final String name) {
		this.assertNotClosed();
		this.tx.checkShouldCreate(name);
	}

	public org.mapdb.Atomic.String atomicStringCreate(final String name, final String initValue) {
		this.assertNotClosed();
		return this.tx.atomicStringCreate(name, initValue);
	}

	public org.mapdb.Atomic.String atomicString(final String name) {
		this.assertNotClosed();
		return this.tx.atomicString(name);
	}

	public <E> Var<E> atomicVarCreate(final String name, final E initValue, final Serializer<E> serializer) {
		this.assertNotClosed();
		return this.tx.atomicVarCreate(name, initValue, serializer);
	}

	public <E> Var<E> atomicVar(final String name) {
		this.assertNotClosed();
		return this.tx.atomicVar(name);
	}

	public <E> Var<E> atomicVar(final String name, final Serializer<E> serializer) {
		this.assertNotClosed();
		return this.tx.atomicVar(name, serializer);
	}

	public <E> E get(final String name) {
		this.assertNotClosed();
		return this.tx.get(name);
	}

	public boolean exists(final String name) {
		this.assertNotClosed();
		return this.tx.exists(name);
	}

	public void delete(final String name) {
		this.assertNotClosed();
		this.tx.delete(name);
	}

	public Map<String, Object> getAll() {
		this.assertNotClosed();
		return this.tx.getAll();
	}

	public void checkNameNotExists(final String name) {
		this.assertNotClosed();
		this.tx.checkNameNotExists(name);
	}

	public void checkNotClosed() {
		this.assertNotClosed();
		this.tx.checkNotClosed();
	}

	public void commit() {
		this.tx.commit();
		this.txClosed = true;
	}

	public void compact() {
		this.assertNotClosed();
		this.tx.compact();
	}

	public void checkType(final String type, final String expected) {
		this.assertNotClosed();
		this.tx.checkType(type, expected);
	}

	public ReadWriteLock consistencyLock() {
		this.assertNotClosed();
		return this.tx.consistencyLock();
	}

	public String getNameForObject(final Object obj) {
		this.assertNotClosed();
		return this.tx.getNameForObject(obj);
	}

	public <K, V> HTreeMap<K, V> hashMap(final String name) {
		this.assertNotClosed();
		return this.tx.hashMap(name);
	}

	public <K, V> HTreeMap<K, V> hashMap(final String name, final Serializer<K> keySerializer,
			final Serializer<V> valueSerializer, final Function1<V, K> valueCreator) {
		this.assertNotClosed();
		return this.tx.hashMap(name, keySerializer, valueSerializer, valueCreator);
	}

	public HTreeMapMaker hashMapCreate(final String name) {
		this.assertNotClosed();
		return this.tx.hashMapCreate(name);
	}

	public <K> Set<K> hashSet(final String name) {
		this.assertNotClosed();
		return this.tx.hashSet(name);
	}

	public <K> Set<K> hashSet(final String name, final Serializer<K> serializer) {
		this.assertNotClosed();
		return this.tx.hashSet(name, serializer);
	}

	public SortedMap<String, Object> getCatalog() {
		this.assertNotClosed();
		return this.tx.getCatalog();
	}

	public Object getFromWeakCollection(final String name) {
		this.assertNotClosed();
		return this.tx.getFromWeakCollection(name);
	}

	@SuppressWarnings("rawtypes")
	public Serializer getDefaultSerializer() {
		this.assertNotClosed();
		return this.tx.getDefaultSerializer();
	}

	public Engine getEngine() {
		this.assertNotClosed();
		return this.tx.getEngine();
	}

	public void metricsLog() {
		this.assertNotClosed();
		this.tx.metricsLog();
	}

	public Map<String, java.lang.Long> metricsGet() {
		this.assertNotClosed();
		return this.tx.metricsGet();
	}

	public <V> V namedPut(final String name, final Object ret) {
		this.assertNotClosed();
		return this.tx.namedPut(name, ret);
	}

	public HTreeSetMaker hashSetCreate(final String name) {
		this.assertNotClosed();
		return this.tx.hashSetCreate(name);
	}

	public <K, V> NavigableMap<K, V> treeMap(final String name) {
		this.assertNotClosed();
		return this.tx.treeMap(name);
	}

	public <K, V> NavigableMap<K, V> treeMap(final String name, final Serializer<K> keySerializer,
			final Serializer<V> valueSerializer) {
		this.assertNotClosed();
		return this.tx.treeMap(name, keySerializer, valueSerializer);
	}

	@SuppressWarnings("rawtypes")
	public <K, V> NavigableMap<K, V> treeMap(final String name, final BTreeKeySerializer keySerializer,
			final Serializer<V> valueSerializer) {
		this.assertNotClosed();
		return this.tx.treeMap(name, keySerializer, valueSerializer);
	}

	public BTreeMapMaker treeMapCreate(final String name) {
		this.assertNotClosed();
		return this.tx.treeMapCreate(name);
	}

	public <K> NavigableSet<K> treeSet(final String name) {
		this.assertNotClosed();
		return this.tx.treeSet(name);
	}

	@SuppressWarnings("rawtypes")
	public <K> NavigableSet<K> treeSet(final String name, final Serializer serializer) {
		this.assertNotClosed();
		return this.tx.treeSet(name, serializer);
	}

	@SuppressWarnings("rawtypes")
	public <K> NavigableSet<K> treeSet(final String name, final BTreeKeySerializer serializer) {
		this.assertNotClosed();
		return this.tx.treeSet(name, serializer);
	}

	public BTreeSetMaker treeSetCreate(final String name) {
		this.assertNotClosed();
		return this.tx.treeSetCreate(name);
	}

	public <K> NavigableSet<K> treeSetCreate(final BTreeSetMaker m) {
		this.assertNotClosed();
		return this.tx.treeSetCreate(m);
	}

	public void rename(final String oldName, final String newName) {
		this.assertNotClosed();
		this.tx.rename(oldName, newName);
	}

	public boolean isClosed() {
		return this.tx.isClosed();
	}

	public void rollback() {
		this.tx.rollback();
		this.txClosed = true;
	}

	public DB snapshot() {
		this.assertNotClosed();
		return this.tx.snapshot();
	}

	@Override
	public String toString() {
		return this.tx.toString();
	}

	protected DB getMapDB() {
		return this.tx;
	}

	private void assertNoUncommittedData() {
		Engine engine = this.tx.getEngine();

		try {
			StoreWAL walEngine = (StoreWAL) engine;
			Method method = walEngine.getClass().getDeclaredMethod("hasUncommitedData");
			method.setAccessible(true);
			boolean uncommittedChanges = (boolean) method.invoke(walEngine);
			if (uncommittedChanges) {
				throw new ChronosException("UNCOMMITTED DATA!");
			}

		} catch (InvocationTargetException | NoSuchMethodException | SecurityException | IllegalAccessException
				| IllegalArgumentException e) {
			e.printStackTrace();
		}
	}

	private void assertNotClosed() {
		if (this.txClosed) {
			throw new IllegalStateException("Transaction is already closed!");
		}
	}

}
