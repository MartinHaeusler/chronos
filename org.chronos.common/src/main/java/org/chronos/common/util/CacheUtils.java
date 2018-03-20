package org.chronos.common.util;

import static com.google.common.base.Preconditions.*;

import java.util.function.Function;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * This is a very simple utility class that eases the syntactical handling of the Guava {@link CacheBuilder}.
 *
 * <p>
 * Please use one of the <code>static</code> methods provided by this class. Do not instantiate it.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public class CacheUtils {

	/**
	 * Builds a new Least-Recently-Used cache with the given size and loading function.
	 *
	 * @param size
	 *            The size of the cache, i.e. the maximum number of elements that can be present. Must be strictly larger than zero. Loading an element that would exceed the given limit will cause the least-recently-used element to be evicted from the cache.
	 * @param loadingFunction
	 *            The function that loads the value for the given key. Must not be <code>null</code>.
	 * 
	 * @return The newly created cache. Never <code>null</code>.
	 */
	public static <K, V> LoadingCache<K, V> buildLRU(final int size, final Function<K, V> loadingFunction) {
		checkArgument(size > 0, "Precondition violation - argument 'size' must be greater than zero!");
		checkNotNull(loadingFunction, "Precondition violation - argument 'loadingFunction' must not be NULL!");
		return CacheBuilder.newBuilder().maximumSize(size).build(new CacheLoader<K, V>() {
			@Override
			public V load(final K key) throws Exception {
				V value = loadingFunction.apply(key);
				if(value == null){
					throw new Exception("No element with ID '" + key + "'!");
				}
				return value;
			}
		});
	}

	/**
	 * Builds a new cache with weakly referenced values.
	 *
	 * @param loadingFunction The function that loads the value for a given key in case of a cache miss. Must not be <code>null</code>.
	 * @param <K> The type of the keys
	 * @param <V> The type of the values
	 * @return The newly created cache instance. Never <code>null</code>.
	 */
	public static <K, V> LoadingCache<K, V> buildWeak(final Function<K, V> loadingFunction) {
		checkNotNull(loadingFunction, "Precondition violation - argument 'loadingFunction' must not be NULL!");
		return CacheBuilder.newBuilder().weakValues().build(new CacheLoader<K, V>() {
			@Override
			public V load(final K key) throws Exception {
				V value = loadingFunction.apply(key);
				if(value == null){
					throw new Exception("No element with ID '" + key + "'!");
				}
				return value;
			}
		});
	}

}
