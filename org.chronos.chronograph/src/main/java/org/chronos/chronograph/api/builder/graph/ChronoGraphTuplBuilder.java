package org.chronos.chronograph.api.builder.graph;

import org.chronos.chronodb.internal.api.ChronoDBConfiguration;

public interface ChronoGraphTuplBuilder extends ChronoGraphFinalizableBuilder<ChronoGraphTuplBuilder> {

	/**
	 * Explicitly sets the size of the cache for the backend TUPL store, in bytes.
	 *
	 * <p>
	 * The default value is 209715200 bytes (= 200MB).
	 *
	 * <p>
	 * This setting corresponds to {@link ChronoDBConfiguration#STORAGE_BACKEND_CACHE}.
	 *
	 * @param backendCacheSize
	 *            The desired maximum size of the backend cache, in bytes. Default is 200MB. Must be strictly greater
	 *            than zero.
	 * @return <code>this</code>, for method chaining.
	 */
	public ChronoGraphTuplBuilder withBackendCacheSize(long backendCacheSize);

}
