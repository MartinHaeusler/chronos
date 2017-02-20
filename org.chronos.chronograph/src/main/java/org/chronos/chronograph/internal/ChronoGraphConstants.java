package org.chronos.chronograph.internal;

public final class ChronoGraphConstants {

	// =====================================================================================================================
	// STATIC FIELDS
	// =====================================================================================================================

	public static final String KEYSPACE_VERTEX = "vertex";
	public static final String KEYSPACE_EDGE = "edge";
	public static final String KEYSPACE_VARIABLES = "variables";
	public static final String KEYSPACE_MANAGEMENT_INDICES = "indices";

	public static final String INDEX_PREFIX_VERTEX = "v_";
	public static final String INDEX_PREFIX_EDGE = "e_";

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	private ChronoGraphConstants() {
		// do not instantiate
	}
}
