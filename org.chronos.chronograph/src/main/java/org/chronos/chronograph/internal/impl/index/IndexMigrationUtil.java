package org.chronos.chronograph.internal.impl.index;

import org.chronos.chronograph.api.index.ChronoGraphIndex;
import org.chronos.chronograph.internal.api.index.IChronoGraphEdgeIndex;
import org.chronos.chronograph.internal.api.index.IChronoGraphVertexIndex;

public class IndexMigrationUtil {

	@SuppressWarnings("deprecation")
	public static IChronoGraphVertexIndex migrate(final IChronoGraphVertexIndex index) {
		if (index instanceof ChronoGraphVertexIndex) {
			return new ChronoGraphVertexIndex2(index.getIndexedProperty(), index.getIndexType());
		}
		// no need to migrate
		return index;
	}

	@SuppressWarnings("deprecation")
	public static IChronoGraphEdgeIndex migrate(final IChronoGraphEdgeIndex index) {
		if (index instanceof ChronoGraphEdgeIndex) {
			return new ChronoGraphEdgeIndex2(index.getIndexedProperty(), index.getIndexType());
		}
		// no need to migrate
		return index;
	}

	public static ChronoGraphIndex migrate(final ChronoGraphIndex index) {
		if (index instanceof IChronoGraphVertexIndex) {
			return migrate((IChronoGraphVertexIndex) index);
		} else if (index instanceof IChronoGraphEdgeIndex) {
			return migrate((IChronoGraphEdgeIndex) index);
		} else {
			throw new IllegalStateException("Unknown subclass of ChronoGraphIndex: '" + index.getClass().getName() + "'!");
		}
	}

}
