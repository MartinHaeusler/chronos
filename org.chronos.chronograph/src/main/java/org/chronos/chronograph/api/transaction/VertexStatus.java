package org.chronos.chronograph.api.transaction;

import org.apache.tinkerpop.gremlin.structure.Vertex;

public sealed interface VertexStatus permits
    VertexStatus.Exists,
    VertexStatus.Deleted,
    VertexStatus.NotFound {

    record Exists(
        long lastModificationTimestamp,
        Vertex vertex
    ) implements VertexStatus {

    }

    record Deleted(
        long lastModificationTimestamp
    ) implements VertexStatus {

    }

    final class NotFound implements VertexStatus {

        public static final NotFound INSTANCE = new NotFound();

        private NotFound() {

        }

    }

}
