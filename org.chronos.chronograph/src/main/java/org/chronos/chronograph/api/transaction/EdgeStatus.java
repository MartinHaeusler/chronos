package org.chronos.chronograph.api.transaction;

import org.apache.tinkerpop.gremlin.structure.Edge;

public sealed interface EdgeStatus permits
    EdgeStatus.Exists,
    EdgeStatus.Deleted,
    EdgeStatus.NotFound {

    record Exists(
        long lastModificationTimestamp,
        Edge edge
    ) implements EdgeStatus {

    }

    record Deleted(
        long lastModificationTimestamp
    ) implements EdgeStatus {

    }

    final class NotFound implements EdgeStatus {

        public static final NotFound INSTANCE = new NotFound();

        private NotFound() {

        }

    }

}
