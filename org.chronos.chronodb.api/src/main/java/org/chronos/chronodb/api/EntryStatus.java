package org.chronos.chronodb.api;

import org.chronos.chronodb.api.EntryStatus.Deleted;
import org.chronos.chronodb.api.EntryStatus.Exists;
import org.chronos.chronodb.api.EntryStatus.NotFound;

public sealed interface EntryStatus permits Exists, Deleted, NotFound {

    record Exists(
        long lastModificationTimestamp,
        Object value
    ) implements EntryStatus {

    }

    record Deleted(
        long lastModificationTimestamp
    ) implements EntryStatus {

    }

    final class NotFound implements EntryStatus {

        public static final NotFound INSTANCE = new NotFound();

        private NotFound() {

        }

    }

}
