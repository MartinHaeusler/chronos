package org.chronos.chronosphere.testutils;

import static com.google.common.base.Preconditions.*;

import java.util.function.Consumer;

import org.chronos.chronosphere.api.ChronoSphere;
import org.chronos.chronosphere.api.ChronoSphereTransaction;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;

public class ChronoSphereTestUtils {

	public static void assertCommitAssert(final ChronoSphereTransaction tx,
			final Consumer<ChronoSphereTransaction> assertion) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(assertion, "Precondition violation - argument 'assertion' must not be NULL!");
		ChronoSphere sphere = ((ChronoSphereTransactionInternal) tx).getOwningSphere();
		assertion.accept(tx);
		tx.commit();
		try (ChronoSphereTransaction tx2 = sphere.tx(tx.getBranch().getName())) {
			assertion.accept(tx2);
		}
	}

}
