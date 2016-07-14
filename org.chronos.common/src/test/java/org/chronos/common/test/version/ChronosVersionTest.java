package org.chronos.common.test.version;

import static com.google.common.base.Preconditions.*;
import static org.junit.Assert.*;

import org.chronos.common.version.ChronosVersion;
import org.chronos.common.version.VersionKind;
import org.junit.Test;

public class ChronosVersionTest {

	@Test
	public void canGetCurrentVersion() {
		ChronosVersion currentVersion = ChronosVersion.getCurrentVersion();
		checkNotNull(currentVersion, "Precondition violation - argument 'currentVersion' must not be NULL!");
	}

	@Test
	public void canConvertToAndFromString() {
		ChronosVersion version1 = new ChronosVersion(1, 2, 3, VersionKind.RELEASE);
		ChronosVersion version2 = new ChronosVersion(0, 0, 0, VersionKind.SNAPSHOT);
		ChronosVersion version3 = new ChronosVersion(0, 1, 0, VersionKind.RELEASE);
		ChronosVersion version4 = new ChronosVersion(1, 0, 0, VersionKind.RELEASE);

		assertEquals(version1, ChronosVersion.parse(version1.toString()));
		assertEquals(version2, ChronosVersion.parse(version2.toString()));
		assertEquals(version3, ChronosVersion.parse(version3.toString()));
		assertEquals(version4, ChronosVersion.parse(version4.toString()));
	}
}
