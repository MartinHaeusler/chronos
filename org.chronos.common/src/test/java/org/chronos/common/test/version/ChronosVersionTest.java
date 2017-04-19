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

	@Test
	public void noTagIsInterpretedAsRelease() {
		ChronosVersion version = new ChronosVersion(0, 5, 5, VersionKind.RELEASE);
		assertEquals(version, ChronosVersion.parse("0.5.5"));
	}

	@Test
	public void readCompatibilityIsIndicatedCorrectly() {
		ChronosVersion v054snapshot = new ChronosVersion(0, 5, 4, VersionKind.SNAPSHOT);
		ChronosVersion v054release = new ChronosVersion(0, 5, 4, VersionKind.RELEASE);
		ChronosVersion v055snapshot = new ChronosVersion(0, 5, 5, VersionKind.SNAPSHOT);
		ChronosVersion v060snapshot = new ChronosVersion(0, 6, 0, VersionKind.SNAPSHOT);

		assertTrue(v054snapshot.isReadCompatibleWith(v054release));
		assertTrue(v054snapshot.isReadCompatibleWith(v055snapshot));
		assertTrue(v054release.isReadCompatibleWith(v054snapshot));
		assertTrue(v055snapshot.isReadCompatibleWith(v054snapshot));

		assertTrue(v060snapshot.isReadCompatibleWith(v054snapshot));
		assertTrue(v060snapshot.isReadCompatibleWith(v054release));
		assertTrue(v060snapshot.isReadCompatibleWith(v055snapshot));

		assertFalse(v054snapshot.isReadCompatibleWith(v060snapshot));
		assertFalse(v054release.isReadCompatibleWith(v060snapshot));
		assertFalse(v055snapshot.isReadCompatibleWith(v060snapshot));

	}
}
