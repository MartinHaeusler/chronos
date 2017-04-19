package org.chronos.common.version;

import static com.google.common.base.Preconditions.*;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.chronos.common.buildinfo.ChronosBuildInfo;

public final class ChronosVersion implements Comparable<ChronosVersion> {

	// =====================================================================================================================
	// OTHER CONSTANTS
	// =====================================================================================================================

	private static final String CHRONOS_VERSION_REGEX = "([0-9]+)\\.([0-9]+)\\.([0-9]+)([-\\.:]([a-zA-Z]+))?";
	private static final Pattern CHRONOS_VERSION_PATTERN = Pattern.compile(CHRONOS_VERSION_REGEX,
			Pattern.CASE_INSENSITIVE);

	// =====================================================================================================================
	// STATIC FACTORY METHODS
	// =====================================================================================================================

	public static ChronosVersion parse(final String stringVersion) {
		checkNotNull(stringVersion, "Precondition violation - argument 'stringVersion' must not be NULL!");
		Matcher matcher = CHRONOS_VERSION_PATTERN.matcher(stringVersion);
		if (matcher.matches() == false) {
			throw new IllegalArgumentException(
					"The given string is no valid Chronos Version: '" + stringVersion + "'!");
		}
		int majorVersion = 0;
		int minorVersion = 0;
		int patchVersion = 0;
		VersionKind kind = null;
		try {
			// NOTE: Group 0 is the entire match!
			majorVersion = Integer.parseInt(matcher.group(1));
			minorVersion = Integer.parseInt(matcher.group(2));
			patchVersion = Integer.parseInt(matcher.group(3));
			String kindString = matcher.group(5);
			if (kindString == null || kindString.trim().isEmpty()) {
				// missing "kind" information is interpreted as RELEASE
				kind = VersionKind.RELEASE;
			} else {
				kind = VersionKind.parse(kindString);
			}
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException(
					"The given string is no valid Chronos Version: '" + stringVersion + "'!");
		}
		return new ChronosVersion(majorVersion, minorVersion, patchVersion, kind);
	}

	public static ChronosVersion getCurrentVersion() {
		String buildVersion = ChronosBuildInfo.getConfiguration().getBuildVersion();
		return ChronosVersion.parse(buildVersion);
	}

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	private final int majorVersion;
	private final int minorVersion;
	private final int patchVersion;
	private final VersionKind versionKind;

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	public ChronosVersion(final int major, final int minor, final int patch, final VersionKind kind) {
		checkArgument(major >= 0, "Precondition violation - argument 'major' must not be negative!");
		checkArgument(minor >= 0, "Precondition violation - argument 'minor' must not be negative!");
		checkArgument(patch >= 0, "Precondition violation - argument 'patch' must not be negative!");
		checkNotNull(kind, "Precondition violation - argument 'kind' must not be NULL!");
		this.majorVersion = major;
		this.minorVersion = minor;
		this.patchVersion = patch;
		this.versionKind = kind;
	}

	// =====================================================================================================================
	// GETTERS & SETTERS
	// =====================================================================================================================

	public int getMajorVersion() {
		return this.majorVersion;
	}

	public int getMinorVersion() {
		return this.minorVersion;
	}

	public int getPatchVersion() {
		return this.patchVersion;
	}

	public VersionKind getVersionKind() {
		return this.versionKind;
	}

	public boolean isReadCompatibleWith(final ChronosVersion other) {
		checkNotNull(other, "Precondition violation - argument 'other' must not be NULL!");
		if (this.compareTo(other) >= 0) {
			// we can read past revisions
			return true;
		}
		// we define "read compatibility" for DBs written by newer versions as "having the same major AND minor version
		// here.
		if (this.getMajorVersion() == other.getMajorVersion() && this.getMinorVersion() == other.getMinorVersion()) {
			return true;
		} else {
			return false;
		}
	}

	// =====================================================================================================================
	// COMPARATOR
	// =====================================================================================================================

	@Override
	public int compareTo(final ChronosVersion o) {
		if (o == null) {
			return 1;
		}
		// first order by major version
		if (this.getMajorVersion() > o.getMajorVersion()) {
			return 1;
		}
		if (this.getMajorVersion() < o.getMajorVersion()) {
			return -1;
		}
		// then order by minor version
		if (this.getMinorVersion() > o.getMinorVersion()) {
			return 1;
		}
		if (this.getMinorVersion() < o.getMinorVersion()) {
			return -1;
		}
		// then order by patch version
		if (this.getPatchVersion() > o.getPatchVersion()) {
			return 1;
		}
		if (this.getPatchVersion() < o.getPatchVersion()) {
			return -1;
		}
		// ultimately, check the version kinds (RELEASE > SNAPSHOT)
		if (this.getVersionKind().equals(VersionKind.RELEASE) && o.getVersionKind().equals(VersionKind.SNAPSHOT)) {
			return 1;
		}
		if (this.getVersionKind().equals(VersionKind.SNAPSHOT) && o.getVersionKind().equals(VersionKind.RELEASE)) {
			return -1;
		}
		// in any other case, they HAVE to be equal
		return 0;
	};

	// =====================================================================================================================
	// HASH CODE & EQUALS
	// =====================================================================================================================

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.majorVersion;
		result = prime * result + this.minorVersion;
		result = prime * result + this.patchVersion;
		result = prime * result + (this.versionKind == null ? 0 : this.versionKind.hashCode());
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (this.getClass() != obj.getClass()) {
			return false;
		}
		ChronosVersion other = (ChronosVersion) obj;
		if (this.majorVersion != other.majorVersion) {
			return false;
		}
		if (this.minorVersion != other.minorVersion) {
			return false;
		}
		if (this.patchVersion != other.patchVersion) {
			return false;
		}
		if (this.versionKind != other.versionKind) {
			return false;
		}
		return true;
	}

	// =====================================================================================================================
	// TO STRING
	// =====================================================================================================================

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(this.getMajorVersion());
		builder.append(".");
		builder.append(this.getMinorVersion());
		builder.append(".");
		builder.append(this.getPatchVersion());
		builder.append("-");
		builder.append(this.getVersionKind());
		return builder.toString();
	}

}
