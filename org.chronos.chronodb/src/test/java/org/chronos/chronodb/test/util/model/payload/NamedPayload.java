package org.chronos.chronodb.test.util.model.payload;

import static com.google.common.base.Preconditions.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.io.FileUtils;

import com.google.common.collect.Sets;

/**
 * A class for testing purposes.
 *
 * <p>
 * Instances of this class are supposed to be used as values for the temporal key-value store. This class provides:
 *
 * <ul>
 * <li>A {@link #name} (not <code>null</code>), to be used for identification purposes
 * <li>A {@link #payload} (not <code>null</code>), to simulate the size of a real-world value object
 * </ul>
 *
 * Note that this class has several factory methods for easier access. Instances of this class are considered to be
 * immutable.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public class NamedPayload implements Serializable {

	// =================================================================================================================
	// SERIAL VERSION ID
	// =================================================================================================================

	private static final long serialVersionUID = 1L;

	// =================================================================================================================
	// STATIC FACTORY METHODS
	// =================================================================================================================

	/**
	 * Factory method. Creates a new {@link NamedPayload} with a random UUID as {@link #name} and a random 1KB
	 * {@link #payload}.
	 *
	 * <p>
	 * <b>Disclaimer:</b> It is only guaranteed that the {@link #payload} has the specified size. The total size of the
	 * returned object as a whole will be larger, due to the memory consumed by the {@link #name} and the overhead of
	 * the JVM. In particular, keep in mind that serialized forms of this class may be subject to compression algorithms
	 * and have different sizes.
	 *
	 * @return The newly created {@link NamedPayload} object.
	 */
	public static NamedPayload create1KB() {
		return createKB(1);
	}

	/**
	 * Factory method. Creates a new {@link NamedPayload} with the given {@link #name} and a random 1KB {@link #payload}
	 * .
	 *
	 * <p>
	 * <b>Disclaimer:</b> It is only guaranteed that the {@link #payload} has the specified size. The total size of the
	 * returned object as a whole will be larger, due to the memory consumed by the {@link #name} and the overhead of
	 * the JVM. In particular, keep in mind that serialized forms of this class may be subject to compression algorithms
	 * and have different sizes.
	 *
	 * @param name
	 *            The name to assign to the new object. Must not be <code>null</code>.
	 *
	 * @return The newly created {@link NamedPayload} object.
	 */
	public static NamedPayload create1KB(final String name) {
		checkNotNull(name, "Precondition violation - argument 'name' must not be NULL!");
		return createKB(name, 1);
	}

	/**
	 * Factory method. Creates a new {@link NamedPayload} with a random UUID as {@link #name} and a random 10KB
	 * {@link #payload}.
	 *
	 * <p>
	 * <b>Disclaimer:</b> It is only guaranteed that the {@link #payload} has the specified size. The total size of the
	 * returned object as a whole will be larger, due to the memory consumed by the {@link #name} and the overhead of
	 * the JVM. In particular, keep in mind that serialized forms of this class may be subject to compression algorithms
	 * and have different sizes.
	 *
	 * @return The newly created {@link NamedPayload} object.
	 */
	public static NamedPayload create10KB() {
		return createKB(10);
	}

	/**
	 * Factory method. Creates a new {@link NamedPayload} with the given {@link #name} and a random 10KB
	 * {@link #payload} .
	 *
	 * <p>
	 * <b>Disclaimer:</b> It is only guaranteed that the {@link #payload} has the specified size. The total size of the
	 * returned object as a whole will be larger, due to the memory consumed by the {@link #name} and the overhead of
	 * the JVM. In particular, keep in mind that serialized forms of this class may be subject to compression algorithms
	 * and have different sizes.
	 *
	 * @param name
	 *            The name to assign to the new object. Must not be <code>null</code>.
	 *
	 * @return The newly created {@link NamedPayload} object.
	 */
	public static NamedPayload create10KB(final String name) {
		checkNotNull(name, "Precondition violation - argument 'name' must not be NULL!");
		return createKB(name, 10);
	}

	/**
	 * Factory method. Creates a new {@link NamedPayload} with a random UUID as {@link #name} and a random 100KB
	 * {@link #payload}.
	 *
	 * <p>
	 * <b>Disclaimer:</b> It is only guaranteed that the {@link #payload} has the specified size. The total size of the
	 * returned object as a whole will be larger, due to the memory consumed by the {@link #name} and the overhead of
	 * the JVM. In particular, keep in mind that serialized forms of this class may be subject to compression algorithms
	 * and have different sizes.
	 *
	 * @return The newly created {@link NamedPayload} object.
	 */
	public static NamedPayload create100KB() {
		return createKB(100);
	}

	/**
	 * Factory method. Creates a new {@link NamedPayload} with the given {@link #name} and a random 100KB
	 * {@link #payload} .
	 *
	 * <p>
	 * <b>Disclaimer:</b> It is only guaranteed that the {@link #payload} has the specified size. The total size of the
	 * returned object as a whole will be larger, due to the memory consumed by the {@link #name} and the overhead of
	 * the JVM. In particular, keep in mind that serialized forms of this class may be subject to compression algorithms
	 * and have different sizes.
	 *
	 * @param name
	 *            The name to assign to the new object. Must not be <code>null</code>.
	 *
	 * @return The newly created {@link NamedPayload} object.
	 */
	public static NamedPayload create100KB(final String name) {
		checkNotNull(name, "Precondition violation - argument 'name' must not be NULL!");
		return createKB(name, 100);
	}

	/**
	 * Factory method. Creates a new {@link NamedPayload} with a random UUID as {@link #name} and a random 1MB
	 * {@link #payload}.
	 *
	 * <p>
	 * <b>Disclaimer:</b> It is only guaranteed that the {@link #payload} has the specified size. The total size of the
	 * returned object as a whole will be larger, due to the memory consumed by the {@link #name} and the overhead of
	 * the JVM. In particular, keep in mind that serialized forms of this class may be subject to compression algorithms
	 * and have different sizes.
	 *
	 * @return The newly created {@link NamedPayload} object.
	 */
	public static NamedPayload create1MB() {
		return createMB(1);
	}

	/**
	 * Factory method. Creates a new {@link NamedPayload} with the given {@link #name} and a random 1MB {@link #payload}
	 * .
	 *
	 * <p>
	 * <b>Disclaimer:</b> It is only guaranteed that the {@link #payload} has the specified size. The total size of the
	 * returned object as a whole will be larger, due to the memory consumed by the {@link #name} and the overhead of
	 * the JVM. In particular, keep in mind that serialized forms of this class may be subject to compression algorithms
	 * and have different sizes.
	 *
	 * @param name
	 *            The name to assign to the new object. Must not be <code>null</code>.
	 *
	 * @return The newly created {@link NamedPayload} object.
	 */
	public static NamedPayload create1MB(final String name) {
		checkNotNull(name, "Precondition violation - argument 'name' must not be NULL!");
		return createMB(name, 1);
	}

	/**
	 * Factory method. Creates a new {@link NamedPayload} with a random UUID as {@link #name} and a random
	 * {@link #payload} of the specified size.
	 *
	 * <p>
	 * <b>Disclaimer:</b> It is only guaranteed that the {@link #payload} has the specified size. The total size of the
	 * returned object as a whole will be larger, due to the memory consumed by the {@link #name} and the overhead of
	 * the JVM. In particular, keep in mind that serialized forms of this class may be subject to compression algorithms
	 * and have different sizes.
	 *
	 * @param payloadKiloBytes
	 *            The size of the intended payload, in kilobytes (KB). Must not be negative.
	 *
	 * @return The newly created {@link NamedPayload} object.
	 */
	public static NamedPayload createKB(final int payloadKiloBytes) {
		return createKB(UUID.randomUUID().toString(), payloadKiloBytes);
	}

	/**
	 * Factory method. Creates a new {@link NamedPayload} with the given {@link #name} and a random {@link #payload} of
	 * the specified size.
	 *
	 * <p>
	 * <b>Disclaimer:</b> It is only guaranteed that the {@link #payload} has the specified size. The total size of the
	 * returned object as a whole will be larger, due to the memory consumed by the {@link #name} and the overhead of
	 * the JVM. In particular, keep in mind that serialized forms of this class may be subject to compression algorithms
	 * and have different sizes.
	 *
	 * @param name
	 *            The name to assign to the new object. Must not be <code>null</code>.
	 * @param payloadKiloBytes
	 *            The size of the intended payload, in kilobytes (KB). Must not be negative.
	 *
	 * @return The newly created {@link NamedPayload} object.
	 */
	public static NamedPayload createKB(final String name, final int payloadKiloBytes) {
		checkNotNull(name, "Precondition violation - argument 'name' must not be NULL!");
		checkArgument(payloadKiloBytes >= 0,
				"Precondition violation - argument 'payloadKiloBytes' (value: " + payloadKiloBytes + ") must be >= 0!");
		return new NamedPayload(name, payloadKiloBytes * 1024);
	}

	/**
	 * Factory method. Creates a new {@link NamedPayload} with a random UUID as {@link #name} and a random
	 * {@link #payload} of the specified size.
	 *
	 * <p>
	 * <b>Disclaimer:</b> It is only guaranteed that the {@link #payload} has the specified size. The total size of the
	 * returned object as a whole will be larger, due to the memory consumed by the {@link #name} and the overhead of
	 * the JVM. In particular, keep in mind that serialized forms of this class may be subject to compression algorithms
	 * and have different sizes.
	 *
	 * @param payloadMegaBytes
	 *            The size of the intended payload, in megabytes (MB). Must not be negative.
	 *
	 * @return The newly created {@link NamedPayload} object.
	 */
	public static NamedPayload createMB(final int payloadMegaBytes) {
		return createKB(UUID.randomUUID().toString(), payloadMegaBytes);
	}

	/**
	 * Factory method. Creates a new {@link NamedPayload} with the given {@link #name} and a random {@link #payload} of
	 * the specified size.
	 *
	 * <p>
	 * <b>Disclaimer:</b> It is only guaranteed that the {@link #payload} has the specified size. The total size of the
	 * returned object as a whole will be larger, due to the memory consumed by the {@link #name} and the overhead of
	 * the JVM. In particular, keep in mind that serialized forms of this class may be subject to compression algorithms
	 * and have different sizes.
	 *
	 * @param name
	 *            The name to assign to the new object. Must not be <code>null</code>.
	 * @param payloadMegaBytes
	 *            The size of the intended payload, in megabytes (MB). Must not be negative.
	 *
	 * @return The newly created {@link NamedPayload} object.
	 */
	public static NamedPayload createMB(final String name, final int payloadMegaBytes) {
		checkNotNull(name, "Precondition violation - argument 'name' must not be NULL!");
		checkArgument(payloadMegaBytes >= 0,
				"Precondition violation - argument 'payloadMegaBytes' (value: " + payloadMegaBytes + ") must be >= 0!");
		return new NamedPayload(name, payloadMegaBytes * 1024 * 1024);
	}

	/**
	 * Creates the given number of {@link NamedPayload} instances.
	 *
	 * <p>
	 * Each instance will have a random {@link UUID} as name and a random payload of the given size.
	 *
	 * @param numberOfInstances
	 *            The number of instances to create. Must be greater than zero.
	 * @param payloadKiloBytes
	 *            The size of the payload for each instance, in kilobytes (KB). Must not be negative.
	 * @return The set of created instances.
	 */
	public static Set<NamedPayload> createMany(final int numberOfInstances, final int payloadKiloBytes) {
		checkArgument(numberOfInstances > 0, "Precondition violation - argument 'numberOfInstances' (value: "
				+ numberOfInstances + ") must be > 0!");
		checkArgument(payloadKiloBytes >= 0,
				"Precondition violation - argument 'payloadKiloBytes' (value: " + payloadKiloBytes + ") must be >= 0!");
		Set<NamedPayload> resultSet = Sets.newHashSet();
		for (int i = 0; i < numberOfInstances; i++) {
			NamedPayload instance = createKB(payloadKiloBytes);
			resultSet.add(instance);
		}
		return resultSet;
	}

	// =================================================================================================================
	// PROPERTIES
	// =================================================================================================================

	/** The name of this object. Never <code>null</code>. */
	private String name;
	/** The actual payload data contained in this object. */
	private byte[] payload;

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	protected NamedPayload() {
		// default constructor for serialization purposes
	}

	/**
	 * Creates a new {@link NamedPayload} instance with the given parameters.
	 *
	 * <p>
	 * Please use one of the factory methods provided by this class instead for better readability.
	 *
	 * @param name
	 *            The name of the new object. Must not be <code>null</code>.
	 * @param payloadSizeBytes
	 *            The intended size of the payload, in Bytes. Must not be negative.
	 */
	protected NamedPayload(final String name, final int payloadSizeBytes) {
		checkNotNull(name, "Precondition violation - argument 'name' must not be NULL!");
		checkArgument(payloadSizeBytes >= 0,
				"Precondition violation - argument 'payloadSizeBytes' (value: " + payloadSizeBytes + ") must be >= 0!");
		this.name = name;
		this.payload = new byte[payloadSizeBytes];
		new Random().nextBytes(this.payload);
	}

	// =================================================================================================================
	// GETTERS & SETTERS
	// =================================================================================================================

	/**
	 * Returns the size of the contained payload in Bytes.
	 *
	 * @return The size of the containd payload in Bytes.
	 */
	public int getPayloadSizeInBytes() {
		return this.payload.length;
	}

	/**
	 * Returns a copy of the internal payload array.
	 *
	 * @return A copy of the internal payload array. Never <code>null</code>. Modifications on the returned array will
	 *         not modify the internal state of this object.
	 */
	public byte[] getPayload() {
		byte[] result = new byte[this.getPayloadSizeInBytes()];
		System.arraycopy(this.payload, 0, result, 0, this.getPayloadSizeInBytes());
		return result;
	}

	/**
	 * Returns the name of this object.
	 *
	 * @return The name of this object. Never <code>null</code>.
	 */
	public String getName() {
		return this.name;
	}

	// =====================================================================================================================
	// HASH CODE & EQUALS
	// =====================================================================================================================

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (this.name == null ? 0 : this.name.hashCode());
		result = prime * result + Arrays.hashCode(this.payload);
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
		NamedPayload other = (NamedPayload) obj;
		if (this.name == null) {
			if (other.name != null) {
				return false;
			}
		} else if (!this.name.equals(other.name)) {
			return false;
		}
		if (!Arrays.equals(this.payload, other.payload)) {
			return false;
		}
		return true;
	}

	// =================================================================================================================
	// TO STRING
	// =================================================================================================================

	@Override
	public String toString() {
		return "NamedPayload['" + this.getName() + "', " + FileUtils.byteCountToDisplaySize(this.payload.length) + "]";
	}

}
