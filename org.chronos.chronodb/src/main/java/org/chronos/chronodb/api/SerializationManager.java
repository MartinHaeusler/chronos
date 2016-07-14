package org.chronos.chronodb.api;

/**
 * The {@link SerializationManager} is responsible for conversion between {@link Object} and <code>byte[]</code>
 * representation.
 *
 * <p>
 * Every {@link ChronoDB} instance has its own SerializationManager, which can be retrieved via
 * {@link ChronoDB#getSerializationManager()}.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface SerializationManager {

	/**
	 * Serializes the given object into its <code>byte[]</code> representation.
	 *
	 * @param object
	 *            The object to serialize. Must not be <code>null</code>.
	 *
	 * @return The serial form of the passed object.
	 */
	public byte[] serialize(Object object);

	/**
	 * Deserializes the given <code>byte[]</code> back into its {@link Object} representation.
	 *
	 * @param serialForm
	 *            The byte array to deserialize. Must not be <code>null</code>.
	 *
	 * @return The object representation of the passed byte array.
	 */
	public Object deserialize(byte[] serialForm);

}
