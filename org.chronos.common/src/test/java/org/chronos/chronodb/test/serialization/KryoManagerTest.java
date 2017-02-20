package org.chronos.chronodb.test.serialization;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.chronos.common.serialization.KryoManager;
import org.junit.Test;

public class KryoManagerTest {

	@Test
	public void canSerializeAndDeserialize() {
		Person johnDoe = new Person("John", "Doe");
		byte[] bytes = KryoManager.serialize(johnDoe);
		assertNotNull(bytes);
		Person deserialized = KryoManager.deserialize(bytes);
		assertEquals(johnDoe, deserialized);
	}

	@Test
	public void canDuplicate() {
		Person johnDoe = new Person("John", "Doe");
		Person clone = KryoManager.deepCopy(johnDoe);
		assertEquals(johnDoe, clone);
	}

	@Test
	public void canSerializeAndDeserializeWithFiles() {
		try {
			File testFile = File.createTempFile("tempFile", "binary");
			testFile.deleteOnExit();
			Person johnDoe = new Person("John", "Doe");
			KryoManager.serializeObjectsToFile(testFile, johnDoe);
			Person deserialized = KryoManager.deserializeObjectFromFile(testFile);
			assertNotNull(deserialized);
			assertEquals(johnDoe, deserialized);
		} catch (IOException ioe) {
			fail(ioe.toString());
		}
	}

	@Test
	public void canSerializeAndDeserializeMultipleObjectsWithFiles() {
		try {
			File testFile = File.createTempFile("tempFile", "binary");
			testFile.deleteOnExit();
			Person p1 = new Person("John", "Doe");
			Person p2 = new Person("Jane", "Doe");
			KryoManager.serializeObjectsToFile(testFile, p1, p2);
			List<Object> deserializedObjects = KryoManager.deserializeObjectsFromFile(testFile);
			assertNotNull(deserializedObjects);
			assertEquals(2, deserializedObjects.size());
			assertEquals(p1, deserializedObjects.get(0));
			assertEquals(p2, deserializedObjects.get(1));
		} catch (IOException ioe) {
			fail(ioe.toString());
		}
	}

	// =====================================================================================================================
	// INNER CLASSES
	// =====================================================================================================================

	@SuppressWarnings("unused")
	private static class Person {

		private String firstName;
		private String lastName;

		protected Person() {
			// default constructor for serialization
			this(null, null);
		}

		public Person(final String firstName, final String lastName) {
			this.firstName = firstName;
			this.lastName = lastName;
		}

		public String getFirstName() {
			return this.firstName;
		}

		public void setFirstName(final String firstName) {
			this.firstName = firstName;
		}

		public String getLastName() {
			return this.lastName;
		}

		public void setLastName(final String lastName) {
			this.lastName = lastName;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + (this.firstName == null ? 0 : this.firstName.hashCode());
			result = prime * result + (this.lastName == null ? 0 : this.lastName.hashCode());
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
			Person other = (Person) obj;
			if (this.firstName == null) {
				if (other.firstName != null) {
					return false;
				}
			} else if (!this.firstName.equals(other.firstName)) {
				return false;
			}
			if (this.lastName == null) {
				if (other.lastName != null) {
					return false;
				}
			} else if (!this.lastName.equals(other.lastName)) {
				return false;
			}
			return true;
		}

	}
}
