package org.chronos.chronodb.test.util.model.person;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.Sets;

public class Person {

	// =====================================================================================================================
	// STATIC METHODS
	// =====================================================================================================================

	public static Person generateRandom() {
		return PersonGenerator.generateRandomPerson();
	}

	public static List<Person> generateRandom(final int setSize) {
		return PersonGenerator.generateRandomPersons(setSize);
	}

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	private String id;
	private String firstName;
	private String lastName;
	private String favoriteColor;
	private Set<String> hobbies;
	private Set<String> pets;

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	public Person() {
		this.id = UUID.randomUUID().toString();
		this.hobbies = Sets.newHashSet();
		this.pets = Sets.newHashSet();
	}

	public Person(final String firstName, final String lastName) {
		this();
		this.setFirstName(firstName);
		this.setLastName(lastName);
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	public String getId() {
		return this.id;
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

	public String getFavoriteColor() {
		return this.favoriteColor;
	}

	public void setFavoriteColor(final String favoriteColor) {
		this.favoriteColor = favoriteColor;
	}

	public Set<String> getHobbies() {
		return this.hobbies;
	}

	public void setHobbies(final Set<String> hobbies) {
		this.hobbies = Sets.newHashSet(hobbies);
	}

	public void setHobbies(final String... hobbies) {
		this.hobbies = Sets.newHashSet(hobbies);
	}

	public Set<String> getPets() {
		return this.pets;
	}

	public void setPets(final Set<String> pets) {
		this.pets = Sets.newHashSet(pets);
	}

	public void setPets(final String... pets) {
		this.pets = Sets.newHashSet(pets);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (this.id == null ? 0 : this.id.hashCode());
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
		if (this.id == null) {
			if (other.id != null) {
				return false;
			}
		} else if (!this.id.equals(other.id)) {
			return false;
		}
		return true;
	}

	public boolean contentEquals(final Person obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (this.getClass() != obj.getClass()) {
			return false;
		}
		Person other = obj;
		if (this.favoriteColor == null) {
			if (other.favoriteColor != null) {
				return false;
			}
		} else if (!this.favoriteColor.equals(other.favoriteColor)) {
			return false;
		}
		if (this.firstName == null) {
			if (other.firstName != null) {
				return false;
			}
		} else if (!this.firstName.equals(other.firstName)) {
			return false;
		}
		if (this.hobbies == null) {
			if (other.hobbies != null) {
				return false;
			}
		} else if (!this.hobbies.equals(other.hobbies)) {
			return false;
		}
		if (this.id == null) {
			if (other.id != null) {
				return false;
			}
		} else if (!this.id.equals(other.id)) {
			return false;
		}
		if (this.lastName == null) {
			if (other.lastName != null) {
				return false;
			}
		} else if (!this.lastName.equals(other.lastName)) {
			return false;
		}
		if (this.pets == null) {
			if (other.pets != null) {
				return false;
			}
		} else if (!this.pets.equals(other.pets)) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return "P['" + this.firstName + "' '" + this.lastName + "']";
	}

}
