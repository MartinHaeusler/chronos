package org.chronos.chronodb.test.util.model.person;

import java.util.Set;

import org.chronos.chronodb.api.indexing.StringIndexer;

public abstract class PersonIndexer implements StringIndexer {

	// =====================================================================================================================
	// STATIC FACTORY METHODS
	// =====================================================================================================================

	public static FirstNameIndexer firstName() {
		return new FirstNameIndexer();
	}

	public static LastNameIndexer lastName() {
		return new LastNameIndexer();
	}

	public static FavoriteColorIndexer favoriteColor() {
		return new FavoriteColorIndexer();
	}

	public static PetsIndexer pets() {
		return new PetsIndexer();
	}

	public static HobbiesIndexer hobbies() {
		return new HobbiesIndexer();
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	@Override
	public boolean canIndex(final Object object) {
		return object instanceof Person;
	}

	@Override
	public Set<String> getIndexValues(final Object object) {
		return this.getIndexValuesInternal((Person) object);
	}

	// =====================================================================================================================
	// ABSTRACT METHOD DECLARATIONS
	// =====================================================================================================================

	protected abstract Set<String> getIndexValuesInternal(Person person);

}
