package org.chronos.chronodb.test.util.model.person;

import static com.google.common.base.Preconditions.*;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.io.FileUtils;
import org.chronos.chronodb.test.util.TestUtils;

import com.google.common.collect.Lists;

public class PersonGenerator {

	private static final List<String> FIRST_NAMES;
	private static final List<String> LAST_NAMES;
	private static final List<String> COLORS = Lists.newArrayList("red", "green", "blue", "orange", "yellow");
	private static final List<String> HOBBIES = Lists.newArrayList("reading", "tv", "cinema", "swimming", "skiing",
			"gaming");
	private static final List<String> PETS = Lists.newArrayList("cat", "dog", "fish", "horse", "mouse", "rat");

	static {
		List<String> firstNames = Collections.emptyList();
		try {
			firstNames = FileUtils.readLines(new File(PersonGenerator.class.getResource("/firstNames.txt").getFile()));
		} catch (Exception e) {
			e.printStackTrace();
		}
		FIRST_NAMES = Collections.unmodifiableList(firstNames);
		List<String> lastNames = Collections.emptyList();
		try {
			lastNames = FileUtils.readLines(new File(PersonGenerator.class.getResource("/lastNames.txt").getFile()));
		} catch (Exception e) {
			e.printStackTrace();
		}
		LAST_NAMES = Collections.unmodifiableList(lastNames);
	}

	public static Person generateRandomPerson() {
		Person person = new Person();
		person.setFirstName(TestUtils.getRandomEntryOf(FIRST_NAMES));
		person.setLastName(TestUtils.getRandomEntryOf(LAST_NAMES));
		person.setFavoriteColor(TestUtils.getRandomEntryOf(COLORS));
		int numberOfHobbies = TestUtils.randomBetween(0, 3);
		int numberOfPets = TestUtils.randomBetween(0, 2);
		person.getHobbies().addAll(TestUtils.getRandomUniqueEntriesOf(HOBBIES, numberOfHobbies));
		person.getPets().addAll(TestUtils.getRandomUniqueEntriesOf(PETS, numberOfPets));
		return person;
	}

	public static List<Person> generateRandomPersons(final int number) {
		checkArgument(number > 0, "Precondition violation - argument 'number' must be positive!");
		return IntStream.range(0, number).parallel().mapToObj(i -> generateRandomPerson()).collect(Collectors.toList());
	}

}
