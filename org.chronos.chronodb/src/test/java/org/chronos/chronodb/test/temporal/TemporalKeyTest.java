package org.chronos.chronodb.test.temporal;

import static org.junit.Assert.*;

import java.util.Set;

import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.api.key.ChronoIdentifier;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.api.key.TemporalKey;
import org.chronos.chronodb.test.base.ChronoDBUnitTest;
import org.chronos.common.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Sets;

@Category(UnitTest.class)
public class TemporalKeyTest extends ChronoDBUnitTest {

	@Test
	public void canCreateQualifiedKey() {
		QualifiedKey qKey = QualifiedKey.create("myKeyspace", "hello");
		assertNotNull(qKey);
		assertEquals("hello", qKey.getKey());
		assertEquals("myKeyspace", qKey.getKeyspace());
	}

	@Test
	public void canCreateQualifiedKeyInDefaultKeyspace() {
		QualifiedKey qKey = QualifiedKey.createInDefaultKeyspace("hello");
		assertNotNull(qKey);
		assertEquals("hello", qKey.getKey());
		assertEquals(ChronoDBConstants.DEFAULT_KEYSPACE_NAME, qKey.getKeyspace());
	}

	@Test
	public void qualifiedKeyImplementsHashCodeAndEqualsCorrectly() {
		Set<QualifiedKey> set = Sets.newHashSet();
		// uniques
		set.add(QualifiedKey.create("a", "1"));
		set.add(QualifiedKey.create("a", "2"));
		set.add(QualifiedKey.create("b", "1"));
		set.add(QualifiedKey.create("b", "2"));
		// duplicates
		set.add(QualifiedKey.create("a", "1"));
		set.add(QualifiedKey.create("b", "1"));
		assertEquals(4, set.size());
		// containment checks
		assertTrue(set.contains(QualifiedKey.create("a", "1")));
		assertTrue(set.contains(QualifiedKey.create("a", "2")));
		assertTrue(set.contains(QualifiedKey.create("b", "1")));
		assertTrue(set.contains(QualifiedKey.create("b", "2")));
	}

	@Test
	public void canCreateTemporalKey() {
		TemporalKey tKey = TemporalKey.create(1000L, "myKeyspace", "hello");
		assertNotNull(tKey);
		assertEquals(1000L, tKey.getTimestamp());
		assertEquals("myKeyspace", tKey.getKeyspace());
		assertEquals("hello", tKey.getKey());
	}

	@Test
	public void canCreateTemporalKeyFromQualifiedKey() {
		QualifiedKey qKey = QualifiedKey.create("myKeyspace", "hello");
		assertNotNull(qKey);
		TemporalKey tKey = TemporalKey.create(1000L, qKey);
		assertNotNull(tKey);
		assertEquals(1000L, tKey.getTimestamp());
		assertEquals("myKeyspace", tKey.getKeyspace());
		assertEquals("hello", tKey.getKey());
	}

	@Test
	public void canCreateTemporalKeyAtMinimumTimestamp() {
		TemporalKey tKey = TemporalKey.createMinTime("myKeyspace", "hello");
		assertNotNull(tKey);
		assertEquals(0L, tKey.getTimestamp());
		assertEquals("myKeyspace", tKey.getKeyspace());
		assertEquals("hello", tKey.getKey());
	}

	@Test
	public void canCreateTemporalKeyAtMaximumTimestamp() {
		TemporalKey tKey = TemporalKey.createMaxTime("myKeyspace", "hello");
		assertNotNull(tKey);
		assertEquals(Long.MAX_VALUE, tKey.getTimestamp());
		assertEquals("myKeyspace", tKey.getKeyspace());
		assertEquals("hello", tKey.getKey());
	}

	@Test
	public void canConvertTemporalKeyIntoQualifiedKey() {
		TemporalKey tKey = TemporalKey.create(1000L, "myKeyspace", "hello");
		assertNotNull(tKey);
		QualifiedKey qKey = tKey.toQualifiedKey();
		assertNotNull(qKey);
		assertEquals("myKeyspace", qKey.getKeyspace());
		assertEquals("hello", qKey.getKey());
	}

	@Test
	public void temporalKeyImplementsHashCodeAndEqualsCorrectly() {
		Set<TemporalKey> set = Sets.newHashSet();
		// uniques
		set.add(TemporalKey.create(10L, "a", "1"));
		set.add(TemporalKey.create(10L, "a", "2"));
		set.add(TemporalKey.create(10L, "b", "1"));
		set.add(TemporalKey.create(10L, "b", "2"));
		set.add(TemporalKey.create(100L, "a", "1"));
		set.add(TemporalKey.create(100L, "a", "2"));
		set.add(TemporalKey.create(100L, "b", "1"));
		set.add(TemporalKey.create(100L, "b", "2"));
		// duplicates
		set.add(TemporalKey.create(10L, "a", "1"));
		set.add(TemporalKey.create(10L, "a", "2"));
		set.add(TemporalKey.create(10L, "b", "1"));
		set.add(TemporalKey.create(10L, "b", "2"));
		set.add(TemporalKey.create(100L, "a", "1"));
		set.add(TemporalKey.create(100L, "a", "2"));
		set.add(TemporalKey.create(100L, "b", "1"));
		set.add(TemporalKey.create(100L, "b", "2"));
		assertEquals(8, set.size());
		// containment checks
		assertTrue(set.contains(TemporalKey.create(10L, "a", "1")));
		assertTrue(set.contains(TemporalKey.create(10L, "b", "1")));
		assertTrue(set.contains(TemporalKey.create(10L, "a", "2")));
		assertTrue(set.contains(TemporalKey.create(10L, "b", "2")));
		assertTrue(set.contains(TemporalKey.create(100L, "a", "1")));
		assertTrue(set.contains(TemporalKey.create(100L, "b", "1")));
		assertTrue(set.contains(TemporalKey.create(100L, "a", "2")));
		assertTrue(set.contains(TemporalKey.create(100L, "b", "2")));
	}

	@Test
	public void canCreateChronoIdentifier() {
		ChronoIdentifier id = ChronoIdentifier.create("myBranch", 1000L, "myKeyspace", "hello");
		assertNotNull(id);
		assertEquals("myBranch", id.getBranchName());
		assertEquals(1000L, id.getTimestamp());
		assertEquals("myKeyspace", id.getKeyspace());
		assertEquals("hello", id.getKey());
	}

	@Test
	public void canCreateChronoIdentifierFromTemporalKey() {
		TemporalKey tKey = TemporalKey.create(1000L, "myKeyspace", "hello");
		assertNotNull(tKey);
		ChronoIdentifier id = ChronoIdentifier.create("myBranch", tKey);
		assertEquals("myBranch", id.getBranchName());
		assertEquals(1000L, id.getTimestamp());
		assertEquals("myKeyspace", id.getKeyspace());
		assertEquals("hello", id.getKey());
	}

	@Test
	public void canCreateChronoIdentifierFromQualifiedKey() {
		QualifiedKey qKey = QualifiedKey.create("myKeyspace", "hello");
		assertNotNull(qKey);
		ChronoIdentifier id = ChronoIdentifier.create("myBranch", 1000L, qKey);
		assertEquals("myBranch", id.getBranchName());
		assertEquals(1000L, id.getTimestamp());
		assertEquals("myKeyspace", id.getKeyspace());
		assertEquals("hello", id.getKey());
	}

	@Test
	public void canConvertChronoIdentifierToTemporalKey() {
		ChronoIdentifier id = ChronoIdentifier.create("myBranch", 1000L, "myKeyspace", "hello");
		assertNotNull(id);
		TemporalKey tKey = id.toTemporalKey();
		assertEquals(1000L, tKey.getTimestamp());
		assertEquals("myKeyspace", tKey.getKeyspace());
		assertEquals("hello", tKey.getKey());
	}

	@Test
	public void canConvertChronoIdentifierToQualifiedKey() {
		ChronoIdentifier id = ChronoIdentifier.create("myBranch", 1000L, "myKeyspace", "hello");
		assertNotNull(id);
		QualifiedKey tKey = id.toQualifiedKey();
		assertEquals("myKeyspace", tKey.getKeyspace());
		assertEquals("hello", tKey.getKey());
	}

	@Test
	public void chronoIdentifierImplementsHashCodeAndEqualsCorrectly() {
		Set<ChronoIdentifier> set = Sets.newHashSet();
		// uniques
		set.add(ChronoIdentifier.create("master", 10L, "a", "1"));
		set.add(ChronoIdentifier.create("master", 10L, "a", "2"));
		set.add(ChronoIdentifier.create("master", 10L, "b", "1"));
		set.add(ChronoIdentifier.create("master", 10L, "b", "2"));
		set.add(ChronoIdentifier.create("master", 100L, "a", "1"));
		set.add(ChronoIdentifier.create("master", 100L, "a", "2"));
		set.add(ChronoIdentifier.create("master", 100L, "b", "1"));
		set.add(ChronoIdentifier.create("master", 100L, "b", "2"));
		set.add(ChronoIdentifier.create("myBranch", 10L, "a", "1"));
		set.add(ChronoIdentifier.create("myBranch", 10L, "a", "2"));
		set.add(ChronoIdentifier.create("myBranch", 10L, "b", "1"));
		set.add(ChronoIdentifier.create("myBranch", 10L, "b", "2"));
		set.add(ChronoIdentifier.create("myBranch", 100L, "a", "1"));
		set.add(ChronoIdentifier.create("myBranch", 100L, "a", "2"));
		set.add(ChronoIdentifier.create("myBranch", 100L, "b", "1"));
		set.add(ChronoIdentifier.create("myBranch", 100L, "b", "2"));
		// duplicates
		set.add(ChronoIdentifier.create("master", 10L, "a", "1"));
		set.add(ChronoIdentifier.create("master", 10L, "a", "2"));
		set.add(ChronoIdentifier.create("master", 10L, "b", "1"));
		set.add(ChronoIdentifier.create("master", 10L, "b", "2"));
		set.add(ChronoIdentifier.create("master", 100L, "a", "1"));
		set.add(ChronoIdentifier.create("master", 100L, "a", "2"));
		set.add(ChronoIdentifier.create("master", 100L, "b", "1"));
		set.add(ChronoIdentifier.create("master", 100L, "b", "2"));
		set.add(ChronoIdentifier.create("myBranch", 10L, "a", "1"));
		set.add(ChronoIdentifier.create("myBranch", 10L, "a", "2"));
		set.add(ChronoIdentifier.create("myBranch", 10L, "b", "1"));
		set.add(ChronoIdentifier.create("myBranch", 10L, "b", "2"));
		set.add(ChronoIdentifier.create("myBranch", 100L, "a", "1"));
		set.add(ChronoIdentifier.create("myBranch", 100L, "a", "2"));
		set.add(ChronoIdentifier.create("myBranch", 100L, "b", "1"));
		set.add(ChronoIdentifier.create("myBranch", 100L, "b", "2"));
		assertEquals(16, set.size());
		// containment checks
		assertTrue(set.contains(ChronoIdentifier.create("master", 10L, "a", "1")));
		assertTrue(set.contains(ChronoIdentifier.create("master", 10L, "a", "2")));
		assertTrue(set.contains(ChronoIdentifier.create("master", 10L, "b", "1")));
		assertTrue(set.contains(ChronoIdentifier.create("master", 10L, "b", "2")));
		assertTrue(set.contains(ChronoIdentifier.create("master", 100L, "a", "1")));
		assertTrue(set.contains(ChronoIdentifier.create("master", 100L, "a", "2")));
		assertTrue(set.contains(ChronoIdentifier.create("master", 100L, "b", "1")));
		assertTrue(set.contains(ChronoIdentifier.create("master", 100L, "b", "2")));
		assertTrue(set.contains(ChronoIdentifier.create("myBranch", 10L, "a", "1")));
		assertTrue(set.contains(ChronoIdentifier.create("myBranch", 10L, "a", "2")));
		assertTrue(set.contains(ChronoIdentifier.create("myBranch", 10L, "b", "1")));
		assertTrue(set.contains(ChronoIdentifier.create("myBranch", 10L, "b", "2")));
		assertTrue(set.contains(ChronoIdentifier.create("myBranch", 100L, "a", "1")));
		assertTrue(set.contains(ChronoIdentifier.create("myBranch", 100L, "a", "2")));
		assertTrue(set.contains(ChronoIdentifier.create("myBranch", 100L, "b", "1")));
		assertTrue(set.contains(ChronoIdentifier.create("myBranch", 100L, "b", "2")));
	}
}
