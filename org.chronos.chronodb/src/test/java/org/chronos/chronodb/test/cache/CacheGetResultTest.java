package org.chronos.chronodb.test.cache;

import static org.junit.Assert.*;

import org.chronos.chronodb.api.exceptions.CacheGetResultNotPresentException;
import org.chronos.chronodb.internal.api.cache.CacheGetResult;
import org.chronos.chronodb.test.base.ChronoDBUnitTest;
import org.chronos.common.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class CacheGetResultTest extends ChronoDBUnitTest {

	@Test
	public void canCreateAHit() {
		CacheGetResult<String> hit = CacheGetResult.hit("180!!!", 180L);
		assertNotNull(hit);
	}

	@Test
	public void canCreateAMiss() {
		CacheGetResult<Object> miss = CacheGetResult.miss();
		assertNotNull(miss);
	}

	@Test
	public void creatingAMissReturnsASingletonInstance() {
		CacheGetResult<Object> miss = CacheGetResult.miss();
		CacheGetResult<Object> miss2 = CacheGetResult.miss();
		assertNotNull(miss);
		assertTrue(miss == miss2);
	}

	@Test
	public void getValueOnHitReturnsTheCorrectValue() {
		CacheGetResult<String> hit = CacheGetResult.hit("180!!!", 180L);
		assertNotNull(hit);
		assertEquals("180!!!", hit.getValue());
		assertEquals(180L, hit.getValidFrom());
	}

	@Test
	public void getValueOnMissThrowsAnException() {
		CacheGetResult<?> miss = CacheGetResult.miss();
		assertNotNull(miss);
		try {
			miss.getValue();
			fail("getValue() succeeded on a cache miss!");
		} catch (CacheGetResultNotPresentException expected) {
			// pass
		}
	}

	@Test
	public void hashCodeAndEqualsWork() {
		CacheGetResult<String> hit = CacheGetResult.hit("180!!!", 180L);
		CacheGetResult<String> hit2 = CacheGetResult.hit("Bulls Eye!", 50L);
		CacheGetResult<String> hit3 = CacheGetResult.hit("Bulls Eye!", 50L);
		CacheGetResult<String> hit4 = CacheGetResult.hit(null, 0L);
		CacheGetResult<?> miss = CacheGetResult.miss();
		CacheGetResult<?> miss2 = CacheGetResult.miss();
		assertNotEquals(hit, hit2);
		assertEquals(hit2, hit3);
		assertEquals(hit3, hit2);
		assertEquals(hit2.hashCode(), hit3.hashCode());
		assertNotEquals(hit, miss);
		assertNotEquals(miss, hit4);
		assertEquals(miss, miss2);
		assertEquals(miss.hashCode(), miss2.hashCode());
	}

}
