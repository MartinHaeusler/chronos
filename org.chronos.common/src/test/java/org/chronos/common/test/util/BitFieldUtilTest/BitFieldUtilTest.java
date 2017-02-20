package org.chronos.common.test.util.BitFieldUtilTest;

import static org.junit.Assert.*;

import org.chronos.common.collections.util.BitFieldUtil;
import org.junit.Test;

public class BitFieldUtilTest {

	@Test
	public void getBitsWorks() {
		// mask: ..0000000111110000 (5 successive bits, skipping the first 4)
		// value: .0000010011010010 (1234 in binary)
		// result: 0000000011010000 (208 in binary)
		assertEquals(208, BitFieldUtil.getBits(1234, 4, 5));

		// mask: ..0000000000111111 (6 successive bits, skipping the first 0)
		// value: .0000010011010010 (1234 in binary)
		// result: 0000000000010010 (18 in binary)
		assertEquals(18, BitFieldUtil.getBits(1234, 0, 6));

		// mask: ..00000000000111110000000000000000 (5 successive bits, skipping first 16)
		// value: .11111111111111111111111111111111 (2 ^ 32 - 1)
		// result: 00000000000111110000000000000000 (2031616 in binary)
		assertEquals(2031616, BitFieldUtil.getBits(Integer.MAX_VALUE, 16, 5));
	}

	@Test
	public void getBitsAndShiftRightWorks() {
		// mask: ..0000000111110000 (5 successive bits, skipping the first 4)
		// value: .0000010011010010 (1234 in binary)
		// result: 0000000000001101 (13 in binary)
		assertEquals(13, BitFieldUtil.getBitsAndShiftRight(1234, 4, 5));
	}

	@Test
	public void getNthBitWorks() {
		assertEquals(false, BitFieldUtil.getNthBit(0, 0));
		assertEquals(true, BitFieldUtil.getNthBit(1, 0));
		assertEquals(true, BitFieldUtil.getNthBit(2, 1));
		assertEquals(false, BitFieldUtil.getNthBit(2, 0));
	}

	@Test
	public void popCountUpToBitIndexWorks() {
		// 59 = 111011
		// up to and including bit index 3: 1011
		// population count of 1011 = 3
		assertEquals(3, BitFieldUtil.popCountUpToBit(59, 3));
	}

	@Test
	public void binaryConversionWorks() {
		// some generic tests
		for (int i = 0; i < 100; i++) {
			assertEquals(i, BitFieldUtil.fromBinary(BitFieldUtil.toBinary(i)));
		}
		// specific tests
		int number = 0;
		assertEquals(number, BitFieldUtil.fromBinary(BitFieldUtil.toBinary(number)));
		number = -1;
		assertEquals(number, BitFieldUtil.fromBinary(BitFieldUtil.toBinary(number)));
		number = Integer.MAX_VALUE;
		assertEquals(number, BitFieldUtil.fromBinary(BitFieldUtil.toBinary(number)));
		number = Integer.MIN_VALUE + 1;
		assertEquals(number, BitFieldUtil.fromBinary(BitFieldUtil.toBinary(number)));
	}

	@Test
	public void settingNthBitWorks() {
		// test with base 0
		int bitfield = 0;
		String binary = null;
		binary = BitFieldUtil.toBinary(BitFieldUtil.setNthBit(bitfield, 0));
		assertEquals("00000000000000000000000000000001", binary);
		binary = BitFieldUtil.toBinary(BitFieldUtil.setNthBit(bitfield, 3));
		assertEquals("00000000000000000000000000001000", binary);
		binary = BitFieldUtil.toBinary(BitFieldUtil.setNthBit(bitfield, 18));
		assertEquals("00000000000001000000000000000000", binary);
		binary = BitFieldUtil.toBinary(BitFieldUtil.setNthBit(bitfield, 31));
		assertEquals("10000000000000000000000000000000", binary);
		// test with another base
		// binary: 0101 0101 0101 0101 0101 0101 0101 0101
		bitfield = 1_431_655_765;
		assertEquals("01010101010101010101010101010101", BitFieldUtil.toBinary(bitfield));
		binary = BitFieldUtil.toBinary(BitFieldUtil.setNthBit(bitfield, 1));
		assertEquals("01010101010101010101010101010111", binary);
		binary = BitFieldUtil.toBinary(BitFieldUtil.setNthBit(bitfield, 29));
		assertEquals("01110101010101010101010101010101", binary);
		// setting bits that are already set should do nothing
		binary = BitFieldUtil.toBinary(BitFieldUtil.setNthBit(bitfield, 0));
		assertEquals("01010101010101010101010101010101", binary);
		binary = BitFieldUtil.toBinary(BitFieldUtil.setNthBit(bitfield, 2));
		assertEquals("01010101010101010101010101010101", binary);
		binary = BitFieldUtil.toBinary(BitFieldUtil.setNthBit(bitfield, 30));
		assertEquals("01010101010101010101010101010101", binary);
	}

	@Test
	public void unsettingNthBitWorks() {
		// test with base 1111111111111111111111111111111
		int bitfield = -1;
		assertEquals("11111111111111111111111111111111", BitFieldUtil.toBinary(bitfield));
		String binary = null;
		binary = BitFieldUtil.toBinary(BitFieldUtil.unsetNthBit(bitfield, 0));
		assertEquals("11111111111111111111111111111110", binary);
		binary = BitFieldUtil.toBinary(BitFieldUtil.unsetNthBit(bitfield, 3));
		assertEquals("11111111111111111111111111110111", binary);
		binary = BitFieldUtil.toBinary(BitFieldUtil.unsetNthBit(bitfield, 18));
		assertEquals("11111111111110111111111111111111", binary);
		binary = BitFieldUtil.toBinary(BitFieldUtil.unsetNthBit(bitfield, 31));
		assertEquals("01111111111111111111111111111111", binary);
		// test with another base
		// binary: 0101 0101 0101 0101 0101 0101 0101 0101
		bitfield = 1_431_655_765;
		assertEquals("01010101010101010101010101010101", BitFieldUtil.toBinary(bitfield));
		binary = BitFieldUtil.toBinary(BitFieldUtil.unsetNthBit(bitfield, 0));
		assertEquals("01010101010101010101010101010100", binary);
		binary = BitFieldUtil.toBinary(BitFieldUtil.unsetNthBit(bitfield, 30));
		assertEquals("00010101010101010101010101010101", binary);
		// unsetting bits that are already unset should do nothing
		binary = BitFieldUtil.toBinary(BitFieldUtil.unsetNthBit(bitfield, 1));
		assertEquals("01010101010101010101010101010101", binary);
		binary = BitFieldUtil.toBinary(BitFieldUtil.unsetNthBit(bitfield, 3));
		assertEquals("01010101010101010101010101010101", binary);
		binary = BitFieldUtil.toBinary(BitFieldUtil.unsetNthBit(bitfield, 31));
		assertEquals("01010101010101010101010101010101", binary);
	}

}
