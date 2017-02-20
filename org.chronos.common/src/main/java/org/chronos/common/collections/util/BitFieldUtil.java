package org.chronos.common.collections.util;

public class BitFieldUtil {

	public static int getBits(final int bitfield, final int bitsFrom, final int bitsCount) {
		if (bitsFrom < 0 || bitsCount < 0 || bitsFrom + bitsCount > 32) {
			throw new IllegalArgumentException(
					"Cannot get " + bitsCount + " bits starting from " + bitsFrom + " in a 32-bit integer!");
		}
		// calculate an integer that is equivalent to "bitsCount" ones
		// e.g. for bitsCount = 3, we want '111' in binary.
		// we do this by taking 2 to the power of bitsCount, and subtracting 1 from the result.
		// In order to avoid the rather expensive Math.pow(...), we left-shift a single 1 bitsCount times
		// and subtract 1 from the result, which in the end results in the same binary pattern.
		int mask = (1 << bitsCount) - 1;
		// then, we shift the map left according to our "bitsFrom" value.
		mask = mask << bitsFrom;
		int result = bitfield & mask;
		return result;
	}

	public static int getBitsAndShiftRight(final int value, final int bitsFrom, final int bitsCount) {
		if (bitsFrom < 0 || bitsCount < 0 || bitsFrom + bitsCount > 32) {
			throw new IllegalArgumentException(
					"Cannot get " + bitsCount + " bits starting from " + bitsFrom + " in a 32-bit integer!");
		}
		int bits = getBits(value, bitsFrom, bitsCount);
		return bits >>> bitsFrom;
	}

	public static boolean getNthBit(final int bitfield, final int bitIndex) {
		int mask = 1 << bitIndex;
		int temp = bitfield & mask;
		if (temp != 0) {
			return true;
		} else {
			return false;
		}
	}

	public static int setNthBit(final int bitfield, final int bitIndex) {
		int mask = 1 << bitIndex;
		return bitfield | mask;
	}

	public static int unsetNthBit(final int bitfield, final int bitIndex) {
		// create a "1" at the position we want to unset
		int mask = 1 << bitIndex;
		// invert 0's and 1's in the mask, producing a mask where all bits are 1 except for the given index, which is
		// set to 0
		mask = invert(mask);
		// AND the bitfield and the mask, this will set the bit at the given index to zero
		return bitfield & mask;
	}

	public static int invert(final int bitfield) {
		// -1 is "all ones" in binary, ^ is the XOR operator, so we invert the mask
		return -1 ^ bitfield;
	}

	public static int popCount(final int bitfield) {
		return Integer.bitCount(bitfield);
	}

	public static int popCountUpToBit(final int bitfield, final int maxBitIndex) {
		if (maxBitIndex >= 31) {
			return Integer.bitCount(bitfield);
		}
		if (maxBitIndex < 0) {
			return 0;
		}
		int mask = 1 << maxBitIndex + 1;
		mask -= 1;
		return Integer.bitCount(bitfield & mask);
	}

	public static String toBinary(final int value) {
		return String.format("%32s", Integer.toBinaryString(value)).replace(' ', '0');
	}

	public static int fromBinary(final String binary) {
		// note: we need to use LONG here because Integer.parseInt(...) chokes on negative values
		return (int) Long.parseLong(binary, 2);
	}
}
