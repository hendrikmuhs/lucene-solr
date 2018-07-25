/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.util.keyvi;

import java.util.Arrays;

/*
 * A bitvector implementation with a fixed length and special methods for finding free slots.
 * 
 * (unfortunately java.util.BitSet lacks functionality we need here)
 */
public class BitVector {

  private final long[] bits;
  private final int size;

  private static long MAX_UNSIGNED = 0xFFFFFFFFFFFFFFFFL;

  public BitVector(int size) {
    this.size = size;
    bits = new long[(size >> 6) + 1];
  }

  public void set(int bit) {
    bits[bit >> 6] |= 1l << (bit & 63);
  }

  /**
   * Sets all bits of the given bitvector
   * 
   * @param other
   *          a bitvector
   * @param startBit
   *          the start position to set the vector
   */
  public void setVector(BitVector other, int startBit) {
    int bytePosition = startBit >> 6;
    int bitPosition = startBit & 63;

    int writeLength = Math.min(bits.length - bytePosition, other.bits.length);

    if (bitPosition == 0) {
      for (int i = 0; i < writeLength; ++i) {
        bits[bytePosition + i] |= other.bits[i];
      }
    } else {
      bits[bytePosition] |= (other.bits[0] << bitPosition);
      for (int i = 1; i < writeLength; ++i) {
        bits[bytePosition + i] |= ((other.bits[i] << bitPosition) | (other.bits[i - 1] >>> (64 - bitPosition)));
      }

      if (bytePosition + writeLength < bits.length) {
        bits[bytePosition + writeLength] |= (other.bits[writeLength - 1] >>> (64 - bitPosition));
      }
    }
  }

  /**
   * Sets all bits of the given bitvector
   * 
   * @param other
   *          a BitVector
   * @param startBitOther
   *          the start position in the given bitvector to use
   */
  public void setVectorAndShiftOther(BitVector other, int startBitOther) {
    int bytePositionOther = startBitOther >> 6;
    int bitPositionOther = startBitOther & 63;

    int writeLength = Math.min(bits.length, other.bits.length) - bytePositionOther;

    for (int i = 0; i < writeLength; ++i) {
      bits[i] |= other.getUnderlyingIntegerAtPosition(bytePositionOther + i, bitPositionOther);
    }
  }

  /**
   * Clear a bit, set it to 0.
   * 
   * @param bit
   *          the bit to erase.
   */
  public void Clear(int bit) {
    bits[bit >> 6] &= (~(1l << (bit & 63)));
  }

  /**
   * Gets the state of the given bit
   *
   * @param bit
   *          the bit
   * @return True if set, false otherwise.
   */
  public boolean get(int bit) {
    return (bits[bit >> 6] & (1l << (bit & 63))) != 0;
  }

  /**
   * Get the next non set bit in the bitvector starting from the given position.
   * 
   * @param startBit
   *          the bit to start searching from
   * @return the next unset bit.
   */
  public int getNextNonSetBit(int startBit) {
    int bytePosition = startBit >> 6;
    int bitPosition = startBit & 63;

    long a = getUnderlyingIntegerAtPosition(bytePosition, bitPosition);

    while (a == MAX_UNSIGNED) {
      ++bytePosition;
      startBit += 64;
      a = getUnderlyingIntegerAtPosition(bytePosition, bitPosition);
    }

    return position(~a) + startBit;
  }

  /**
   * Checks whether this bitvector at the given start position and the given bitvector are disjoint.
   * 
   * @param other
   *          a bitvector to compare with
   * @param startBit
   *          the start position of this BitVector
   * @return true if the sets are disjoint.
   */
  public boolean Disjoint(BitVector other, int startBit) {
    int bytePosition = startBit >> 6;
    int lenthToCheck = Math.min(other.bits.length, bits.length - bytePosition);
    int bitPosition = startBit & 63;

    for (int i = 0; i < lenthToCheck; ++i) {
      long b = other.bits[i];
      if (b != 0) {
        long a = getUnderlyingIntegerAtPosition(bytePosition, bitPosition);

        if ((a & b) != 0) {
          // shift until it fits
          return false;
        }
      }

      ++bytePosition;
    }

    return true;
  }

  /**
   * Checks whether this bitvector at the given start position and the given bitvector are disjoint and otherwise
   * returns the minimum number of bits the "other" has to be shifted.
   * 
   * @param other
   *          a bitvector to compare with
   * @param startBit
   *          the start positions
   * @return 0 if the sets are disjoint, otherwise the minimum number of shift operations.
   * @remarks This method is a performance critical.
   */
  public int DisjointAndShiftOther(BitVector other, int startBit) {
    int bytePosition = startBit >> 6;
    int lengthToCheck = Math.min(other.bits.length, bits.length - bytePosition);
    int bitPosition = startBit & 63;

    for (int i = 0; i < lengthToCheck; ++i) {
      long b = other.bits[i];
      if (b != 0) {
        long a = getUnderlyingIntegerAtPosition(bytePosition, bitPosition);

        if ((a & b) != 0) {
          // shift until it fits
          return getMinimumNumberOfShifts(a, b);
        }
      }

      ++bytePosition;
    }

    return 0;
  }

  /**
   * Checks whether this bit vector at the given start position and the given bitvector are disjoint and returns the
   * minimum number of bits to shift until it could fit.
   * 
   * @param other
   *          the bit vector to compare with
   * @param startBit
   *          the starting position in this bit vector
   * @return 0 if the sets are disjoint, otherwise the minimum number of shift operations.
   * @remark This method is a performance hotspot.
   */
  public int DisjointAndShiftThis(BitVector other, int startBit) {
    int bytePosition = startBit >> 6;
    int lengthToCheck = Math.min(other.bits.length, bits.length - bytePosition);
    int bit_position = startBit & 63;

    for (int i = 0; i < lengthToCheck; ++i) {
      long b = other.bits[i];
      if (b != 0) {
        long a = getUnderlyingIntegerAtPosition(bytePosition, bit_position);

        if ((a & b) != 0) {
          // shift until it fits
          return getMinimumNumberOfShifts(b, a);
        }
      }

      ++bytePosition;
    }

    return 0;
  }

  /***
   * Clear the bit vector (reset all to 0)
   */
  void clear() {
    Arrays.fill(bits, 0);
  }

  /***
   * Get the size of the bit vector
   */
  public int size() {
    return size;
  }

  // todo: inline?
  private int position(long number) {
    return Long.numberOfTrailingZeros(number);
  }

  private int getMinimumNumberOfShifts(long b, long a) {
    int shifts = 1;
    a = a >>> 1;
    while ((a & b) != 0) {
      a = a >>> 1;
      ++shifts;
    }

    return shifts;
  }

  protected long getUnderlyingIntegerAtPosition(int byte_position, int bit_position) {
    if (bit_position == 0) {
      return bits[byte_position];
    }

    if (byte_position + 1 < bits.length) {
      return (bits[byte_position] >>> bit_position) | (bits[byte_position + 1] << (64 - bit_position));
    }

    return bits[byte_position] >>> bit_position;
  }
}
