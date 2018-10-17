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

import org.apache.lucene.util.keyvi.UnpackedState.Transition;

public class SparseArrayBuilder {

  private SparseArrayPersistence persistence;
  private long numerOfStates = 0;
  long highestPersistedState = 0;
  private boolean innerWeight;
  private boolean minimize;
  private PackedState packedState;
  private LeastRecentlyUsedGenerationsCache<PackedState> stateHashTable;
  SlidingWindowBitVectorPositionTracker stateStartPositions;
  SlidingWindowBitVectorPositionTracker takenPositionsInSparsearray;
  private SlidingWindowBitVectorPositionTracker zeroByteScramblingStateStartPositions;

  public SparseArrayBuilder(long memoryLimit, SparseArrayPersistence persistence, boolean innerWeight,
      boolean minimize) {
    this.persistence = persistence;
    this.innerWeight = innerWeight;
    this.minimize = minimize;
    this.packedState = new PackedState();
    this.stateHashTable = new LeastRecentlyUsedGenerationsCache<PackedState>(packedState, memoryLimit);
    stateStartPositions = new SlidingWindowBitVectorPositionTracker();
    takenPositionsInSparsearray = new SlidingWindowBitVectorPositionTracker();
    zeroByteScramblingStateStartPositions = new SlidingWindowBitVectorPositionTracker();
  }

  public long persistState(UnpackedState unpackedState) {

    PackedState existingState = new PackedState();
    PackedState.Key existingKey = existingState.new Key();

    if (unpackedState.getNoMinimizationCounter() == 0) {
      // try to find a match of two equal states to minimize automata
      if (stateHashTable.get(unpackedState, existingKey)) {
        // if we are hitting this line minimization succeeded
        int offset = existingKey.getOffset();
        if (unpackedState.getWeight() > 0) {
          updateWeightIfNeeded(offset, unpackedState.getWeight());
        }

        return offset;
      }
    }
    // minimization failed, all predecessors of this state will not be minimized, so
    // stop trying
    unpackedState.incrementNoMinimizationCounter();
    long offset = findFreeBucket(unpackedState);
    // TRACE("write at %d", offset);

    writeState((int) offset, unpackedState);
    ++numerOfStates;

    PackedState.Key key = existingState.new Key((int) offset, unpackedState.hashCode(), unpackedState.size());

    // if minimization failed several time in a row while the minimization hash has
    // decent amount of data,
    // do not push the state to the minimization hash to avoid unnecessary overhead
    if (minimize && (numerOfStates < 1000000 || unpackedState.getNoMinimizationCounter() < 8)) {
      stateHashTable.add(key);
    }

    return offset;
  }

  public int size() {
    return 0;
  }

  public long getNumberOfStates() {
    return numerOfStates;
  }

  long findFreeBucket(UnpackedState unpackedState) {
    long startPosition = highestPersistedState > KeyviConstants.SPARSE_ARRAY_SEARCH_OFFSET
        ? highestPersistedState - KeyviConstants.SPARSE_ARRAY_SEARCH_OFFSET
        : 1;

    // further shift it taking the first outgoing transition and find the slot where
    // it fits in
    startPosition = takenPositionsInSparsearray.nextFreeSlot(startPosition + unpackedState.get(0).getLabel())
        - unpackedState.get(0).getLabel();

    do {
      startPosition = stateStartPositions.nextFreeSlot(startPosition);

      if (zeroByteScramblingStateStartPositions.isSet(startPosition)) {
        // clash with zerobyte start position, skip it
        ++startPosition;
        continue;
      }

      if (unpackedState.isFinal()) {
        if (stateStartPositions.isSet(startPosition + KeyviConstants.NUMBER_OF_STATE_CODINGS)) {
          ++startPosition;

          // clash with start positions, jump to next position
          continue;
        }
      }

      int shift = takenPositionsInSparsearray.isAvailable(unpackedState.getBitVector(), startPosition);

      if (shift == 0) {
        // check for potential conflict with existing state which could become final if
        // the current state has
        // a outgoing transition with label 1
        if (startPosition > KeyviConstants.NUMBER_OF_STATE_CODINGS
            && unpackedState.hasLabel(KeyviConstants.FINAL_OFFSET_CODE)
            && stateStartPositions.isSet(startPosition - KeyviConstants.NUMBER_OF_STATE_CODINGS)) {
          ++startPosition;
          // interference with other state, continue search

          continue;
        }

        if (unpackedState.get(0).getLabel() != 0 && !takenPositionsInSparsearray.isSet(startPosition)) {
          // need special handling for zero-byte state, position

          // state has no 0-byte, we have to 'scramble' the 0-byte to avoid a ghost state
          if (startPosition >= KeyviConstants.NUMBER_OF_STATE_CODINGS) {
            int zerobyteScramblingState = (int) stateStartPositions
                .nextFreeSlot(startPosition - KeyviConstants.NUMBER_OF_STATE_CODINGS);

            if (zerobyteScramblingState >= startPosition) {
              // unable to scramble zero byte position
              ++startPosition;
              continue;
            }

            byte zerobyteScramblingLabel = (byte) (startPosition - zerobyteScramblingState);
            // avoid finalizing a state by mistake
            if (zerobyteScramblingLabel == KeyviConstants.FINAL_OFFSET_CODE
                && stateStartPositions.isSet(startPosition - KeyviConstants.NUMBER_OF_STATE_CODINGS)) {
              // unable to scramble zero byte position (state finalization), continue search
              ++startPosition;
              continue;
            }

            // found zero byte label
            unpackedState.setZeroByteState(zerobyteScramblingState);
            unpackedState.setZeroByteLabel(zerobyteScramblingLabel);
          }
        }

        // found slot at
        return startPosition;
      }

      // state does not fit in, shifting start position

      startPosition += shift;
    } while (true);

  }

  void writeState(int offset, UnpackedState unpackedState) {
    int i;
    int len = unpackedState.size();
    int weight = unpackedState.getWeight();

    if (offset > highestPersistedState) {
      highestPersistedState = offset;
    }

    persistence.beginNewState(offset);

    if (unpackedState.get(0).getLabel() != 0) {
      // make sure no other state is placed at offset - 256, which could cause
      // interference
      if (unpackedState.get(0).getLabel() == 1 && offset > KeyviConstants.NUMBER_OF_STATE_CODINGS) {
        stateStartPositions.set(offset - KeyviConstants.NUMBER_OF_STATE_CODINGS);
      }

      // no zero byte, need special handling
      // check if something is already written there
      if (takenPositionsInSparsearray.isSet(offset) == false) {
        if (offset >= KeyviConstants.NUMBER_OF_STATE_CODINGS) {
          // block the position as a possible start state
          zeroByteScramblingStateStartPositions.set(unpackedState.getZeroByteState());
        }

        // write the zerobyte label (it can get overridden later, which is ok)
        writeTransition(offset, unpackedState.getZeroByteLabel(), 0);
      }
    } else {
      // first bit is a 0 byte, so check [1]
      // make sure no other state is placed at offset - 256, which could cause
      // interference
      if (unpackedState.size() > 1 && (unpackedState.get(1).getLabel() == 1)
          && offset >= KeyviConstants.NUMBER_OF_STATE_CODINGS) {
        stateStartPositions.set(offset - KeyviConstants.NUMBER_OF_STATE_CODINGS);
      }

      // zero byte to be written
    }

    // 1st pass: reserve the buckets in the sparse array
    takenPositionsInSparsearray.setVector(unpackedState.getBitVector(), offset);

    if (unpackedState.isFinal()) {
      // Make sure no other state is placed at offset + 255, which could cause
      // interference
      stateStartPositions.set(offset + KeyviConstants.NUMBER_OF_STATE_CODINGS);
    }

    // no other state should start at this offset
    stateStartPositions.set(offset);

    // 2nd pass: write the actual values into the buckets
    for (i = 0; i < len; ++i) {
      // typename UnpackedState<SparseArrayPersistence<uint16_t>>::Transition e =
      // unpacked_state[i];
      Transition e = unpackedState.get(i);

      if (e.getLabel() < KeyviConstants.FINAL_OFFSET_TRANSITION) {
        writeTransition(offset + e.getLabel(), (byte) e.getLabel(), e.getValue());
      } else {
        if (e.getLabel() == KeyviConstants.FINAL_OFFSET_TRANSITION) {
          writeFinalTransition(offset, e.getValue());
        }
      }
    }

    if (weight > 0) {
      // TRACE("Write inner weight at %d, value %d", offset, weight);
      // as all states have this, no need to code it specially
      updateWeightIfNeeded(offset, weight);
    }

  }

  void updateWeightIfNeeded(int offset, int weight) {

    short newWeight = (short) (weight < KeyviConstants.COMPACT_SIZE_INNER_WEIGHT_MAX_VALUE ? weight
        : KeyviConstants.COMPACT_SIZE_INNER_WEIGHT_MAX_VALUE);

    if (persistence.readTransitionValue(offset + KeyviConstants.INNER_WEIGHT_TRANSITION_COMPACT) < newWeight) {

      persistence.writeTransition(offset + KeyviConstants.INNER_WEIGHT_TRANSITION_COMPACT, (byte) 0, newWeight);
      // it might be, that the slot is not taken yet
      takenPositionsInSparsearray.set(offset + KeyviConstants.INNER_WEIGHT_TRANSITION_COMPACT);

      // block this bucket for the start of a new state
      stateStartPositions.set(offset + KeyviConstants.INNER_WEIGHT_TRANSITION_COMPACT);
    }
  }

  /**
   * Compact Encode for ushorts
   *
   * bit 1 0: value fits into bits 1-16 (value <32768) 1: overflow encoding 2 0: overflow encoding 1: absolute value
   * fits in bits 2-16 (value<16384)
   *
   * compact (0x): value is the difference of offset + 1024 - transitionPointer
   *
   * absolute compact (11): value is the absolute address coded in bits 2-16
   *
   * overflow: (10)
   *
   * bits 3-12 pointer to extra bucket in the range -512 -> +511 from transitionPointer bit 13 whether pointer is
   * absolute(0) or relative(1) bits 14-16 lower part (3 bits) of absolute value coded in extra bucket extra bucket:
   * variable length encoded absolute address of transition Pointer, higher bits
   *
   */
  void writeTransition(int offset, byte transitionId, int transitionPointer) {

    int difference = Integer.MAX_VALUE;
    if (offset + KeyviConstants.COMPACT_SIZE_WINDOW > transitionPointer) {
      difference = offset + KeyviConstants.COMPACT_SIZE_WINDOW - transitionPointer;
    }

    if (difference < KeyviConstants.COMPACT_SIZE_RELATIVE_MAX_VALUE) {
      short diffAsShort = (short) difference;
      // transition fits in uint16

      persistence.writeTransition(offset, transitionId, diffAsShort);
      return;
    }

    if (transitionPointer < KeyviConstants.COMPACT_SIZE_ABSOLUTE_MAX_VALUE) {
      // Transition fits in uint16 absolute
      short absoluteCompactCoding = (short) ((short) (transitionPointer) | 0xc000);
      persistence.writeTransition(offset, transitionId, absoluteCompactCoding);
      return;
    }
    // transition requires overflow
    // pointer to overflow bucket with variable length encoding
    // set first bit to indicate overflow
    short pointerToOverflowBucket = (short) 0x8000;
    int overflowCode = transitionPointer;

    if (difference < transitionPointer) {
      // do relative coding
      // set corresponding bit
      pointerToOverflowBucket |= 0x8;
      overflowCode = difference;
    }

    int transitionPointerLow = overflowCode & 0x7; // get the lower part
    int transitionPointerHigh = overflowCode >> 3; // the higher part

    // else overflow encoding
    short[] vshortPointer = new short[8];
    int vshortSize = VInt.encodeVarShort(transitionPointerHigh, vshortPointer);

    // find free spots in the sparse array where the pointer fits in
    int startPosition = offset > KeyviConstants.COMPACT_SIZE_WINDOW ? offset - KeyviConstants.COMPACT_SIZE_WINDOW
        : 0;
    int zerobyteScramblingState = 0;
    byte zerobyteScramblingLabel = (byte) 0xff;

    for (;;) {
      startPosition = (int) takenPositionsInSparsearray.nextFreeSlot(startPosition);

      // prevent that states without a weight get a 'zombie weight'.
      // check that we do not write into a bucket that is used for an inner weight of
      // another transition
      if (innerWeight
          && stateStartPositions.isSet(startPosition + KeyviConstants.INNER_WEIGHT_TRANSITION_COMPACT)) {

        startPosition += 1;
        continue;
      }

      if (takenPositionsInSparsearray.isSet(startPosition)) {
        // startPosition is already taken
        startPosition += 1;
        continue;
      }

      int foundSlots = 1;

      for (; foundSlots < vshortSize; foundSlots++) {
        if (takenPositionsInSparsearray.isSet(startPosition + foundSlots)) {
          startPosition += foundSlots + 1;
          foundSlots = 0;
          break;
        }
        // check that we do not write into a bucket that is used for an inner weight of
        // another transition
        if (innerWeight && stateStartPositions
            .isSet(startPosition + foundSlots - KeyviConstants.INNER_WEIGHT_TRANSITION_COMPACT)) {
          // found clash wrt. weight transition, skipping

          startPosition += foundSlots + 1;
          foundSlots = 0;
          break;
        }
      }

      if (foundSlots > 0 && startPosition >= KeyviConstants.NUMBER_OF_STATE_CODINGS) {
        // ensure enough space: if vshort has length 2, label must start from 0xfe
        zerobyteScramblingState = (int) stateStartPositions
            .nextFreeSlot(startPosition + vshortSize - KeyviConstants.NUMBER_OF_STATE_CODINGS - 1);

        if (zerobyteScramblingState >= startPosition) {
          // did not find a state to scramble zero-bytes, no good start position, skipping

          // we can probable advance more if this happens
          startPosition += foundSlots + 1;
          foundSlots = 0;
        } else {
          zerobyteScramblingLabel = (byte) (startPosition - zerobyteScramblingState);

          if (zerobyteScramblingLabel == KeyviConstants.FINAL_OFFSET_CODE) {
            // did not find a state to scramble zero-bytes, skipping

            // we can probable advance more if this happens
            startPosition += foundSlots + 1;
            foundSlots = 0;
          }
        }
      }

      if (foundSlots == vshortSize) {
        break;
      }
    }

    // write overflow transition

    // block the pseudo state used for zerobyte scrambling
    // state_start_positions_.Set(zerobyte_scrambling_state);
    zeroByteScramblingStateStartPositions.set(zerobyteScramblingState);
    // write the overflow pointer using scrambled zerobyte labels
    for (int i = 0; i < vshortSize; ++i) {
      takenPositionsInSparsearray.set(startPosition + i);
      persistence.writeTransition(startPosition + i, (byte) (zerobyteScramblingLabel + i), vshortPointer[i]);
    }

    // encode the pointer to that bucket
    int overflowBucket = (KeyviConstants.COMPACT_SIZE_WINDOW + startPosition) - offset;
    pointerToOverflowBucket |= overflowBucket << 4;

    // add the lower part (4 bits)
    pointerToOverflowBucket += transitionPointerLow;

    persistence.writeTransition(offset, transitionId, pointerToOverflowBucket);
  }

  void writeFinalTransition(int offset, long value) {

    // todo: do not re-create every time
    short[] shortBuffer = new short[8];

    int varShortSize = VInt.encodeVarShort(value, shortBuffer);

    for (int i = 0; i < varShortSize; ++i) {
      persistence.writeTransition(offset + KeyviConstants.FINAL_OFFSET_TRANSITION + i,
          (byte) (KeyviConstants.FINAL_OFFSET_CODE + i), shortBuffer[i]);
    }
  }
}
