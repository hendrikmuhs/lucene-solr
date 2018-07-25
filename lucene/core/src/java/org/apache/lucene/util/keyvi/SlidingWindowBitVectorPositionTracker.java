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

public class SlidingWindowBitVectorPositionTracker {

	private static int SLIDING_WINDOW_SIZE = 2048;
	private static int SLIDING_WINDOW_MASK = 2047;
	private static int SLIDING_WINDOW_SHIFT = 11;

	private BitVector currentVector = new BitVector(SLIDING_WINDOW_SIZE);
	private BitVector previousVector = new BitVector(SLIDING_WINDOW_SIZE);
	private long windowStartPosition = 0;

	public SlidingWindowBitVectorPositionTracker() {

	}

	public boolean isSet(long position) {
		// divide by SLIDING_WINDOW_SIZE
		long blockerWindow = position >> SLIDING_WINDOW_SHIFT;

		int blockerOffset = (int) position & SLIDING_WINDOW_MASK;

		if (blockerWindow == windowStartPosition) {
			return currentVector.get(blockerOffset);
		}

		if (blockerWindow > windowStartPosition) {
			return false;
		}

		return previousVector.get(blockerOffset);
	}

	public long nextFreeSlot(long position) {
		// divide by SLIDING_WINDOW_SIZE
		long blockerWindow = position >> SLIDING_WINDOW_SHIFT;

		int blockerOffset = (int) position & SLIDING_WINDOW_MASK;

		if (blockerWindow > windowStartPosition) {
			return position;
		}

		if (blockerWindow < windowStartPosition) {
			long offset = previousVector.getNextNonSetBit(blockerOffset);

			if (offset < SLIDING_WINDOW_SIZE) {
				return offset + (blockerWindow << SLIDING_WINDOW_SHIFT);
			}

			// else: check currentVector
			++blockerWindow;
			blockerOffset = 0;
		}

		return currentVector.getNextNonSetBit(blockerOffset) + (blockerWindow << SLIDING_WINDOW_SHIFT);
	}

	public void set(long position) {
		// divide by SLIDING_WINDOW_SIZE
		long blockerWindow = position >> SLIDING_WINDOW_SHIFT;

		int blockerOffset = (int) position & SLIDING_WINDOW_MASK;

		if (blockerWindow > windowStartPosition) {
			// swap and reset
			BitVector tmp = previousVector;
			previousVector = currentVector;
			currentVector = tmp;
			currentVector.clear();
			windowStartPosition = blockerWindow;
		}

		if (blockerWindow == windowStartPosition) {
			currentVector.set(blockerOffset);
		} else if (windowStartPosition > 0 && blockerWindow == windowStartPosition - 1) {
			previousVector.set(blockerOffset);
		}
	}

	public void setVector(BitVector other, long position) {
		// divide by SLIDING_WINDOW_SIZE
		long blockerWindow = position >> SLIDING_WINDOW_SHIFT;
		long blockerWindowEnd = (position + other.size()) >> SLIDING_WINDOW_SHIFT;
		int blockerOffset = (int) position & SLIDING_WINDOW_MASK;

		// check if start position is already over the boundary now
		if (blockerWindowEnd > windowStartPosition) {
			// swap and reset
			BitVector tmp = previousVector;
			previousVector = currentVector;
			currentVector = tmp;
			currentVector.clear();
			windowStartPosition = blockerWindow;
		}

		if (blockerWindow == windowStartPosition) {
			currentVector.setVector(other, blockerOffset);
		} else if (windowStartPosition > 0 && blockerWindow == windowStartPosition - 1) {
			previousVector.setVector(other, blockerOffset);
			if (blockerWindowEnd == windowStartPosition) {
				currentVector.setVectorAndShiftOther(other, SLIDING_WINDOW_SIZE - blockerOffset);
			}
		}

	}

	public int isAvailable(BitVector requestedPositions, long position) {
		// divide by SLIDING_WINDOW_SIZE
		long blockerWindow = position >> SLIDING_WINDOW_SHIFT;

		int blockerOffset = (int) position & SLIDING_WINDOW_MASK;

		if (blockerWindow == windowStartPosition) {
			return currentVector.DisjointAndShiftThis(requestedPositions, blockerOffset);
		}

		if (blockerWindow > windowStartPosition) {
			return 0;
		}

		int shift = previousVector.DisjointAndShiftThis(requestedPositions, blockerOffset);

		if (shift == 0 && (SLIDING_WINDOW_SIZE - blockerOffset < KeyviConstants.MAX_TRANSITIONS_OF_A_STATE)) {
			return requestedPositions.DisjointAndShiftOther(currentVector, SLIDING_WINDOW_SIZE - blockerOffset);
		}

		return shift;
	}

}
