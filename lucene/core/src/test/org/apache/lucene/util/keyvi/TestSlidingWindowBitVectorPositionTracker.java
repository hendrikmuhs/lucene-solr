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

import org.apache.lucene.util.LuceneTestCase;

public class TestSlidingWindowBitVectorPositionTracker extends LuceneTestCase {

	public void testNextFreeSlot() {
		SlidingWindowBitVectorPositionTracker positions = new SlidingWindowBitVectorPositionTracker();

		positions.set(8);
		positions.set(9);
		positions.set(10);
		assertEquals(11, positions.nextFreeSlot(8));
		assertEquals(12, positions.nextFreeSlot(12));

	}

	public void testSliding() {
		SlidingWindowBitVectorPositionTracker positions = new SlidingWindowBitVectorPositionTracker();

		positions.set(8);
		positions.set(9);
		positions.set(10);

		// trigger switch of the bit vectors
		positions.set(1050);

		assertTrue(positions.isSet(8));
		assertTrue(positions.isSet(9));
		assertTrue(positions.isSet(10));
		assertTrue(!positions.isSet(1024 + 8));
		assertTrue(!positions.isSet(1024 + 9));
		assertTrue(!positions.isSet(1024 + 10));

		// trigger switch of the bit vectors and cause slide
		positions.set(2100);
		assertFalse(positions.isSet(1024 + 8));
		assertFalse(positions.isSet(1024 + 9));
		assertFalse(positions.isSet(1024 + 10));

		assertFalse(positions.isSet(2048 + 8));
		assertFalse(positions.isSet(2048 + 9));
		assertFalse(positions.isSet(2048 + 10));

		positions.set(4095);
	}

	public void testSetVector() {
		SlidingWindowBitVectorPositionTracker positions = new SlidingWindowBitVectorPositionTracker();

		positions.set(8);
		positions.set(9);
		positions.set(10);

		BitVector a = new BitVector(32);
		a.set(1);
		a.set(3);
		a.set(25);
		a.set(29);
		a.set(31);

		positions.setVector(a, 2);
		assertTrue(positions.isSet(3));
		assertTrue(positions.isSet(5));
		assertTrue(positions.isSet(27));
	}

	public void testSetVectorOverlap() {
		SlidingWindowBitVectorPositionTracker positions = new SlidingWindowBitVectorPositionTracker();

		positions.set(8);
		positions.set(9);
		positions.set(10);

		BitVector a = new BitVector(32);
		a.set(1);
		a.set(3);
		a.set(25);
		a.set(29);
		a.set(31);

		positions.setVector(a, 1020);
		assertTrue(positions.isSet(1021));
		assertTrue(positions.isSet(1023));
		assertTrue(positions.isSet(1045));
		assertTrue(positions.isSet(1049));
		assertTrue(positions.isSet(1051));
	}

	public void testSetVectorOverlap2() {
		SlidingWindowBitVectorPositionTracker positions = new SlidingWindowBitVectorPositionTracker();

		positions.set(8);
		positions.set(9);
		positions.set(10);

		BitVector a = new BitVector(260);
		a.set(119);
		a.set(151);
		positions.setVector(a, 929);
		assertTrue(positions.isSet(929 + 119));

		assertTrue(positions.isSet(929 + 151));

		positions.setVector(a, 940);
		assertTrue(positions.isSet(940 + 119));

		BitVector b = new BitVector(260);
		b.set(100);
		positions.setVector(b, 924);
		assertTrue(positions.isSet(924 + 100));
	}

	public void testSetVectorOverlap3() {
		SlidingWindowBitVectorPositionTracker positions = new SlidingWindowBitVectorPositionTracker();

		positions.set(8);
		positions.set(9);
		positions.set(10);

		BitVector a = new BitVector(260);
		a.set(256);
		a.set(257);
		positions.setVector(a, 5311);
		assertTrue(positions.isSet(5311 + 256));

		assertTrue(positions.isSet(5311 + 257));

		positions.set(5311 + 257);

		assertTrue(positions.isSet(5311 + 257));
	}

}
