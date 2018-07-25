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

public class TestBitVector extends LuceneTestCase {

	public void testSize() {
		BitVector a = new BitVector(1);
		assertEquals(1, a.size());
	}

	public void testClear() {
		BitVector a = new BitVector(100);

		a.set(42);
		assertFalse(a.get(41));
		assertTrue(a.get(42));
		assertFalse(a.get(43));

		a.clear();
		assertFalse(a.get(42));
	}

	public void testBitShifting() {
		BitVector origin = new BitVector(16);

		// 0000 0010 1010 0100
		origin.set(6);
		origin.set(8);
		origin.set(13);
		origin.set(10);

		assertEquals(9536, origin.getUnderlyingIntegerAtPosition(0, 0));
		assertEquals(298, origin.getUnderlyingIntegerAtPosition(0, 5));
		assertEquals(149, origin.getUnderlyingIntegerAtPosition(0, 6));
		assertEquals(9, origin.getUnderlyingIntegerAtPosition(0, 10));
		assertEquals(0, origin.getUnderlyingIntegerAtPosition(0, 15));

		// 0101 0100
		BitVector a = new BitVector(16);
		a.set(6);
		a.set(8);
		a.set(13);
		a.set(10);
		assertEquals(a.getUnderlyingIntegerAtPosition(0, 0), origin.getUnderlyingIntegerAtPosition(0, 0));

		// 0101 0100 1000 0000
		BitVector b = new BitVector(32);
		b.set(1);
		b.set(3);
		b.set(5);
		b.set(8);
		assertEquals(b.getUnderlyingIntegerAtPosition(0, 0), origin.getUnderlyingIntegerAtPosition(0, 5));

		// 1010 1000
		BitVector c = new BitVector(32);
		c.set(0);
		c.set(2);
		c.set(4);
		c.set(7);
		assertEquals(c.getUnderlyingIntegerAtPosition(0, 0), origin.getUnderlyingIntegerAtPosition(0, 6));

		// 1001 0000
		BitVector d = new BitVector(32);
		d.set(0);
		d.set(3);
		assertEquals(d.getUnderlyingIntegerAtPosition(0, 0), origin.getUnderlyingIntegerAtPosition(0, 10));

		// 0000 0000
		BitVector e = new BitVector(15);
		assertEquals(e.getUnderlyingIntegerAtPosition(0, 0), origin.getUnderlyingIntegerAtPosition(0, 15));
	}

	public void testDisjoint() {
		BitVector origin = new BitVector(16);

		// 0000 0010 1010 0100
		origin.set(6);
		origin.set(8);
		origin.set(13);
		origin.set(10);

		// 0000 0100
		BitVector a = new BitVector(32);
		a.set(5);

		// 0000 0010
		BitVector b = new BitVector(16);
		b.set(6);

		// 0010 0000
		BitVector c = new BitVector(16);
		c.set(2);

		// 1001 0000
		BitVector d = new BitVector(16);
		d.set(0);
		d.set(3);

		// 0000 0010
		// 0000 0100
		assertTrue(origin.Disjoint(a, 0));

		// 0000 0010 1010
		// 000 0010 0
		assertFalse(origin.Disjoint(a, 1));

		// 0000 0010 1010
		// 00 0001 00
		assertTrue(origin.Disjoint(a, 2));

		// 0000 0010 1010 0100
		// 0000 0100
		assertFalse(origin.Disjoint(a, 8));

		// 0000 0010 1010 0100
		// 00 0001 00
		assertTrue(origin.Disjoint(a, 10));

		// 0000 0010 1010 0100
		// 00 0001 00
		assertTrue(origin.Disjoint(a, 14));

		// 0000 0010 1010
		// 0000 0010
		assertFalse(origin.Disjoint(b, 0));

		// 0000 0010 1010
		// 0010 0000
		assertFalse(origin.Disjoint(c, 4));
	}

	public void testUnsignedBitShiftConsistency() {
		// 0000 0000 0000 0000 0000 0000 0000 0001
		BitVector f = new BitVector(32);
		f.set(31);
		assertTrue(f.get(31));
		assertFalse(f.Disjoint(f, 0));

		BitVector g = new BitVector(8);
		g.set(7);
		assertTrue(g.get(7));
		assertFalse(g.Disjoint(g, 0));

	}

	public void testNextZeroBit() {
		// 1010 0000
		BitVector a = new BitVector(32);
		assertEquals(0, a.getNextNonSetBit(0));
		a.set(0);
		a.set(2);

		assertEquals(1, a.getNextNonSetBit(0));
		assertEquals(1, a.getNextNonSetBit(1));

		// 1110 0000
		a.set(1);
		assertEquals(3, a.getNextNonSetBit(0));
		assertEquals(3, a.getNextNonSetBit(1));
		assertEquals(3, a.getNextNonSetBit(2));
		assertEquals(3, a.getNextNonSetBit(3));

		// 1110 0000
		a.set(4);
		assertEquals(3, a.getNextNonSetBit(0));
		assertEquals(3, a.getNextNonSetBit(1));
		assertEquals(3, a.getNextNonSetBit(2));
		assertEquals(3, a.getNextNonSetBit(3));
		assertEquals(5, a.getNextNonSetBit(4));

		for (int i = 0; i < 32; ++i) {
			a.set(i);
		}

		assertEquals(32, a.getNextNonSetBit(0));

		BitVector b = new BitVector(64);

		for (int i = 32; i < 39; ++i) {
			b.set(i);
		}

		for (int i = 40; i < 52; ++i) {
			b.set(i);
		}

		assertEquals(0, b.getNextNonSetBit(0));
		assertEquals(39, b.getNextNonSetBit(32));
	}

	public void testSetVector() {
		BitVector origin = new BitVector(64);

		// 0000 0010 1010 0000 0000 0000 0000 0000 0000 0000 0000 0000
		origin.set(6);
		origin.set(8);
		origin.set(10);

		// 1100 0000
		BitVector a = new BitVector(8);
		a.set(0);
		a.set(1);

		// should result in 0011 0010 1010 0000
		origin.setVector(a, 2);

		assertTrue(origin.get(2));
		assertTrue(origin.get(3));

		// 0000 0000 0000 0000 0100 0011
		BitVector b = new BitVector(32);
		b.set(30);
		b.set(31);
		b.set(25);

		// should result in
		// 0 0000 0000 0000 0000 0000 0000 1000 011
		// 0011 0010 1010 0000 0000 0000 0000 0000 1000 0110 0000
		origin.setVector(b, 7);

		assertTrue(origin.get(32));
		assertTrue(origin.get(37));
		assertTrue(origin.get(38));

		origin.clear();
		// 0000 0010 1010 0000 0000 0000 0000 0000 1000 0000 0000 0000
		origin.set(6);
		origin.set(8);
		origin.set(10);
		origin.set(32);

		// 0110 0000 0000 0000 0100 0011
		BitVector c = new BitVector(32);
		c.set(1);
		c.set(2);
		c.set(25);
		c.set(30);
		c.set(31);

		// should result in
		// 0110 0000 0000 0000 0000 0000 1000 011
		// 0000 0010 1010 0000 0000 0000 0000 0000 1110 0000 0000 0000 0000 0000 1000
		// 011
		origin.setVector(c, 32);

		assertFalse(origin.get(0));
		assertFalse(origin.get(1));
		assertTrue(origin.get(32));
		assertTrue(origin.get(33));
		assertTrue(origin.get(34));
		assertTrue(origin.get(57));
		assertTrue(origin.get(62));
		assertTrue(origin.get(63));

		origin.setVector(c, 50);
		assertTrue(origin.get(52));
	}

	public void testSetVectorOverlap() {
		BitVector origin = new BitVector(64);
		origin.set(6);
		origin.set(8);
		origin.set(10);
		origin.set(32);

		BitVector c = new BitVector(64);
		c.set(1);
		c.set(2);
		c.set(25);
		c.set(30);
		c.set(31);
		c.set(45);
		c.set(57);

		origin.setVector(c, 50);
		assertTrue(origin.get(6));
		assertTrue(origin.get(32));
		assertTrue(origin.get(51));
		assertTrue(origin.get(52));
	}

	public void testSetVectorAndShiftOther() {
		BitVector origin = new BitVector(64);
		origin.set(6);
		origin.set(8);
		origin.set(10);
		origin.set(32);

		BitVector c = new BitVector(64);
		c.set(1);
		c.set(2);
		c.set(25);
		c.set(30);
		c.set(31);
		c.set(45);
		c.set(57);

		origin.setVectorAndShiftOther(c, 14);
		assertTrue(origin.get(6));
		assertTrue(origin.get(8));
		assertTrue(origin.get(11));
		assertTrue(origin.get(16));
		assertTrue(origin.get(17));
		assertTrue(origin.get(31));
		assertTrue(origin.get(32));
		assertTrue(origin.get(43));
	}
}
