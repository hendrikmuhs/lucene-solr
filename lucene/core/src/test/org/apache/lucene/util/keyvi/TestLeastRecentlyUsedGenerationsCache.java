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

public class TestLeastRecentlyUsedGenerationsCache extends LuceneTestCase {

	public void testGenerations() {
		PackedState ps = new PackedState();
		
		LeastRecentlyUsedGenerationsCache<PackedState> cache = new LeastRecentlyUsedGenerationsCache<PackedState>(ps, 3, 5);

		PackedState.Key p1 = ps.new Key(1,1,1);
		PackedState.Key p1_1 = ps.new Key(1,1,1);
		PackedState.Key p2 = ps.new Key(2,2,1);
		PackedState.Key p2_1 = ps.new Key(2,2,1);
		PackedState.Key p3 = ps.new Key(3,3,1);
		PackedState.Key p3_1 = ps.new Key(3,3,1);
		PackedState.Key p4 = ps.new Key(4,4,1);
		PackedState.Key p5 = ps.new Key(5,5,1);
		PackedState.Key p6 = ps.new Key(6,6,1);
		PackedState.Key p7 = ps.new Key(7,7,1);
		PackedState.Key p8 = ps.new Key(8,8,1);
		PackedState.Key p9 = ps.new Key(9,9,1);
		PackedState.Key p9_1 = ps.new Key(9,9,1);
		PackedState.Key p10 = ps.new Key(10,10,1);
		PackedState.Key p11 = ps.new Key(11,11,1);
		PackedState.Key p11_1 = ps.new Key(11,11,1);
		PackedState.Key p12 = ps.new Key(12,12,1);
		PackedState.Key p13 = ps.new Key(13,13,1);
		PackedState.Key p14 = ps.new Key(14,14,1);
		PackedState.Key p14_1 = ps.new Key(14,14,1);
		PackedState.Key p15 = ps.new Key(15,15,1);
		PackedState.Key p15_1 = ps.new Key(15,15,1);
		PackedState.Key p16 = ps.new Key(16,16,1);
		PackedState.Key p17 = ps.new Key(17,17,1);
		PackedState.Key p18 = ps.new Key(18,18,1);
		PackedState.Key p19 = ps.new Key(19,19,1);
		PackedState.Key p20 = ps.new Key(20,20,1);
		PackedState.Key p21 = ps.new Key(21,21,1);
		
		cache.add(p1);
		cache.add(p2);
		cache.add(p3);
		cache.add(p4);
		cache.add(p5);
		cache.add(p6);
		cache.add(p7);
		cache.add(p8);
		cache.add(p9);
		cache.add(p10);
		cache.add(p11);
		cache.add(p12);
		cache.add(p13);
		cache.add(p14);
		cache.add(p15);

		PackedState.Key lookupKey = ps.new Key();
		assertTrue(cache.get(p1_1, lookupKey));
		assertEquals(p1, lookupKey);
		
		cache.add(p16);
		cache.add(p17);
		cache.add(p18);
		cache.add(p19);
		
		assertTrue(cache.get(p1_1, lookupKey));
		assertEquals(p1, lookupKey);
		assertFalse(cache.get(p2_1, lookupKey));
		assertFalse(cache.get(p3_1, lookupKey));
		cache.add(p20);
		cache.add(p21);
		
		assertTrue(cache.get(p1_1, lookupKey));
		assertEquals(p1, lookupKey);
		assertFalse(cache.get(p9_1, lookupKey));
		
		assertTrue(cache.get(p11_1, lookupKey));
		assertEquals(p11, lookupKey);
		assertTrue(cache.get(p14_1, lookupKey));
		assertEquals(p14, lookupKey);
		assertTrue(cache.get(p15_1, lookupKey));
		assertEquals(p15, lookupKey);
		assertTrue(cache.get(p1_1, lookupKey));
		assertEquals(p1, lookupKey);
	}
}
