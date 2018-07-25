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

public class TestMinimizationHash extends LuceneTestCase {

	public void testInsert() {
		
		PackedState p = new PackedState();
		MinimizationHash<PackedState> hash = new MinimizationHash<PackedState>(p);
		//PackedState p = new PackedState();
		
		
		PackedState.Key p1 = p.new Key(10, 25, 2);
		hash.add(p1);
		PackedState.Key p2 = p.new Key(12, 25, 3);
		hash.add(p2);
		PackedState.Key p3 = p.new Key(13, 25, 5);
		hash.add(p3);
		PackedState.Key p4 = p.new Key(15, 25, 6);
		hash.add(p4);

		PackedState.Key p5 = p.new Key();
		assertTrue(hash.get(p1, p5));
		assertEquals(p1, p5);
		
		assertTrue(hash.get(p2, p5));
		assertEquals(p2, p5);
		
		assertTrue(hash.get(p3, p5));
		assertEquals(p3, p5);
		
		assertTrue(hash.get(p4, p5));
		assertEquals(p4, p5);
	}
	
}
