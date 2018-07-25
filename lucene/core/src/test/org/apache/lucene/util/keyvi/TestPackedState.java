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

public class TestPackedState extends LuceneTestCase {

	public void testHashcode() {
		PackedState ps = new PackedState();
		MinimizationHashEntry.Key p = ps.new Key(0, 42, 42);
		
		assertEquals(42, p.hashCode());	
		}
	
	public void testNumberOutgoingStatesAndCookie() {
		PackedState ps = new PackedState();
		
		PackedState.Key p1 = ps.new Key(0,0,1);
		
		assertEquals(1, p1.getNumberOfOutgoingTransitions());
		assertEquals(0, p1.getCookie());
		
		p1.setCookie(25);	
		assertEquals(1, p1.getNumberOfOutgoingTransitions());
		assertEquals(25, p1.getCookie());
		
		p1.setCookie(6948);
		assertEquals(1, p1.getNumberOfOutgoingTransitions());
		assertEquals(6948, p1.getCookie());
		
		p1.setCookie(0);
		assertEquals(1, p1.getNumberOfOutgoingTransitions());
		assertEquals(0, p1.getCookie());
		
		PackedState ps2 = new PackedState();
		PackedState.Key p2 = ps2.new Key(0, 0, 257);
		
		assertEquals(257, p2.getNumberOfOutgoingTransitions());
		p2.setCookie(6948);
		
		assertEquals(257, p2.getNumberOfOutgoingTransitions());
		assertEquals(6948, p2.getCookie());
		
		p2.setCookie(ps.getMaxCookieSize());
		assertEquals(257, p2.getNumberOfOutgoingTransitions());
		assertEquals(ps.getMaxCookieSize(), p2.getCookie());
	}
}
