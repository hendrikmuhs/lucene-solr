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

public class TestVInt extends LuceneTestCase {

	public void testVShortSimple() {
		short[] buffer = new short[8];
		int size;

		size = VInt.encodeVarShort(77777, buffer);

		assertEquals(2, size);
		assertEquals(77777, VInt.decodeVarShort(buffer));

		size = VInt.encodeVarShort(55, buffer);

		assertEquals(1, size);
		assertEquals(55, VInt.decodeVarShort(buffer));
	}

	public void testVShortLength() {
		short[] buffer = new short[16];
		int size;

		size = VInt.encodeVarShort(77777, buffer);

		assertEquals(VInt.getVarShortLength(77777), size);
		assertEquals(77777, VInt.decodeVarShort(buffer));

		size = VInt.encodeVarShort(32767, buffer);
		assertEquals(VInt.getVarShortLength(32767), size);
		assertEquals(1, size);
		assertEquals(32767, VInt.decodeVarShort(buffer));

		size = VInt.encodeVarShort(32768, buffer);
		assertEquals(VInt.getVarShortLength(32768), size);
		assertEquals(2, size);
		assertEquals(32768, VInt.decodeVarShort(buffer));

		size = VInt.encodeVarShort(0x3fffffffl, buffer);
		assertEquals(VInt.getVarShortLength(0x3fffffffl), size);
		assertEquals(2, size);
		assertEquals(0x3fffffffl, VInt.decodeVarShort(buffer));

		size = VInt.encodeVarShort(0x40000000l, buffer);
		assertEquals(VInt.getVarShortLength(0x40000000l), size);
		assertEquals(3, size);
		assertEquals(0x40000000l, VInt.decodeVarShort(buffer));

		size = VInt.encodeVarShort(0x1fffffffffffl, buffer);
		assertEquals(VInt.getVarShortLength(0x1fffffffffffl), size);
		assertEquals(3, size);
		assertEquals(0x1fffffffffffl, VInt.decodeVarShort(buffer));

		size = VInt.encodeVarShort(0x200000000000l, buffer);
		assertEquals(VInt.getVarShortLength(0x200000000000l), size);
		assertEquals(4, size);
		assertEquals(0x200000000000l, VInt.decodeVarShort(buffer));

		assertEquals(1, VInt.getVarShortLength(11687));
		size = VInt.encodeVarShort(11687, buffer);
		assertEquals(1, size);
	}
}
