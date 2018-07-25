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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.lucene.util.LuceneTestCase;

public class TestUnpackedState extends LuceneTestCase {

	public void testBasic() throws IOException {
		Path temporaryDirectory = Files.createTempDirectory("dictionary-fsa-unittest");
		SparseArrayPersistence p = new SparseArrayPersistence(2048, temporaryDirectory);
		UnpackedState u1 = new UnpackedState(p);
		u1.add(65, 100);
		u1.add(66, 101);
		
		assertEquals(2, u1.size());
		u1.add(80, 102);
		assertEquals(3, u1.size());

		assertTrue(u1.getBitVector().get(65));
		assertTrue(u1.getBitVector().get(66));
		assertFalse(u1.getBitVector().get(67));
		int hashCode = u1.hashCode();
		
		u1.clear();
		assertEquals(u1.size(), 0);

		assertFalse(u1.getBitVector().get(65));
		assertFalse(u1.getBitVector().get(66));
		assertFalse(u1.getBitVector().get(67));
		assertTrue(hashCode  != u1.hashCode());
	}

	public void testHashWithWeights() throws IOException {
		Path temporaryDirectory = Files.createTempDirectory("dictionary-fsa-unittest");
		SparseArrayPersistence p = new SparseArrayPersistence(2048, temporaryDirectory);
		UnpackedState u1 = new UnpackedState(p);
		u1.add(65, 100);
		u1.add(66, 101);
		
		UnpackedState u2 = new UnpackedState(p);
		u2.add(65, 100);
		u2.add(66, 101);
		u2.updateWeightIfHigher(42);
		
		UnpackedState u3 = new UnpackedState(p);
		u3.add(65, 100);
		u3.add(66, 101);
		u3.updateWeightIfHigher(444);
		
		// u1 and u2 should have different hashcodes as u2 has weight but u1 not
		assertTrue(u1.hashCode() != u2.hashCode());

		// u2 and u3 should have equal hashcodes although the weights are different
		assertEquals(u2.hashCode(), u3.hashCode());
	}
}
