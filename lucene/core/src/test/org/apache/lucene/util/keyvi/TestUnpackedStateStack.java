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

public class TestUnpackedStateStack extends LuceneTestCase {
	public void testBasic() throws IOException {
		Path temporaryDirectory = Files.createTempDirectory("dictionary-fsa-unittest");
		SparseArrayPersistence p = new SparseArrayPersistence(2048, temporaryDirectory);

		UnpackedStateStack s = new UnpackedStateStack(p, 20, 100);

		s.insert(3, 42, 56);
		s.insert(3, 47, 57);
		s.insert(2, 40, 33);
		s.insert(1, 20, 11);

		UnpackedState u = s.get(3);
		assertEquals(u.size(), 2);
		assertTrue(u.getBitVector().get(42));
		assertFalse(u.getBitVector().get(66));
		assertTrue(u.getBitVector().get(47));
	}

}
