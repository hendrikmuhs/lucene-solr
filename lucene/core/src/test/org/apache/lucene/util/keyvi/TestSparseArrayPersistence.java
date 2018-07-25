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

public class TestSparseArrayPersistence extends LuceneTestCase {

	public void testBasic() throws IOException {
		int memoryLimit = 1024 * 1024;
		Path temporaryDirectory = Files.createTempDirectory("dictionary-fsa-unittest");

		SparseArrayPersistence p = new SparseArrayPersistence(memoryLimit, temporaryDirectory);

		p.writeTransition(1, (byte) 42, (short) 43);
		p.writeTransition(200, (byte) 44, (short) 45);
		assertEquals(42, p.readTransitionLabel(1));
		assertEquals(43, p.readTransitionValue(1));
		assertEquals(44, p.readTransitionLabel(200));
		assertEquals(45, p.readTransitionValue(200));

		// enforce flush in persistence
		p.beginNewState(memoryLimit * 20);
		assertEquals(42, p.readTransitionLabel(1));
		assertEquals(43, p.readTransitionValue(1));
		assertEquals(44, p.readTransitionLabel(200));
		assertEquals(45, p.readTransitionValue(200));
	}

}
