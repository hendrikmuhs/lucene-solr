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
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import org.apache.lucene.util.LuceneTestCase;

public class TestMemoryMapManager extends LuceneTestCase {

	public void testBasic() throws IOException {
		int chunkSize = 1024 * 1024;
		Path temporaryDirectory = Files.createTempDirectory("dictionary-fsa-unittest");
		MemoryMapManager m = new MemoryMapManager(chunkSize, temporaryDirectory, "basic test");
		m.getAddressAsByteBuffer(0);
		ByteBuffer buf = m.getAddressAsByteBuffer(2 * chunkSize);

		for (int i = 0; i < chunkSize; ++i) {
			buf.put(i, (byte) 42);
		}
		assertEquals(buf.get(42), 42);

		ByteBuffer buf2 = m.getAddressAsByteBuffer(chunkSize);
		for (int i = 0; i < chunkSize - 1; ++i) {
			buf2.put(i, (byte) 24);
		}

		assertEquals(buf.get(42), 42);
		assertEquals(buf2.get(42), 24);

		// TODO: fix: Files.deleteIfExists(temporaryDirectory);
	}

	public void testGetBuffer() throws IOException {
		int chunkSize = 1024 * 1024;
		Path temporaryDirectory = Files.createTempDirectory("dictionary-fsa-unittest");
		MemoryMapManager m = new MemoryMapManager(chunkSize, temporaryDirectory, "basic test");

		m.getAddressAsByteBuffer(0);
		ByteBuffer buf = m.getAddressAsByteBuffer(chunkSize - 2);
		buf.put((byte) 104);

		buf = m.getAddressAsByteBuffer(chunkSize - 1);
		buf.put((byte) 101);

		buf = m.getAddressAsByteBuffer(chunkSize);
		buf.put((byte) 108);

		buf = m.getAddressAsByteBuffer(chunkSize + 1);
		buf.put((byte) 109);

		buf = m.getAddressAsByteBuffer(chunkSize + 2);
		buf.put((byte) 111);

	}

	public void testAppendLargeChunk() throws IOException {
		int chunkSize = 1024 * 1024;
		Path temporaryDirectory = Files.createTempDirectory("dictionary-fsa-unittest");
		MemoryMapManager m = new MemoryMapManager(chunkSize, temporaryDirectory, "basic test");

		m.getAddressAsByteBuffer(0);

		byte[] buffer = new byte[16384];
		Arrays.fill(buffer, 0, 4096, (byte) 121);
		Arrays.fill(buffer, 4096, 16384, (byte) 122);

		buffer[8887] = 'w';
		buffer[8889] = 'y';

		buffer[9503] = 'a';
		buffer[9504] = 'b';

		buffer[12003] = 'c';
		buffer[12005] = 'd';

		buffer[14000] = 'e';
		buffer[14001] = 'f';

		buffer[16382] = 'g';
		buffer[16383] = 'h';

		m.append(buffer, buffer.length);
		
		assertEquals(122, m.getAddressAsByteBuffer(8888).get());
		assertEquals(97, m.getAddressAsByteBuffer(9503).get());
		assertEquals(122, m.getAddressAsByteBuffer(12004).get());
		assertEquals(101, m.getAddressAsByteBuffer(14000).get());
		assertEquals(104, m.getAddressAsByteBuffer(16383).get());
	}

	public void testAppendChunkOverflow() throws IOException {
		int chunkSize = 4096;
		Path temporaryDirectory = Files.createTempDirectory("dictionary-fsa-unittest");
		MemoryMapManager m = new MemoryMapManager(chunkSize, temporaryDirectory, "chunk test");

		m.getAddressAsByteBuffer(0);

		byte[] buffer = new byte[1000];
		Arrays.fill(buffer, 0, 1000, (byte) 122);
		m.append(buffer, buffer.length);		
		Arrays.fill(buffer, 0, 1000, (byte) 121);
		m.append(buffer, buffer.length);
		Arrays.fill(buffer, 0, 1000, (byte) 123);
		m.append(buffer, buffer.length);
		Arrays.fill(buffer, 0, 1000, (byte) 120);
		m.append(buffer, buffer.length);
		Arrays.fill(buffer, 0, 1000, (byte) 119);
		m.append(buffer, buffer.length);
		Arrays.fill(buffer, 0, 1000, (byte) 118);
		m.append(buffer, buffer.length);
		
		assertEquals(122, m.getAddressAsByteBuffer(999).get());
		assertEquals(121, m.getAddressAsByteBuffer(1567).get());
		assertEquals(123, m.getAddressAsByteBuffer(2356).get());
		assertEquals(120, m.getAddressAsByteBuffer(3333).get());
		assertEquals(119, m.getAddressAsByteBuffer(4444).get());
		assertEquals(118, m.getAddressAsByteBuffer(5555).get());
		assertEquals(6000, m.size());
	}

	 public void testAppendChunkOverflowShorts() throws IOException {
	    int chunkSize = 4096;
	    Path temporaryDirectory = Files.createTempDirectory("dictionary-fsa-unittest");
	    MemoryMapManager m = new MemoryMapManager(chunkSize, temporaryDirectory, "chunk test");

	    m.getAddressAsByteBuffer(0);

	    short[] buffer = new short[1000];
	    Arrays.fill(buffer, 0, 1000, (short) -10000);
	    m.append(buffer, buffer.length);    
	    Arrays.fill(buffer, 0, 1000, (short) 32000);
	    m.append(buffer, buffer.length);
	    Arrays.fill(buffer, 0, 1000, (short) 123);
	    m.append(buffer, buffer.length);
	    Arrays.fill(buffer, 0, 1000, (short) 15000);
	    m.append(buffer, buffer.length);
	    Arrays.fill(buffer, 0, 1000, (short) 119);
	    m.append(buffer, buffer.length);
	    Arrays.fill(buffer, 0, 1000, (short) -15001);
	    m.append(buffer, buffer.length);
	    
	    assertEquals(-10000, m.getAddressAsByteBuffer(999*2).asShortBuffer().get());
	    assertEquals(32000, m.getAddressAsByteBuffer(1567*2).asShortBuffer().get());
	    assertEquals(123, m.getAddressAsByteBuffer(2356*2).asShortBuffer().get());
	    assertEquals(15000, m.getAddressAsByteBuffer(3333*2).asShortBuffer().get());
	    assertEquals(119, m.getAddressAsByteBuffer(4444*2).asShortBuffer().get());
	    assertEquals(-15001, m.getAddressAsByteBuffer(5555*2).asShortBuffer().get());
	    assertEquals(12000, m.size());
	  }
	
	public void testChunkBehindTail() throws IOException {
		int chunkSize = 4096;
		Path temporaryDirectory = Files.createTempDirectory("dictionary-fsa-unittest");
		MemoryMapManager m = new MemoryMapManager(chunkSize, temporaryDirectory, "basic test");

		m.getAddressAsByteBuffer(0);


		byte[] buffer = new byte[1024];
		Arrays.fill(buffer, 0, 1024, (byte) 122);
		m.append(buffer, buffer.length);		
		Arrays.fill(buffer, 0, 1024, (byte) 121);
		m.append(buffer, buffer.length);
		Arrays.fill(buffer, 0, 1024, (byte) 123);
		m.append(buffer, buffer.length);
		
		byte[] buffer2 = new byte[1023];
		Arrays.fill(buffer2, 0, 1023, (byte) 120);
		m.append(buffer2, buffer2.length);
		
		// tail should now be at 4096
		assertEquals(4095, m.size());
		
		// force a new chunk to be created, although tail does not require it
		m.getBuffer(4094, 10);

		/*
		  auto filename = path;
		  filename /= "out";
		  std::ofstream out_stream(filename.native(), std::ios::binary);
		  m.Write(out_stream, m.GetSize() );
		  BOOST_CHECK_EQUAL(4095, out_stream.tellp());
		  out_stream.close();
		  */
	}
	
	/*

	 
	 
	 * 
	 * BOOST_AUTO_TEST_CASE( Persist ) { size_t chunkSize = 4096;
	 * 
	 * boost::filesystem::path path = boost::filesystem::temp_directory_path();
	 * path /= boost::filesystem::unique_path(
	 * "dictionary-fsa-unittest-%%%%-%%%%-%%%%-%%%%");
	 * boost::filesystem::create_directory(path); MemoryMapManager m(chunkSize,
	 * path, "append large chunk test");
	 * 
	 * char buffer[4096]; std::fill(buffer, buffer+4096, 'x'); m.Append(buffer,
	 * 4096); m.Append(buffer, 1);
	 * 
	 * m.Persist();
	 * 
	 * auto filename = path; filename /= "out"; std::ofstream
	 * out_stream(filename.native(), std::ios::binary); m.Write(out_stream, 0 );
	 * BOOST_CHECK_EQUAL(4097, out_stream.tellp()); out_stream.close();
	 * 
	 * boost::filesystem::remove_all(path); }
	 * 
	 */
}
