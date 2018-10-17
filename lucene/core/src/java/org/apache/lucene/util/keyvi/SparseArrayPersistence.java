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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ShortBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import org.apache.lucene.store.DataOutput;

public class SparseArrayPersistence implements Closeable {
	private Path temporaryPath;
	private int bufferSize;
	private int flushSize;
	private byte[] labels;
	private short[] transitions;
	private MemoryMapManager transitionsExtern;
	private MemoryMapManager labelsExtern;
	private int highestRawWriteBucket;
	private int inMemoryBufferOffset;
	private int highestStateBegin;

	public SparseArrayPersistence(int memoryLimit, Path temporaryPath) throws IOException {
		this.temporaryPath = temporaryPath;

		bufferSize = (int) (memoryLimit / 3); // 3 == sizeof(char) + sizeof(short)

		// align it to 16bit (for fast memcpy)
		bufferSize += 16 - (bufferSize % 16);
		flushSize = (bufferSize * 3) / 5;

		// align it to 16bit
		flushSize += 16 - (flushSize % 16);

		labels = new byte[bufferSize];

		Path temporaryDirectory = Files.createTempDirectory(temporaryPath, "dictionary-fsa");

		// size of external memory chunk: not more than 1 or 4 GB
		int externalMemoryChunkSize = Math.min(flushSize * 2, 1073741824);

		// the chunk size must be a multiplier of the flush_size
		externalMemoryChunkSize = externalMemoryChunkSize - (externalMemoryChunkSize % flushSize);

		labelsExtern = new MemoryMapManager(externalMemoryChunkSize, temporaryDirectory, "characterTableFileBuffer");

		transitions = new short[bufferSize];

		transitionsExtern = new MemoryMapManager(externalMemoryChunkSize * 2, temporaryDirectory,
				"valueTableFileBuffer");

	}
	
	@Override
	public void close() throws IOException {
		transitionsExtern.close();
		labelsExtern.close();
	}

	public void beginNewState(int offset) {
		while ((offset + KeyviConstants.COMPACT_SIZE_WINDOW + KeyviConstants.NUMBER_OF_STATE_CODINGS) >= (bufferSize
				+ inMemoryBufferOffset)) {
			flushBuffers();
		}

		if (offset > highestStateBegin) {
			highestStateBegin = offset;
		}
	}

	public void writeTransition(int offset, byte transitionId, short transitionPointer) {
		highestRawWriteBucket = Math.max(highestRawWriteBucket, offset);

		if (offset > inMemoryBufferOffset) {
			labels[offset - inMemoryBufferOffset] = transitionId;
			transitions[offset - inMemoryBufferOffset] = transitionPointer;
			return;
		}

		labelsExtern.getAddressAsByteBuffer(offset).put(transitionId);
		transitionsExtern.getAddressAsByteBuffer(offset * 2).asShortBuffer().put(transitionPointer);
	}

	public int readTransitionLabel(int offset) {
		if (offset >= inMemoryBufferOffset) {
			return labels[offset - inMemoryBufferOffset];
		}
		return labelsExtern.getAddressAsByteBuffer(offset).get();
	}

	public short readTransitionValue(int offset) {
		if (offset >= inMemoryBufferOffset) {
			return transitions[offset - inMemoryBufferOffset];
		}
		
		return transitionsExtern.getAddressAsByteBuffer(offset * 2).asShortBuffer().get();
	}

	public int resolveTransitionValue(int offset, int value) {
		int pt = value;
		int resolved_ptr;

		if ((pt & 0xC000) == 0xC000) {
			// compact transition uint16 absolute

			resolved_ptr = pt & 0x3FFF;
			return resolved_ptr;
		}

		if ((pt & 0x8000) > 0) {
			// clear the first bit
			pt &= 0x7FFF;
			int overflow_bucket = (pt >> 4) + offset - KeyviConstants.COMPACT_SIZE_WINDOW;

			if (overflow_bucket >= inMemoryBufferOffset) {
				resolved_ptr = (int) VInt.decodeVarShort(transitions, overflow_bucket - inMemoryBufferOffset);

			} else {
	      // value needs to be read from external storage, which in 99.9% is a trivial access to the mmap'ed area
	      // but in rare cases might be spread across 2 chunks, for the chunk border test we assume worst case 3 varshorts
	      // to be read, that is a maximum of 2**45, so together with shifting 2**48 == 256 TB of addressable space
				if (transitionsExtern.getAddressQuickTestOk(overflow_bucket * 2, 3 * 2)) {
					ShortBuffer buffer = transitionsExtern.getAddressAsByteBuffer(overflow_bucket * 2).asShortBuffer();

					resolved_ptr = (int) VInt.decodeVarShort(buffer);
				} else {
					// value might be on the chunk border, take a secure approach
          ShortBuffer buffer = transitionsExtern
              .getBuffer(overflow_bucket * 2, 3 * 2).asShortBuffer();

					resolved_ptr = (int) VInt.decodeVarShort(buffer);
				}
			}

			resolved_ptr = (resolved_ptr << 3) + (pt & 0x7);

			if ((pt & 0x8) > 0) {
				// relative coding
				resolved_ptr = offset - resolved_ptr + KeyviConstants.COMPACT_SIZE_WINDOW;
			}

		} else {
			resolved_ptr = offset - pt + KeyviConstants.COMPACT_SIZE_WINDOW;
		}

		return resolved_ptr;
	}

	public int readFinalValue(int offset) {
		if (offset + KeyviConstants.FINAL_OFFSET_TRANSITION >= inMemoryBufferOffset) {

			return (int) VInt.decodeVarShort(transitions,
					offset - inMemoryBufferOffset + KeyviConstants.FINAL_OFFSET_TRANSITION);
		}

		if (transitionsExtern.getAddressQuickTestOk((offset + KeyviConstants.FINAL_OFFSET_TRANSITION) * 2, 5)) {
			ShortBuffer buffer = transitionsExtern
          .getAddressAsByteBuffer((offset + KeyviConstants.FINAL_OFFSET_TRANSITION) * 2).asShortBuffer();

			return (int) VInt.decodeVarShort(buffer);
		}

		// value might be on the chunk border, take a secure approach
		ShortBuffer buffer = transitionsExtern.getBuffer((offset + KeyviConstants.FINAL_OFFSET_TRANSITION) * 2,
        20).asShortBuffer();

		
		return (int) VInt.decodeVarShort(buffer);
	}
	
	public int GetChunkSizeExternalTransitions() {
	  return transitionsExtern.getChunkSize();
	}

	/**
	 * Flush all internal buffers
	 */
	public void flush() {
		// make idempotent, so it can be called twice or more);
		if (labels != null) {
			int highestWritePosition = Math.max(highestStateBegin + KeyviConstants.MAX_TRANSITIONS_OF_A_STATE,
					highestRawWriteBucket + 1);

			labelsExtern.append(labels, highestWritePosition - inMemoryBufferOffset);

			transitionsExtern.append(transitions, highestWritePosition - inMemoryBufferOffset);

			labels = null;
			transitions = null;
		}
	}

	public void write(DataOutput out) throws IOException {
	  int highestWritePosition = Math.max(highestStateBegin + KeyviConstants.MAX_TRANSITIONS_OF_A_STATE,
        highestRawWriteBucket + 1);
	  
	  // version
	  out.writeVInt(2);
	  out.writeVInt(highestWritePosition);
	  labelsExtern.write(out, highestWritePosition);
	  transitionsExtern.write(out, highestWritePosition * 2);
	}
	
	public void writeKeyvi(OutputStream stream) throws IOException {
		int highestWritePosition = Math.max(highestStateBegin + KeyviConstants.MAX_TRANSITIONS_OF_A_STATE,
				highestRawWriteBucket + 1);

		String properties = "{\"version\":\"" + 2 + "\", \"size\":\"" + highestWritePosition + "\"}";

		byte[] propertiesBytes = properties.getBytes(UTF_8);
		
		// todo: re-factor, writing 32bit int
		stream.write((propertiesBytes.length >>> 24) & 0xFF);
		stream.write((propertiesBytes.length >>> 16) & 0xFF);
		stream.write((propertiesBytes.length >>> 8) & 0xFF);
		stream.write(propertiesBytes.length & 0xFF);
		
		stream.write(propertiesBytes);

		labelsExtern.write(stream, highestWritePosition);
		transitionsExtern.write(stream, highestWritePosition * 2);
	}

	private void flushBuffers() {
		labelsExtern.append(labels, flushSize);
		transitionsExtern.append(transitions, flushSize);

		int overlap = bufferSize - flushSize;
		System.arraycopy(labels, flushSize, labels, 0, overlap);
		System.arraycopy(transitions, flushSize, transitions, 0, overlap);
		Arrays.fill(labels, overlap, labels.length, (byte) 0);
		Arrays.fill(transitions, overlap, labels.length, (short) 0);

		inMemoryBufferOffset += flushSize;
	}
}