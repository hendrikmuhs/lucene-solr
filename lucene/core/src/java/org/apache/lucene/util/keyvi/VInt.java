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

import java.nio.ShortBuffer;

public class VInt {
	public static long getVarShortLength(long value) {
		return (value > 0x1fffffffffffL) ? 4 : (value < 0x40000000) ? (value < 0x8000) ? 1 : 2 : 3;
	}

	public static int encodeVarShort(long value, short[] output) {
		int outputSize = 0;
		// While more than 15 bits of data are left, occupy the last output byte
		// and set the next byte flag
		while (value > 32767) {
			// |32768: Set the next byte flag
			output[outputSize] = (short) (((short) (value & 32767)) | 32768);

			// Remove the 15 bits we just wrote
			value >>= 15;
			outputSize++;
		}
		output[outputSize++] = (short) (((short) value) & 32767);
		return outputSize;
	}

	public static long decodeVarShort(short[] input) {
		long value = 0;
		for (int i = 0;; i++) {
			value |= ((long)input[i] & 32767) << (15 * i);

			// If the next-byte flag is set
			if ((input[i] & 32768) == 0) {
				break;
			}
		}
		return value;
	}
	
	public static long decodeVarShort(short[] input, int offset) {
		long value = 0;
		for (int i = 0;; i++) {
			value |= ((long)input[offset + i] & 32767) << (15 * i);

			// If the next-byte flag is set
			if ((input[offset + i] & 32768) == 0) {
				break;
			}
		}
		return value;
	}

	public static long decodeVarShort(ShortBuffer buffer) {
		long value = 0;
		for (int i = 0;; i++) {
			short current = buffer.get();
			
			value |= ((long)current & 32767) << (15 * i);

			// If the next-byte flag is set
			if ((current & 32768) == 0) {
				break;
			}
		}
		return value;
	}

}
