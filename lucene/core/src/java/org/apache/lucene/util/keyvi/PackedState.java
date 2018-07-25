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

/**
 * Represents a state in the state hashtable. Since we'll need to save millions
 * of these, we aim to make each object very small.
 */
public final class PackedState implements MinimizationHashEntry {

	private static int MAX_COOKIE_SIZE = 0x7FFFFE;

	public final class Key implements MinimizationHashEntry.Key {

		// for now all integers
		private int offset;
		private int hashCode;
		private int numberOutgoingStatesAndCookie;

		public Key() {
			offset = 0;
			hashCode = 0;
			numberOutgoingStatesAndCookie = 0;
		}

		public Key(int offset, int hashcode, int numberOutgoingStatesAndCookie) {
			this.offset = offset;
			this.hashCode = hashcode;
			this.numberOutgoingStatesAndCookie = numberOutgoingStatesAndCookie;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.lucene.util.keyvi.MinimizationHashEntry#set(int, int, int)
		 */
		@Override
		public void set(int offset, int hashcode, int numberOutgoingStatesAndCookie) {
			this.offset = offset;
			this.hashCode = hashcode;
			this.numberOutgoingStatesAndCookie = numberOutgoingStatesAndCookie;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.lucene.util.keyvi.MinimizationHashEntry#getExtra()
		 */
		@Override
		public int getExtra() {
			return numberOutgoingStatesAndCookie;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.lucene.util.keyvi.MinimizationHashEntry#getExtra()
		 */
		@Override
		public int recalculateExtra(int extra, int newCookie) {
			return (extra & 0x1FF) | (newCookie << 9);
		}

		public int hashCode() {
			return this.hashCode;
		}

		public int getNumberOfOutgoingTransitions() {
			return numberOutgoingStatesAndCookie & 0x1FF;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.lucene.util.keyvi.MinimizationHashEntry#getCookie()
		 */
		@Override
		public int getCookie() {
			return (numberOutgoingStatesAndCookie & 0xFFFFFE00) >>> 9;

		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.lucene.util.keyvi.MinimizationHashEntry#setCookie(int)
		 */
		@Override
		public void setCookie(int value) {
			numberOutgoingStatesAndCookie = (numberOutgoingStatesAndCookie & 0x1FF) | (value << 9);
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.lucene.util.keyvi.MinimizationHashEntry#getOffset()
		 */
		@Override
		public int getOffset() {
			return offset;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.lucene.util.keyvi.MinimizationHashEntry#isEmpty()
		 */
		@Override
		public boolean isEmpty() {
			return offset == 0 && hashCode == 0;
		}

		@Override
		public boolean equals(Object other) {
			Key k = (Key) other;
			return offset == k.offset;
		}
	}

	public int getCookie(int numberOutgoingStatesAndCookie) {
		return (numberOutgoingStatesAndCookie & 0xFFFFFE00) >>> 9;
	}

	public int getMaxCookieSize() {
		return MAX_COOKIE_SIZE;
	}

}
