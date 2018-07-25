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

import java.util.Arrays;

// to bad: we can not use generics (T) as we need an array of T, which isn't possible
public class MinimizationHash<T extends MinimizationHashEntry> {

	public class MemoryLimitConfiguration {
		private long bestFitMemoryLimit = 0;
		private int bestFitGenerations = 0;
		private int bestFitMaximumNumbersOfItemsPerTable = 0;
		
		public MemoryLimitConfiguration(long memoryLimit, int generations, int maxNumberOfItemsPerTable) {
			this.bestFitMemoryLimit = memoryLimit;
			this.bestFitGenerations = generations;
			this.bestFitMaximumNumbersOfItemsPerTable = maxNumberOfItemsPerTable;
		}
		
		public long getBestFitMemoryLimit() {
			return bestFitMemoryLimit;
		}

		public void setBestFitMemoryLimit(long bestFitMemoryLimit) {
			this.bestFitMemoryLimit = bestFitMemoryLimit;
		}

		public int getBestFitGenerations() {
			return bestFitGenerations;
		}

		public void setBestFitGenerations(int bestFitGenerations) {
			this.bestFitGenerations = bestFitGenerations;
		}

		public int getBestFitMaximumNumbersOfItemsPerTable() {
			return bestFitMaximumNumbersOfItemsPerTable;
		}

		public void setBestFitMaximumNumbersOfItemsPerTable(int bestFitMaximumNumbersOfItemsPerTable) {
			this.bestFitMaximumNumbersOfItemsPerTable = bestFitMaximumNumbersOfItemsPerTable;
		}
		
	}
	
	/// magic constants definition of good hash table sizes
	private static int[] HASH_SIZE_STEP_TABLE = { 997, 2029, 4079, 8171, 16363, 32749, 65519, 131041, 262127, 524269,
			1048559, 2097133, 4194287, 8388587, 16777199, 33554393, 67108837, 134217689, 268435399, 536870879,
			1073741789, 2147483629 };

	/// Load factor of the hash table, used to calculate the rehashLimit.
	private static float LOAD_FACTOR = 0.6f;

	private int hashSizeStep;

	private final int initialHashSizeStep;

	private final int overflowLimit;

	private final T entry;

	private int hashSize;

	private int rehashLimit;

	private int[] entriesOffsets;

	private int overflowEntriesSize;

	private int[] overflowEntriesOffsets;

	private int count;

	private int overflowCount;

	private int[] entriesHashes;

	private int[] entriesCookies;

	private int[] overflowEntriesHashes;

	private int[] overflowEntriesCookies;

	public MinimizationHash(T entry) {
		this(entry, 3, 8);
	}

	public MinimizationHash(T entry, int initialStep, int overflowLimit) {
		initialHashSizeStep = initialStep;
		this.overflowLimit = overflowLimit;
		hashSizeStep = Math.min(initialStep, HASH_SIZE_STEP_TABLE.length - 1);

		// todo: retrieve from T
		// T entry = new T();
		this.entry = entry;
		clear();
	}

	MemoryLimitConfiguration findMemoryLimitConfiguration(long memoryLimit, int min, int max) {
	    long bestFitMemoryLimit = 0;
	    int bestFitGenerations = 0;
	    int bestFitMaximumNumberOfItemsPerTable = 0;
	    
		// try to find a good number of generations (between given minimum and maximum) with equal size given the memory
	    // Limit
	    for (int i = min; i <= max; ++i) {
	      int maximumNumberOfItems = 0;

	      // find the value that fits to it in the HashmapSteptable
	      for (int step = 3; step < HASH_SIZE_STEP_TABLE.length; ++step) {
	        // the memory usage is the size of the hashtable itself and the size of the array for the overflow buckets which
	        // is the number of buckets divided by 4.
	        long itemForecastForHashtable =
	        		HASH_SIZE_STEP_TABLE[step] + Math.min(HASH_SIZE_STEP_TABLE[step] >> 2, entry.getMaxCookieSize());

	        // todo: replace '12' with constant
	        long memoryUsageForecast = itemForecastForHashtable * 12 * i;

	        if (memoryLimit < memoryUsageForecast) {
	          maximumNumberOfItems = HASH_SIZE_STEP_TABLE[step - 1];
	          break;
	        }
	      }

	      // calculate memory usage
	        // todo: replace '12' with constant

	      int usage = (maximumNumberOfItems + (maximumNumberOfItems >> 2)) * 12 * i;
	      if (usage > bestFitMemoryLimit) {
	    	  bestFitMemoryLimit = usage;
	        bestFitGenerations = i;
	        bestFitMaximumNumberOfItemsPerTable = (int) (maximumNumberOfItems * LOAD_FACTOR);
	      }
	    }

	    if (bestFitMaximumNumberOfItemsPerTable == 0) {
	      // todo: exception?
	    }
		
		return new MemoryLimitConfiguration(bestFitMemoryLimit, bestFitGenerations, bestFitMaximumNumberOfItemsPerTable);
	}
	
	/*
	 * 
	 MemoryLimitConfiguration FindMemoryLimitConfiguration ( size_t memory_limit,
	 * int min, int max) const { int best_fit_memory_limit = 0; int
	 * best_fit_generations = 0; int best_fit_maximum_number_of_items_per_table = 0;
	 * 
	 * // try to find a good number of generations (between given minimum and
	 * maximum) with equal size given the memory Limit for (int i = min; i <= max;
	 * ++i) { int maximum_number_of_items = 0;
	 * 
	 * // find the value that fits to it in the HashmapSteptable for (unsigned int
	 * step = 3; step < kHashMaxSizeStep; ++step) { // the memory usage is the size
	 * of the hashtable itself and the size of the array for the overflow buckets
	 * which is the number of buckets divided by 4. size_t
	 * item_forecast_for_hashtable = kHashSizeStepTable[step] +
	 * std::min(kHashSizeStepTable[step] >> 2, max_cookie_size_);
	 * 
	 * size_t memoryUsageForeCast = item_forecast_for_hashtable * sizeof(T) i;
	 * 
	 * if (memory_limit < memoryUsageForeCast) { maximum_number_of_items =
	 * kHashSizeStepTable[step - 1]; break; } }
	 * 
	 * // calculate memory usage int usage = (maximum_number_of_items +
	 * (maximum_number_of_items >> 2)) sizeof(T) * i; if (usage >
	 * best_fit_memory_limit) { best_fit_memory_limit = usage; best_fit_generations
	 * = i; best_fit_maximum_number_of_items_per_table = (int)
	 * (maximum_number_of_items * kLoadFactor); } }
	 * 
	 * if (best_fit_maximum_number_of_items_per_table == 0) { // todo: exception? }
	 * 
	 * return MemoryLimitConfiguration( best_fit_memory_limit, best_fit_generations,
	 * best_fit_maximum_number_of_items_per_table); }
	 */
	
	
	
	
	
	/**
	 * Remove all entries from the hash table
	 */
	public void clear() {
		hashSizeStep = initialHashSizeStep;
		hashSize = HASH_SIZE_STEP_TABLE[hashSizeStep];
		rehashLimit = Math.round(hashSize * LOAD_FACTOR);

		entriesOffsets = new int[hashSize];
		entriesHashes = new int[hashSize];
		entriesCookies = new int[hashSize];

		overflowEntriesSize = Math.min(hashSize >> 2, entry.getMaxCookieSize());

		overflowEntriesOffsets = new int[overflowEntriesSize];
		overflowEntriesHashes = new int[overflowEntriesSize];
		overflowEntriesCookies = new int[overflowEntriesSize];

		count = 0;
		overflowCount = 1;
	}

	/**
	 * Removes all entries from the hashtable without recreating the table.
	 */
	public void reset() {
		Arrays.fill(entriesOffsets, 0);
		Arrays.fill(entriesHashes, 0);
		Arrays.fill(entriesCookies, 0);

		count = 0;
		overflowCount = 1;
	}

	/**
	 * Perform a hash lookup. If the hash values are equal, the key's equality
	 * operator is called upon each entry with the same hash value.
	 * 
	 * @tparam EqualityType a type that can be used for comparison (must implement a
	 *         get_hashcode and operator=)
	 * @param key
	 *            key for lookup
	 * @return the equal state or an empty value
	 */

	// todo: factor out interface for comparison
	public boolean get(Object key, T.Key entry) {
		int hash = key.hashCode() & 0x7fffffff;
		int bucket = hash % hashSize;

		entry.set(entriesOffsets[bucket], entriesHashes[bucket], entriesCookies[bucket]);
		while (entry.isEmpty() == false) {
			if (key.equals(entry)) {
				return true;
			}

			int overflowBucket = entry.getCookie();
			if (overflowBucket != 0) {
				entry.set(overflowEntriesOffsets[overflowBucket], overflowEntriesHashes[overflowBucket],
						overflowEntriesCookies[overflowBucket]);
			} else {
				return false;
			}
		}

		return false;
	}

	/**
	 * Perform a hash lookup. If the hash values are equal, the key's equality
	 * operator is called upon each entry with the same hash value.
	 * 
	 * @param key
	 *            key for lookup
	 * @param other
	 *            another instance of MinimizationHash to move the entry to
	 * @return the equal state or an empty value
	 */

	public boolean getAndMove(Object key, MinimizationHash<T> other, T.Key entry) {
		int hash = key.hashCode() & 0x7fffffff;;
		int bucket = hash % hashSize;

		entry.set(entriesOffsets[bucket], entriesHashes[bucket], entriesCookies[bucket]);
		if (entry.isEmpty() == false) {
			if (key.equals(entry)) {

				// delete the old entry
				int overflowBucket = entry.getCookie();
				if (overflowBucket != 0) {
					// overwrite with the entry from overflow
					entriesOffsets[bucket] = overflowEntriesOffsets[overflowBucket];
					entriesHashes[bucket] = overflowEntriesHashes[overflowBucket];
					entriesCookies[bucket] = overflowEntriesCookies[overflowBucket];
				}

				entry.setCookie(0);
				other.add(entry);

				return true;
			}

			// check for more items in overflow
			int overflowBucket = entry.getCookie();
			if (overflowBucket != 0) {
				entry.set(overflowEntriesOffsets[overflowBucket], overflowEntriesHashes[overflowBucket],
						overflowEntriesCookies[overflowBucket]);
				if (key.equals(entry)) {
					// disconnect this entry
					entriesCookies[bucket] = entry.recalculateExtra(entriesCookies[bucket], entry.getCookie());

					entry.setCookie(0);
					other.add(entry);
					return true;
				}
			}

			// search further in overflowEntries
			overflowBucket = entry.getCookie();
			entry.set(overflowEntriesOffsets[overflowBucket], overflowEntriesHashes[overflowBucket],
					overflowEntriesCookies[overflowBucket]);

			while (entry.isEmpty() == false) {
				if (key.equals(entry)) {
					// disconnect entry
					overflowEntriesCookies[overflowBucket] = entry.recalculateExtra(entriesCookies[bucket],
							entry.getCookie());

					entry.setCookie(0);
					other.add(entry);

					return true;
				}

				overflowBucket = entry.getCookie();
				entry.set(overflowEntriesOffsets[overflowBucket], overflowEntriesHashes[overflowBucket],
						overflowEntriesCookies[overflowBucket]);
			}
		}
		return false;
	}

	/**
	 * Return the number of items in the hash.
	 * 
	 * @return number of items.
	 */
	int size() {
		return count;
	}

	/**
	 * Add this entry. This does not test whether the object is already contained in
	 * the hash.
	 * 
	 * @param key
	 *            The key to add.
	 */

	public void add(T.Key key) {
		insert(key);

		// do not increment count in insert as it is used for rehashing
		++count;

		// check condition for re-hashing: count reaches limit
		if (count > rehashLimit && hashSizeStep < HASH_SIZE_STEP_TABLE.length - 1) {
			growAndRehash(key);
		}

		// check condition for re-hashing: overflow reaches limit
		if (overflowCount == overflowEntriesSize && overflowEntriesSize < entry.getMaxCookieSize()
				&& hashSizeStep < HASH_SIZE_STEP_TABLE.length - 1) {
			growAndRehash(key);
		}
	}

	long getMemoryUsage() { 
		return 0;
	}
	

	private void insert(T.Key key) {
		int hash = key.hashCode() & 0x7fffffff;;
		int bucket = hash % hashSize;

		if (entriesOffsets[bucket] == 0) {
			entriesOffsets[bucket] = key.getOffset();
			entriesHashes[bucket] = key.hashCode();
			entriesCookies[bucket] = key.getExtra();
		} else {
			// overflow handling

			// overflowing is limited to the address space in PrivateUse, if we run out we
			// drop the entries
			if (overflowCount == entry.getMaxCookieSize()) {
				return;
			}
			// todo: only get the cookie!
			int overflowBucket = entry.getCookie(entriesCookies[bucket]);

			if (overflowBucket == 0) {
				entriesCookies[bucket] = key.recalculateExtra(entriesCookies[bucket], overflowCount);
				overflowEntriesOffsets[overflowCount] = key.getOffset();
				overflowEntriesHashes[overflowCount] = key.hashCode();
				overflowEntriesCookies[overflowCount] = key.getExtra();
				overflowCount++;
			} else {
				int numberOfOverflows = 0;
				

				while (overflowEntriesCookies[overflowBucket] != 0 && numberOfOverflows < overflowLimit) {
					// todo: only get the cookie!
					bucket = overflowBucket;
					
					overflowBucket = entry.getCookie(overflowEntriesCookies[overflowBucket]);

					++numberOfOverflows;
				}

				if (numberOfOverflows == overflowLimit) {
					return;
				}

				overflowEntriesCookies[bucket] = key.recalculateExtra(overflowEntriesCookies[bucket], overflowCount);
				overflowEntriesOffsets[overflowCount] = key.getOffset();
				overflowEntriesHashes[overflowCount] = key.hashCode();
				overflowEntriesCookies[overflowCount] = key.getExtra();
				overflowCount++;
			}
		}
	}

	/**
	 * Enlarge the hash table: make larger and re-hash
	 */
	// TODO: requiring a key to store temporary objects is really odd
	private void growAndRehash(T.Key key) {
		hashSizeStep++;

		int oldHashSize = hashSize;
		hashSize = HASH_SIZE_STEP_TABLE[hashSizeStep];
		rehashLimit = Math.round(hashSize * LOAD_FACTOR);

		int[] oldEntriesOffsets = entriesOffsets;
		int[] oldEntriesHashes = entriesOffsets;
		int[] oldEntriesCookies = entriesOffsets;

		entriesOffsets = new int[hashSize];
		entriesHashes = new int[hashSize];
		entriesCookies = new int[hashSize];

		int[] oldOverflowEntriesOffsets = overflowEntriesOffsets;
		int[] oldOverflowEntriesHashes = overflowEntriesOffsets;
		int[] oldOverflowEntriesCookies = overflowEntriesOffsets;

		overflowEntriesSize = Math.min(hashSize >> 2, entry.getMaxCookieSize());

		overflowEntriesOffsets = new int[overflowEntriesSize];
		overflowEntriesHashes = new int[overflowEntriesSize];
		overflowEntriesCookies = new int[overflowEntriesSize];

		int oldOverflowCount = overflowCount;
		overflowCount = 1;

		for (int i = 0; i < oldHashSize; ++i) {

			key.set(oldEntriesOffsets[i], oldEntriesHashes[i], oldEntriesCookies[i]);

			if (key.isEmpty() == false) {
				// clear PrivateUse
				key.setCookie(0);
				insert(key);
			}
		}

		// overflowEntries[0] does not exist, therefore starting with 1
		for (int i = 1; i < oldOverflowCount; ++i) {
			key.set(oldOverflowEntriesOffsets[i], oldOverflowEntriesHashes[i], oldOverflowEntriesCookies[i]);

			// clear PrivateUse
			key.setCookie(0);
			insert(key);
		}
	}
}
