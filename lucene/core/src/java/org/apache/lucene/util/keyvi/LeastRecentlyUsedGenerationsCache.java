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

import java.util.LinkedList;
import java.util.List;

public class LeastRecentlyUsedGenerationsCache<T extends MinimizationHashEntry> {
	
	private MinimizationHash<T> currentGeneration;
	
	private List<MinimizationHash<T>> generations = new LinkedList<MinimizationHash<T>>();
	
	private int sizePerGeneration;
	
	private int maxNumberOfGenerations;
	
	private T entryType;
	
	public LeastRecentlyUsedGenerationsCache(T entryType, long memoryLimit) {
		this.entryType = entryType;
		this.currentGeneration = new MinimizationHash<T>(entryType);
		
		MinimizationHash<T>.MemoryLimitConfiguration memoryConfiguration = currentGeneration.findMemoryLimitConfiguration(memoryLimit, 3, 6);
		
		sizePerGeneration = memoryConfiguration.getBestFitMaximumNumbersOfItemsPerTable();
		maxNumberOfGenerations = memoryConfiguration.getBestFitGenerations();
	}
	
	public LeastRecentlyUsedGenerationsCache(T entryType, int sizePerGeneration, int maxNumberOfGenerations) {
		this.entryType = entryType;
		this.sizePerGeneration = sizePerGeneration;
		this.maxNumberOfGenerations = maxNumberOfGenerations;
		this.currentGeneration = new MinimizationHash<T>(entryType);
	}
	
	public void add (T.Key key) {
		if (currentGeneration.size() >= sizePerGeneration) {
			MinimizationHash<T> newGeneration = null;
			
			if (generations.size() + 1 == maxNumberOfGenerations) {
		        // remove(free) the first generation
		        newGeneration = generations.get(0);
		        newGeneration.reset();
		        generations.remove(0);
		      }

		      generations.add(currentGeneration);

		      if (newGeneration == null) {
		        newGeneration = new MinimizationHash<T>(entryType);
		      }

		      currentGeneration = newGeneration;
		}
		
		currentGeneration.add(key);
	}
	
	public boolean get (Object key, T.Key entry) {
		if (currentGeneration.get(key, entry)) {
			return true;
		}
		
		// try to find it in one of the generations
	    for (int i = generations.size(); i > 0; --i) {
	      if (generations.get(i-1).getAndMove(key, currentGeneration, entry)) {
	    	  return true;
	      }
	    }
		
		return false;
	}
	
	public void clear() {
		currentGeneration.clear();
		generations.clear();
	}
	
	public long getMemoryUsage() {
		long memoryUsage = currentGeneration.getMemoryUsage();
		
		for (MinimizationHash<T> generation: this.generations) {
			memoryUsage += generation.getMemoryUsage();
		}
		
		return memoryUsage;
	}
}
