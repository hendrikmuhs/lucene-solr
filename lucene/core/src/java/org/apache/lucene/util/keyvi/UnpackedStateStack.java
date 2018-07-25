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

import java.util.ArrayList;
import java.util.List;

public final class UnpackedStateStack {
	private List<UnpackedState> unpackedStatePool;
	private SparseArrayPersistence persistence;
	private int weightCutOff;
	
	public UnpackedStateStack(SparseArrayPersistence persistence, int initialSize, int weightCutOff) {
		this.persistence = persistence;
		this.weightCutOff = weightCutOff;
		unpackedStatePool = new ArrayList<UnpackedState>(initialSize);
	}
	
	public UnpackedState get(int position) {
		while (position >= unpackedStatePool.size()) {
			unpackedStatePool.add(new UnpackedState(persistence));
	      }
	      return unpackedStatePool.get(position);
	}
	
	public void insert(int position, int transitionLabel, int transitionValue) {
		UnpackedState state = get(position);
		state.add(transitionLabel, transitionValue);
	}
	
	public void insertFinalState(int position, int transitionValue, boolean noMinimization) {
		UnpackedState state = get(position);
		state.addFinalState(transitionValue);
	      
		// only do it explicit if no_minimization is true, it could be that the state is already marked, so we should
	    // not overwrite it.
		if (noMinimization) {
			state.incrementNoMinimizationCounter();
		}
	}
	
	public void updateWeights(int start, int end, int weight) {
		if (start > weightCutOff) {
			return;
		}
		
		if (end > weightCutOff) {
	        end = weightCutOff;
	    }
		
		for (int i = start; i<end; ++i) {
			UnpackedState state = get(i);
			state.updateWeightIfHigher(weight);
		}
	}

	public void pushTransitionPointer(int position, int transitionValue, int minimizationCounter) {
		UnpackedState state = get(position);
		state.setTransitionValue(transitionValue);
		state.incrementNoMinimizationCounter(minimizationCounter);
	}
	
	public void Erase(int position) {
		get(position).clear();
	}
}
