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

public final class UnpackedState {
  public final class Transition {
    private int label;
    private int value;

    public Transition() {}

    public void set(int label, int value) {
      this.label = label;
      this.value = value;
    }

    public int getValue() {
      return value;
    }

    public void setValue(int value) {
      this.value = value;
    }

    public int getLabel() {
      return label;
    }

    public void setLabel(int label) {
      this.label = label;
    }
  }

  private SparseArrayPersistence persistence;
  private Transition[] outgoing;
  private BitVector bitVector = new BitVector(KeyviConstants.MAX_TRANSITIONS_OF_A_STATE);
  private int used = 0;
  private int hashCode = -1;
  private int noMinimizationCounter = 0;
  private int weight = 0;
  private int zeroByteState = 0;
  private byte zeroByteLabel = (byte) 0xff;
  private boolean finalState = false;

  public UnpackedState(SparseArrayPersistence persistence) {
    this.persistence = persistence;

    outgoing = new Transition[KeyviConstants.MAX_TRANSITIONS_OF_A_STATE];
    for (int i = 0; i < outgoing.length; i++) {
      outgoing[i] = new Transition();
    }
  }

  public boolean isFinal() {
    return finalState;
  }

  public void add(int transitionLabel, int transitionValue) {
    outgoing[used++].set(transitionLabel, transitionValue);
    bitVector.set(transitionLabel);
  }

  public boolean hasLabel(int transitionLabel) {
    return bitVector.get(transitionLabel);
  }

  public void addFinalState(int transitionValue) {
    outgoing[used++].set(KeyviConstants.FINAL_OFFSET_TRANSITION, transitionValue);

    long vShortSize = VInt.getVarShortLength(transitionValue);
    for (int i = 0; i < vShortSize; ++i) {
      bitVector.set(KeyviConstants.FINAL_OFFSET_TRANSITION + i);
    }

    finalState = true;
  }

  public void setTransitionValue(int transitionValue) {
    outgoing[used - 1].setValue(transitionValue);
  }

  public void clear() {
    used = 0;
    hashCode = -1;
    bitVector.clear();
    noMinimizationCounter = 0;
    weight = 0;
    zeroByteState = 0;
    zeroByteLabel = (byte) 0xff;
    finalState = false;
  }

  public BitVector getBitVector() {
    return bitVector;
  }

  public int size() {
    return used;
  }

  public void incrementNoMinimizationCounter() {
    noMinimizationCounter++;
  }

  public void incrementNoMinimizationCounter(int counter) {
    noMinimizationCounter += counter;
  }

  public int getNoMinimizationCounter() {
    return noMinimizationCounter;
  }

  public void updateWeightIfHigher(int weight) {
    if (weight > this.weight) {
      this.weight = weight;
      bitVector.set(KeyviConstants.INNER_WEIGHT_TRANSITION_COMPACT);
    }
  }

  public int getWeight() {
    return weight;
  }

  public void setZeroByteState(int position) {
    this.zeroByteState = position;
  }

  public int getZeroByteState() {
    return zeroByteState;
  }

  public void setZeroByteLabel(byte label) {
    this.zeroByteLabel = label;
  }

  public byte getZeroByteLabel() {
    return zeroByteLabel;
  }

  public int hashCode() {
    if (this.hashCode == -1) {
      int b;
      int a = b = 0x9e3779b9;
      int c = weight > 0 ? 1 : 0;
      int sz = used;
      for (int i = 0; i < sz; ++i) {
        Transition t = outgoing[i];
        a += t.label;
        b += t.value;
        if (i < sz - 1) {
          ++i;
          t = outgoing[i];
          a += t.label << 16;
          b += t.value << 16;
        }

        // good old Bob Jenkins Hash
        a -= b;
        a -= c;
        a ^= c >>> 13;
        b -= c;
        b -= a;
        b ^= a << 8;
        c -= a;
        c -= b;
        c ^= b >>> 13;
        a -= b;
        a -= c;
        a ^= c >>> 12;
        b -= c;
        b -= a;
        b ^= a << 16;
        c -= a;
        c -= b;
        c ^= b >>> 5;
        a -= b;
        a -= c;
        a ^= c >>> 3;
        b -= c;
        b -= a;
        b ^= a << 10;
        c -= a;
        c -= b;
        c ^= b >>> 15;
      }

      this.hashCode = c;
    }

    return this.hashCode;
  }

  public boolean equals(PackedState.Key packed) {
    // First filter - check if hash code and the number of transitions is the same
    if (packed.hashCode() != hashCode()
        || packed.getNumberOfOutgoingTransitions() != used) {
      return false;
    }

    // The number of transitions is the same. Verify that they also look the same.
    for (int i = 0; i < used; ++i) {
      Transition t = outgoing[i];
      int label = t.label;

      if (label < KeyviConstants.FINAL_OFFSET_TRANSITION) {
        // Is there a transition of this kind?
        if (persistence.readTransitionLabel(packed.getOffset() + label) != label) {
          return false;
        }

        // Does this transition lead to the same target state?
        int target = persistence.readTransitionValue(packed.getOffset() + label);
        target = persistence.resolveTransitionValue(packed.getOffset() + label, target);
        if (t.value != target) {
          return false;
        }
      } else // (label == FINAL_OFFSET_TRANSITION)
      {
        if (persistence.readTransitionLabel(packed.getOffset() + label) != KeyviConstants.FINAL_OFFSET_CODE) {
          return false;
        }

        // check if l has final info
        int value = persistence.readFinalValue(packed.getOffset());

        if (t.value != value) {
          return false;
        }
      }
      //// This transition is ok.
    }

    // note: we do not compare the weight entry because if one state has weight and the other not the hashes differ

    // all checks succeeded, states must be equal.
    return true;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }

    if (obj.getClass() == PackedState.Key.class) {
      return this.equals((PackedState.Key) obj);
    }

    if (getClass() != obj.getClass()) {
      return false;
    }

    UnpackedState other = (UnpackedState) obj;

    if ((this.persistence == other.persistence &&
        this.bitVector == other.bitVector
        && this.used == other.used
        && this.noMinimizationCounter == other.noMinimizationCounter
        && this.weight == other.weight
        && this.zeroByteState == other.zeroByteState
        && this.zeroByteLabel == other.zeroByteLabel
        && this.finalState == other.finalState
        && this.outgoing.length == other.outgoing.length) == false) {
      return false;
    }

    for (int i = 0; i < this.used; ++i) {
      if (this.outgoing[i] != other.outgoing[i]) {
        return false;
      }
    }

    return true;
  }

  public Transition get(int position) {
    return outgoing[position];
  }
}
