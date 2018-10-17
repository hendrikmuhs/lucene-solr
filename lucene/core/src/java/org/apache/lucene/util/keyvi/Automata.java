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

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.IndexInput;

public class Automata {

  private final int startState;
  private final int numberOfKeys;
  private final int numberOfStates;
  private final int valueStoreVersion;
  private final int persistenceVersion;
  private final int persistenceSize;
  private final IndexInput in;
  private final long labelsOffset;
  private final long transitionsOffset;

  public Automata(IndexInput in) throws IOException {
    int version = CodecUtil.checkHeader(in, Generator.CODEC_NAME, Generator.VERSION_CURRENT, Generator.VERSION_CURRENT);
    startState = in.readVInt();
    numberOfKeys = in.readVInt();
    numberOfStates = in.readVInt();
    valueStoreVersion = in.readVInt();

    persistenceVersion = in.readVInt();
    persistenceSize = in.readVInt();

    labelsOffset = in.getFilePointer();
    transitionsOffset = labelsOffset + persistenceSize;

    this.in = in;
  }

  public int getStartState() {
    return startState;
  }

  public int getNumberOfKeys() {
    return numberOfKeys;
  }

  public int getNumberOfStates() {
    return numberOfStates;
  }

  public boolean empty() {
    return numberOfKeys == 0;
  }

  public int tryWalkTransition(int startingState, byte c) {
    try {
      in.seek(labelsOffset + startingState + (c & 0xff));
      if (in.readByte() == c) {
        return resolvePointer(startingState, c);
      }
    } catch (IOException e) {
      return 0;
    }
    return 0;
  }

  public boolean isFinalState(int stateToCheck) {
    try {
      in.seek(labelsOffset + stateToCheck + KeyviConstants.FINAL_OFFSET_TRANSITION);
      if (in.readByte() == KeyviConstants.FINAL_OFFSET_CODE) {
        return true;
      }
    } catch (IOException e) {
      return false;
    }

    return false;
  }

  public int getStateValue(int state) {
    try {
      // todo: *2??
      in.seek(transitionsOffset + state + KeyviConstants.FINAL_OFFSET_TRANSITION);
    } catch (IOException e) {
      return 0;
    }

    return 0;
    // return keyvi::util::decodeVarshort(transitions_compact_ + state + FINAL_OFFSET_TRANSITION);
  }

  private int resolvePointer(int state, byte c) throws IOException {
    in.seek(transitionsOffset + 2 * (state + (c & 0xff)));

    // read it little endian order
    short pt = (short) ((in.readByte() & 0xFF) | ((in.readByte() & 0xFF) <<  8) );
    
    if ((pt & 0xC000) == 0xC000) {
      return pt & 0x3FFF;
    } else if ((pt & 0x8000) > 0) {

      // clear the first bit
      pt &= 0x7FFF;
      int overflowBucket;

      overflowBucket = (pt >>> 4) + state + c - 512;

      // todo: implement overflow handling
      
      
      // resolved_ptr = keyvi::util::decodeVarshort(transitions_compact_ + overflow_bucket);
      // resolved_ptr = (resolved_ptr << 3) + (pt & 0x7);
      /*
       * if (pt & 0x8) { // relative coding resolved_ptr = (starting_state + c) - resolved_ptr + 512; }
       */
      return 0;
    }

    
    
    return state + (c & 0xff) - pt + 512;
  }

}
