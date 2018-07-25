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

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.DataOutput;

import static java.nio.charset.StandardCharsets.*;

public class Generator implements Closeable {

  public static final String CODEC_NAME = "KEYVI";
  public static final int VERSION_CURRENT = 2;
  SparseArrayPersistence persistence;
  SparseArrayBuilder sparseArrayBuilder;
  UnpackedStateStack unpackedStateStack;
  byte[] lastKey;
  int highestStack = 0;
  int numberOfKeysAdded = 0;
  int startState = 0;
  int numberOfStates = 0;
  boolean minimize;

  public Generator() throws IOException {

    int memoryLimit = 1000000;
    Path temporaryDirectory = Files.createTempDirectory("keyvi-generator");
    persistence = new SparseArrayPersistence(memoryLimit, temporaryDirectory);
    sparseArrayBuilder = new SparseArrayBuilder(memoryLimit, persistence, false, true);
    unpackedStateStack = new UnpackedStateStack(persistence, 30, 30);

    lastKey = new byte[0];
  }

  @Override
  public void close() throws IOException {
    persistence.close();
  }

  public void add(String key) {

    byte[] utf8Key = key.getBytes(UTF_8);

    int commonPrefixLength = getCommonPrefixLength(utf8Key, lastKey);

    // check which stack can be consumed (packed into the sparse array)
    consumeStack(commonPrefixLength);

    // put everything that is not common between the two strings (the suffix)
    // into the stack
    feedStack(commonPrefixLength, utf8Key);

    // get value and mark final state
    boolean noMinimization = false;
    int valueIndex = 0; // value_store_->GetValue(value, &no_minimization);

    unpackedStateStack.insertFinalState(utf8Key.length, valueIndex, noMinimization);

    // count number of entries
    ++numberOfKeysAdded;

    // if inner weights are used update them
    int weight = 0; // value_store_->GetWeightValue(value);
    if (weight > 0) {
      unpackedStateStack.updateWeights(0, utf8Key.length, weight);
    }

    lastKey = utf8Key;
  }

  public void closeFeeding() {
    // Consume all but stack[0].
    consumeStack(0);

    // handling of last State.
    UnpackedState unpackedState = unpackedStateStack.get(0);

    startState = (int) sparseArrayBuilder.persistState(unpackedState);

    // free structures that are not needed anymore
    unpackedStateStack = null;

    numberOfStates = (int) sparseArrayBuilder.getNumberOfStates();
    sparseArrayBuilder = null;

    persistence.flush();
  }

  public void write(DataOutput out) throws IOException {
    CodecUtil.writeHeader(out, CODEC_NAME, VERSION_CURRENT);
    out.writeVInt(startState);
    out.writeVInt(numberOfKeysAdded);
    out.writeVInt(numberOfStates);
    // value store type
    out.writeVInt(1);
    
    persistence.write(out);
  }

  public void writeKeyvi(OutputStream stream) throws IOException {
    stream.write(KeyviConstants.KEYVI_FILE_MAGIC);
    writeHeader(stream);

    persistence.writeKeyvi(stream);
  }

  private void writeHeader(OutputStream stream) throws IOException {

    String properties = "{\"version\":\"" + 2 + "\", \"start_state\":\"" + startState + "\", \"number_of_keys\":\""
        + numberOfKeysAdded + "\", \"value_store_type\":\"" + 1 + "\", \"number_of_states\":\"" + numberOfStates
        + "\"}";

    byte[] propertiesBytes = properties.getBytes(UTF_8);

    // todo: re-factor, writing 32bit int

    stream.write((propertiesBytes.length >>> 24) & 0xFF);
    stream.write((propertiesBytes.length >>> 16) & 0xFF);
    stream.write((propertiesBytes.length >>> 8) & 0xFF);
    stream.write(propertiesBytes.length & 0xFF);

    stream.write(propertiesBytes);

    /*
     * pt.put("version", "2"); pt.put("start_state", std::to_string(start_state_)); pt.put("number_of_keys",
     * std::to_string(number_of_keys_added_)); pt.put("value_store_type",
     * std::to_string(value_store_->GetValueStoreType())); pt.put("number_of_states",
     * std::to_string(number_of_states_)); pt.add_child("manifest", manifest_);
     * 
     * keyvi::util::SerializationUtils::WriteJsonRecord(stream, pt);
     */
  }

  private int getCommonPrefixLength(byte[] first, byte[] second) {
    int commonPrefixLength = 0;

    while (commonPrefixLength < first.length && commonPrefixLength < second.length
        && first[commonPrefixLength] == second[commonPrefixLength]) {
      ++commonPrefixLength;
    }
    return commonPrefixLength;
  }

  private void feedStack(int start, byte[] key) {

    for (int i = start; i < key.length; ++i) {
      unpackedStateStack.insert(i, key[i], 0);
    }

    // remember highest stack
    if (key.length > highestStack) {
      highestStack = key.length;
    }
  }

  private void consumeStack(int end) {
    while (highestStack > end) {
      // Get outgoing transitions from the stack.
      UnpackedState unpackedState = unpackedStateStack.get(highestStack);

      int transistionPointer = (int) sparseArrayBuilder.persistState(unpackedState);

      // Save transition_pointer in previous stack, indicate whether it makes
      // sense continuing minimization
      unpackedStateStack.pushTransitionPointer(highestStack - 1, transistionPointer,
          unpackedState.getNoMinimizationCounter());

      // Delete state
      unpackedStateStack.Erase(highestStack);

      --highestStack;
    }
  }

}
