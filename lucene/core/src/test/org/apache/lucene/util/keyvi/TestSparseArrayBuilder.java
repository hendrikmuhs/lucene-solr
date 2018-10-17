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
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.lucene.util.LuceneTestCase;

public class TestSparseArrayBuilder extends LuceneTestCase {

  public void testWriteFinalStateCompact() throws IOException {
    Path temporaryDirectory = Files.createTempDirectory("dictionary-fsa-unittest");
    SparseArrayPersistence p = new SparseArrayPersistence(16000, temporaryDirectory);

    SparseArrayBuilder b = new SparseArrayBuilder(1024 * 1024, p, false, true);

    b.writeFinalTransition(25, 55);
    assertEquals(55, p.readFinalValue(25));

    b.writeFinalTransition(42, 0);
    assertEquals(0, p.readFinalValue(42));

    b.writeFinalTransition(2048, 23);
    assertEquals(23, p.readFinalValue(2048));
  }

  public void testWriteTransitionAbsoluteMaxValue() throws IOException {
    Path temporaryDirectory = Files.createTempDirectory("dictionary-fsa-unittest");
    SparseArrayPersistence p = new SparseArrayPersistence(64000, temporaryDirectory);

    SparseArrayBuilder b = new SparseArrayBuilder(1024 * 1024, p, false, true);

    // simulate that sparse array builder got tons of states
    b.highestPersistedState = 1024 * 1024;

    // write a state with a large offset but a low pointer
    p.beginNewState(1000000 - 65);
    b.writeTransition(1000000, (byte) 65, 20);
    b.takenPositionsInSparsearray.set(1000000);
    assertEquals(65, p.readTransitionLabel(1000000));
    assertEquals(20, p.resolveTransitionValue(1000000, p.readTransitionValue(1000000)));
  }

  public void testWriteTransitionRelativeOverflow() throws IOException {
    Path temporaryDirectory = Files.createTempDirectory("dictionary-fsa-unittest");
    SparseArrayPersistence p = new SparseArrayPersistence(64000, temporaryDirectory);

    SparseArrayBuilder b = new SparseArrayBuilder(1024 * 1024, p, false, true);

    // simulate that sparse array builder got tons of states
    b.highestPersistedState = 1024 * 1024;

    // write a state with a large offset and a low pointer > short
    p.beginNewState(1000001 - 65);
    b.writeTransition(1000001, (byte) 65, 34000);
    b.takenPositionsInSparsearray.set(1000001);
    assertEquals(65, p.readTransitionLabel(1000001));
    assertEquals(34000, p.resolveTransitionValue(1000001, p.readTransitionValue(1000001)));
  }

  public void testWriteTransitionRelativeOverflowZerobyteGhostState() throws IOException {
    Path temporaryDirectory = Files.createTempDirectory("dictionary-fsa-unittest");
    SparseArrayPersistence p = new SparseArrayPersistence(64000, temporaryDirectory);

    SparseArrayBuilder b = new SparseArrayBuilder(1024 * 1024, p, false, true);

    // write 1 state, starting at position 0
    UnpackedState u1 = new UnpackedState(p);
    u1.add(65, 100);
    u1.add(66, 101);
    u1.add(233, 102);

    b.writeState(0, u1);

    // it should be allowed to put something at position 255
    assertFalse(b.stateStartPositions.isSet(0xff));
    assertEquals(0, p.readTransitionLabel(0xff));
    // 2nd state, at position 255
    UnpackedState u2 = new UnpackedState(p);
    u2.add(65, 100);
    u2.add(66, 101);
    u2.add(233, 102);
    for (int i = 1; i < 255 + 65; ++i) {
      // mark transitions
      if (i == 255) {
        continue;
      }
      b.takenPositionsInSparsearray.set(i);
    }

    assertEquals(255, b.findFreeBucket(u2));
    b.writeState(0xff, u2);

    // 0 + 255 -> 255 should not exist as it would mean u1 has a transition 255
    assertEquals((byte) 0xfe, p.readTransitionLabel(255));
  }

  public void testWriteTransitionRelativeOverflowZerobyte() throws IOException {
    Path temporaryDirectory = Files.createTempDirectory("dictionary-fsa-unittest");
    SparseArrayPersistence p = new SparseArrayPersistence(64000, temporaryDirectory);

    SparseArrayBuilder b = new SparseArrayBuilder(1024 * 1024, p, false, true);

    // simulate that sparse array builder got tons of states
    b.highestPersistedState = 1024 * 1024;

    p.beginNewState(999999 - 67);
    assertEquals(0, p.readTransitionLabel(1000000));
    assertEquals(0, p.readTransitionLabel(1000001));

    b.writeTransition(999999, (byte) 67, 21);
    b.takenPositionsInSparsearray.set(999999);
    b.writeTransition(1000002, (byte) 70, 22);
    b.takenPositionsInSparsearray.set(1000002);

    assertEquals(0, p.readTransitionLabel(1000000));
    assertEquals(0, p.readTransitionLabel(1000001));

    // write a state with a large offset and a low pointer > short
    p.beginNewState(1000512 - 65);
    b.writeTransition(1000512, (byte) 65, 333333);
    assertEquals(p.readTransitionLabel(1000512), 65);
    assertEquals(p.resolveTransitionValue(1000512, p.readTransitionValue(1000512)), 333333);

    assertTrue(p.readTransitionLabel(1000000) != 0);
    assertTrue(p.readTransitionLabel(1000001) != 0);
  }

  public void testWriteTransitionRelativeOverflowZerobyte2() throws IOException {
    Path temporaryDirectory = Files.createTempDirectory("dictionary-fsa-unittest");
    SparseArrayPersistence p = new SparseArrayPersistence(64000, temporaryDirectory);

    SparseArrayBuilder b = new SparseArrayBuilder(1024 * 1024, p, false, true);

    // simulate that sparse array builder got tons of states
    b.highestPersistedState = 1024 * 1024;

    p.beginNewState(1000000);

    // write a valid zero byte state
    b.writeTransition(1000000, (byte) 0, 21);
    b.takenPositionsInSparsearray.set(1000000);
    b.writeTransition(1000003, (byte) 70, 22);
    b.takenPositionsInSparsearray.set(1000003);

    assertEquals(0, p.readTransitionLabel(1000000));
    assertEquals(0, p.readTransitionLabel(1000001));

    // write a state with a large offset and a low pointer > short
    p.beginNewState(1000512 - 65);
    b.writeTransition(1000512, (byte) 65, 333334);
    b.takenPositionsInSparsearray.set(1000512);
    assertEquals(65, p.readTransitionLabel(1000512));
    assertEquals(333334, p.resolveTransitionValue(1000512, p.readTransitionValue(1000512)));

    // the zero byte state should not be overwritten
    assertTrue(p.readTransitionLabel(1000000) == 0);
    assertTrue(p.readTransitionLabel(1000001) != 0);
    assertTrue(p.readTransitionLabel(1000002) != 0);
    assertEquals(70, p.readTransitionLabel(1000003));
  }

  public void testWriteTransitionRelativeOverflowZerobyte3() throws IOException {
    Path temporaryDirectory = Files.createTempDirectory("dictionary-fsa-unittest");
    SparseArrayPersistence p = new SparseArrayPersistence(64000, temporaryDirectory);

    SparseArrayBuilder b = new SparseArrayBuilder(1024 * 1024, p, false, true);

    // simulate that sparse array builder got tons of states
    b.highestPersistedState = 1024 * 1024;

    p.beginNewState(1000000);

    // mark some state beginnings that could lead to zombie states
    b.stateStartPositions.set(1000001 - 0xff);
    b.stateStartPositions.set(1000001 - 0xfe);
    b.stateStartPositions.set(1000001 - 0xfd);
    b.stateStartPositions.set(1000001 - 0xfc);
    b.stateStartPositions.set(1000001 - 0xfb);
    b.stateStartPositions.set(1000001 - 0xfa);

    // write a valid zero byte state
    b.writeTransition(1000000, (byte) 0, 21);
    b.takenPositionsInSparsearray.set(1000000);
    b.writeTransition(1000003, (byte) 70, 22);
    b.takenPositionsInSparsearray.set(1000003);

    assertEquals(0, p.readTransitionLabel(1000000));
    assertEquals(0, p.readTransitionLabel(1000001));

    // write a state with a large offset and a low pointer > short
    p.beginNewState(1000512 - 65);
    b.writeTransition(1000512, (byte) 65, 333335);
    b.takenPositionsInSparsearray.set(1000512);

    assertEquals(65, p.readTransitionLabel(1000512), 65);
    assertEquals(333335, p.resolveTransitionValue(1000512, p.readTransitionValue(1000512)));

    // the zero byte state should not be overwritten
    assertEquals(0, p.readTransitionLabel(1000000));
    assertTrue(p.readTransitionLabel(1000001) != 0);
    assertEquals((byte) 0xf9, p.readTransitionLabel(1000001));
    assertTrue(p.readTransitionLabel(1000002) != 0);
    assertEquals(70, p.readTransitionLabel(1000003));
  }

  public void testWriteTransitionRelativeOverflowZerobyteEdgecase() throws IOException {
    Path temporaryDirectory = Files.createTempDirectory("dictionary-fsa-unittest");
    SparseArrayPersistence p = new SparseArrayPersistence(64000, temporaryDirectory);

    SparseArrayBuilder b = new SparseArrayBuilder(1024 * 1024, p, false, true);

    // simulate that sparse array builder got tons of states
    b.highestPersistedState = 1024 * 1024;

    p.beginNewState(1000000);

    for (int i = 0xff; i > 1; i--) {
      // mark some state beginnings that could lead to zombie states
      b.stateStartPositions.set(1000001 - i);
    }

    // write a valid zero byte state
    b.writeTransition(1000000, (byte) 0, 21);
    b.takenPositionsInSparsearray.set(1000000);
    b.writeTransition(1000003, (byte) 70, 22);
    b.takenPositionsInSparsearray.set(1000003);

    assertEquals(0, p.readTransitionLabel(1000000));
    assertEquals(0, p.readTransitionLabel(1000001));

    // write a state with a large offset and a low pointer > short
    p.beginNewState(1000512 - 65);
    b.writeTransition(1000512, (byte) 65, 333336);
    b.takenPositionsInSparsearray.set(1000512);

    assertEquals(p.readTransitionLabel(1000512), 65);
    assertEquals(333336, p.resolveTransitionValue(1000512, p.readTransitionValue(1000512)));

    // the zero byte state should not be overwritten
    assertEquals(0, p.readTransitionLabel(1000000));

    assertTrue(b.stateStartPositions.isSet(1000001 - 0xff));

    // if 1000002 has label 1 we would have a wrong final state
    assertTrue(p.readTransitionLabel(1000002) != 1);

    assertEquals(70, p.readTransitionLabel(1000003));
    assertTrue(p.readTransitionLabel(1000004) != 0);
    assertTrue(p.readTransitionLabel(1000005) != 0);
  }

  public void testWriteTransitionRelativeOverflowZerobyteEdgecaseStartPositions() throws IOException {
    Path temporaryDirectory = Files.createTempDirectory("dictionary-fsa-unittest");
    SparseArrayPersistence p = new SparseArrayPersistence(64000, temporaryDirectory);

    SparseArrayBuilder b = new SparseArrayBuilder(1024 * 1024, p, false, true);

    // simulate that sparse array builder got tons of states
    b.highestPersistedState = 1024 * 1024;

    p.beginNewState(1000000);

    for (int i = 0; i < 1000; ++i) {
      // mark some state beginnings that could lead to zombie states
      b.stateStartPositions.set(1000000 + i);

      // fill the labels, just for the purpose of checking it later
      b.writeTransition(1000000 + i, (byte) 70, 21);
    }

    // write a state with a large offset and a large pointer that does not fit in a
    // short and requires overflow
    p.beginNewState(1001000 - 65);
    b.writeTransition(1001000, (byte) 65, 333336);
    b.takenPositionsInSparsearray.set(1001000);

    assertEquals(65, p.readTransitionLabel(1001000));
    assertEquals(333336, p.resolveTransitionValue(1001000, p.readTransitionValue(1001000)));

    for (int i = 0; i < 1000; ++i) {
      assertEquals((byte) 70, p.readTransitionLabel(1000000 + i));
    }
  }

  public void testWriteTransitionZerobyteWeightCase() throws IOException {
    Path temporaryDirectory = Files.createTempDirectory("dictionary-fsa-unittest");
    SparseArrayPersistence p = new SparseArrayPersistence(64000, temporaryDirectory);

    SparseArrayBuilder b = new SparseArrayBuilder(1024 * 1024, p, false, true);

    // simulate that sparse array builder got tons of states
    b.highestPersistedState = 1024 * 1024;

    p.beginNewState(1000000);

    // write a weight state
    b.updateWeightIfNeeded(1000000, 42);
    b.takenPositionsInSparsearray.set(1000000 + KeyviConstants.INNER_WEIGHT_TRANSITION_COMPACT);

    assertEquals(0, p.readTransitionLabel(1000000 + KeyviConstants.INNER_WEIGHT_TRANSITION_COMPACT));
    assertTrue(b.stateStartPositions.isSet(1000000 + KeyviConstants.INNER_WEIGHT_TRANSITION_COMPACT));
  }

  public void testWriteTransitionFinalStateTransition() throws IOException {
    Path temporaryDirectory = Files.createTempDirectory("dictionary-fsa-unittest");
    SparseArrayPersistence p = new SparseArrayPersistence(64000, temporaryDirectory);

    SparseArrayBuilder b = new SparseArrayBuilder(1024 * 1024, p, false, true);

    // simulate that sparse array builder got tons of states
    b.highestPersistedState = 1024 * 1024;

    p.beginNewState(1000000);

    // write a final state which requires an overflow to the next cell
    b.writeFinalTransition(1000000, 1000000);
    b.takenPositionsInSparsearray.set(1000000 + KeyviConstants.FINAL_OFFSET_TRANSITION);
    b.takenPositionsInSparsearray.set(1000000 + KeyviConstants.FINAL_OFFSET_TRANSITION + 1);

    b.writeTransition(1000003, (byte) 70, 22);
    b.takenPositionsInSparsearray.set(1000003);

    assertEquals(1, p.readTransitionLabel(1000000 + KeyviConstants.FINAL_OFFSET_TRANSITION));
    assertEquals(2, p.readTransitionLabel(1000000 + KeyviConstants.FINAL_OFFSET_TRANSITION + 1));
  }

  public void testWriteTransitionExternalMemory() throws IOException {
    Path temporaryDirectory = Files.createTempDirectory("dictionary-fsa-unittest");
    int memoryLimitPersistence = 64000;
    SparseArrayPersistence p = new SparseArrayPersistence(memoryLimitPersistence, temporaryDirectory);

    SparseArrayBuilder b = new SparseArrayBuilder(1024 * 1024, p, false, true);

    // simulate that sparse array builder got tons of states
    b.highestPersistedState = 1024 * 1024;

    int chunkSize = p.GetChunkSizeExternalTransitions();
    int factor = (1024 * 1024) / memoryLimitPersistence;
    int offset = (factor * chunkSize) - 2;
    p.beginNewState(offset - 100);
    // write a transition on the chunk border with a overflowing transition
    b.writeTransition(offset - 20, (byte) 20, offset - 80000);
    b.takenPositionsInSparsearray.set(offset - 20);

    short value = p.readTransitionValue(offset - 20);
    assertTrue(value != 0);
    assertEquals(offset - 80000, p.resolveTransitionValue(offset - 20, value));

    // trigger a flush to external memory
    p.beginNewState(chunkSize * (factor + 2));
    short value2 = p.readTransitionValue(offset - 20);
    assertTrue(value != 0);
    assertEquals(value, value2);
    assertEquals(offset - 80000, p.resolveTransitionValue(offset - 20, value2));
  }

  public void testWriteTransitionChunkBorder() throws IOException {
    Path temporaryDirectory = Files.createTempDirectory("dictionary-fsa-unittest");
    int memoryLimitPersistence = 64000;
    SparseArrayPersistence p = new SparseArrayPersistence(memoryLimitPersistence, temporaryDirectory);

    SparseArrayBuilder b = new SparseArrayBuilder(1024 * 1024, p, false, true);

    // simulate that sparse array builder got tons of states
    b.highestPersistedState = 1024 * 1024;
    int chunkSize = p.GetChunkSizeExternalTransitions();
    int factor = (1024 * 1024) / memoryLimitPersistence;
    int offset = (factor * chunkSize) - 2;

    // mark slots taken in sparse array to force writing on chunk border
    for (int i = offset - KeyviConstants.COMPACT_SIZE_WINDOW - 10; i <= offset - 1; ++i) {
      b.takenPositionsInSparsearray.set(i);
    }

    p.beginNewState(offset - 5);

    // write a transition on the chunk border with a overflowing transition
    b.writeTransition(offset - 3, (byte) 5, offset - 80000);
    b.takenPositionsInSparsearray.set(offset - 3);

    // force flushing buffers
    p.beginNewState(chunkSize * (factor + 2));

    short value = p.readTransitionValue(offset - 3);
    assertEquals(offset - 80000, p.resolveTransitionValue(offset - 3, value));
  }

}
