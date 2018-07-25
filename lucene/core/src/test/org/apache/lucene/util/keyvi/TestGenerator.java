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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.LuceneTestCase;

public class TestGenerator extends LuceneTestCase {

  public void testSimpleKeyvi() throws IOException {

    Generator g = new Generator();

    g.add("aaaa");
    g.add("aabb");
    g.add("aabc");
    g.add("aacd");
    g.add("bbcd");

    g.closeFeeding();

    File file = new File("/tmp/t.kv");
    FileOutputStream stream = new FileOutputStream(file);
    g.writeKeyvi(stream);
    g.close();
  }

  public void testRandom() throws IOException {

    Generator g = new Generator();
    for (int i = 0; i < 1000000; ++i) {
      g.add("aa" + String.format("%08d", i));
    }

    g.closeFeeding();
    File file = new File("/tmp/t2.kv");
    FileOutputStream stream = new FileOutputStream(file);
    g.writeKeyvi(stream);
    g.close();
  }
  
  public void testSimple() throws IOException {

    Generator g = new Generator();

    g.add("aaaa");
    g.add("aabb");
    g.add("aabc");
    g.add("aacd");
    g.add("bbcd");

    g.closeFeeding();

    File file = new File("/tmp/t-l.kv");
    FileOutputStream stream = new FileOutputStream(file);
    DataOutput out = new OutputStreamDataOutput(stream);
    
    
    g.write(out);
    g.close();
  }
  
  
}
