/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.storage;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.ratis.util.Preconditions;

/**
 * An {@link ByteReaderStrategy} implementation which supports ByteBuffer as the
 * input read data buffer.
 */
public class ByteBufferReader implements ByteReaderStrategy {
  private final ByteBuffer readBuf;
  private int targetLen;
  private List<ByteBuffer> bufList;

  public ByteBufferReader(ByteBuffer buf) {
    this.readBuf = Objects.requireNonNull(buf, "buf == null");
    this.targetLen = buf.remaining();
  }

  public ByteBufferReader(int len) {
    if (len < 0) {
      throw new IndexOutOfBoundsException();
    }
    this.readBuf = null;
    this.targetLen = len;
    this.bufList = new ArrayList<>();
  }

  ByteBuffer getBuffer() {
    return readBuf;
  }

  List<ByteBuffer> getBufferList() {
    return bufList;
  }

  int readImpl(InputStream inputStream) throws IOException {
    return Preconditions.assertInstanceOf(inputStream, ByteBufferReadable.class)
        .read(readBuf);
  }

  @Override
  public int readFromBlock(InputStream is, int numBytesToRead) throws
      IOException {
    if (readBuf != null) {
      // change buffer limit
      int bufferLimit = readBuf.limit();
      if (numBytesToRead < targetLen) {
        readBuf.limit(readBuf.position() + numBytesToRead);
      }
      int numBytesRead;
      try {
        numBytesRead = readImpl(is);
      } finally {
        // restore buffer limit
        if (numBytesToRead < targetLen) {
          readBuf.limit(bufferLimit);
        }
      }
      targetLen -= numBytesRead;
      return numBytesRead;
    } else if (is instanceof ExtendedInputStream) {
      List<ByteBuffer> list = ((ExtendedInputStream) is).readBytes(numBytesToRead);
      if (list != null) {
        bufList.addAll(list);
        int numBytesRead = list.stream().mapToInt(ByteBuffer::remaining).sum();
        targetLen -= numBytesRead;
        return numBytesRead;
      }
      return 0;
    } else {
      throw new NotImplementedException("readBytes is not implemented for " +
          is.getClass().getSimpleName());
    }
  }

  @Override
  public int getTargetLength() {
    return this.targetLen;
  }
}
