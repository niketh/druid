package org.apache.lucene.store;

import java.nio.ByteBuffer;

public class AccessPackagePrivate {
  public static IndexInput make(String resourceDescription, ByteBuffer[] buffers, int offset, long length, int chunkSizePower, ByteBufferGuard guard) {
    return new ByteBufferIndexInput.MultiBufferImpl(resourceDescription, buffers, offset, length, chunkSizePower, guard);
  }

  public static ByteBufferGuard makeByteBuffer(String resourceDescription){
    return new ByteBufferGuard(resourceDescription, null);
  }
}
