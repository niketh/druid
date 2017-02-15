package io.druid.segment.indexer;

import com.metamx.common.io.smoosh.SmooshedFileMapper;
import org.apache.lucene.store.AccessPackagePrivate;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.SingleInstanceLockFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class DruidDirectory extends BaseDirectory {
  SmooshedFileMapper smooshedFileMapper;
  Map<String, ByteBuffer> bufferMap;

  public DruidDirectory(ByteBuffer bb) {
    super(new SingleInstanceLockFactory());

    int nFiles = bb.getInt();

    bufferMap = new HashMap<>(nFiles);
    String[][] a = new String[nFiles][2];

    int cnt = 0;
    while(cnt < nFiles) {
      byte[] bytes = new byte[bb.getInt()];
      int tmp = 0;
      while (tmp < bytes.length) {
        bytes[tmp] = bb.get();
        tmp++;
      }

      long length = bb.getLong();

      a[cnt][0]=new String(bytes);
      a[cnt][1]=String.valueOf(length);

      cnt++;
    }

    cnt = 0;
    while(cnt < nFiles) {
      ByteBuffer buff = bb.asReadOnlyBuffer();
      int size = Integer.parseInt(a[cnt][1]);
      buff.limit(bb.position() + size);
      bb.position(bb.position() + size);
      bufferMap.put(a[cnt][0], buff);
      cnt++;
    }
  }

  /**
   * Sole constructor.
   *
   * @param lockFactory
   */
  protected DruidDirectory(LockFactory lockFactory) {
    super(lockFactory);
  }

  @Override
  public String[] listAll() throws IOException {
      return bufferMap.keySet().toArray(new String[bufferMap.size()]);
  }

  @Override
  public void deleteFile(String name) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long fileLength(String name) throws IOException {
    return smooshedFileMapper.mapFile(name).remaining();
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void sync(Collection<String> names) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void rename(String source, String dest) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void syncMetaData() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public IndexInput openInput(String name , IOContext context) throws IOException {
      return AccessPackagePrivate.make(name,
          new ByteBuffer[] {bufferMap.get(name).asReadOnlyBuffer() },
          bufferMap.get(name).position(),
          bufferMap.get(name).remaining() , 30,
          AccessPackagePrivate.makeByteBuffer(name));
  }

  @Override
  public void close() throws IOException {
    throw new UnsupportedOperationException();
  }
}
