package io.druid.segment.data;

import com.google.common.io.InputSupplier;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.WritableByteChannel;

public interface GenericWriter<T>{
  public void open() throws IOException;
  public void write(T objectToWrite) throws IOException;
  public long getSerializedSize();
  public InputSupplier<InputStream> combineStreams();
  public void writeToChannel(WritableByteChannel channel) throws IOException;
  public void close() throws IOException;
}
