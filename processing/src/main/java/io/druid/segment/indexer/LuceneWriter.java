package io.druid.segment.indexer;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.io.ByteStreams;
import com.google.common.io.InputSupplier;
import io.druid.segment.data.GenericWriter;
import io.druid.segment.data.IOPeon;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.MMapDirectory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LuceneWriter implements GenericWriter<String> {
  private MMapDirectory idx;
  private IndexWriter writer;
  private final Path filePath;
  private final IOPeon ioPeon;
  private final Map<String, String> fileMapping;
  private final String dimensionName;
  private static final String FIELD_NAME = "c";

  public LuceneWriter(IOPeon ioPeon, String filenameBase, String dimensionName) {
    this.filePath = Paths.get(filenameBase);
    this.ioPeon = ioPeon;
    this.dimensionName = dimensionName;
    this.fileMapping = new HashMap<>();
  }

  private Document createDocument(String content) {
    Document doc = new Document();
    doc.add(new StringField(FIELD_NAME, content, Field.Store.NO));
    return doc;
  }

  public void addFilesToIOPeon(String prefix) throws IOException
  {
    //add lucene files to IOPeon
    for(String filename : idx.listAll()){
      ioPeon.addFile(prefix + "_" + filename, idx.getDirectory().resolve(filename));
      fileMapping.put(filename, prefix + "_" + filename);
    }
  }

  @Override
  public void open() throws IOException
  {
    idx = new MMapDirectory(filePath);
    writer = new IndexWriter(idx, new IndexWriterConfig(new StandardAnalyzer(CharArraySet.EMPTY_SET)));
  }

  @Override
  public void close() throws IOException {
    writer.commit();
    writer.close();

    addFilesToIOPeon(dimensionName);
  }

  @Override
  public InputSupplier<InputStream> combineStreams() {
    throw new UnsupportedOperationException("");
  }

  private InputSupplier<InputStream> combineStreams(List<String> filenames) {
    return ByteStreams.join(
            Iterables.transform(
                    filenames,
                    new Function<String, InputSupplier<InputStream>>() {
                      @Override
                      public InputSupplier<InputStream> apply(final String input) {
                        return new InputSupplier<InputStream>() {
                          @Override
                          public InputStream getInput() throws IOException {
                            return ioPeon.makeInputStream(fileMapping.get(input));
                          }
                        };
                      }
                    }
            )
    );
  }

  @Override
  public void writeToChannel(WritableByteChannel channel) throws IOException {
    List<String> filePaths = new ArrayList<String>();

    /* | Num of files | Length of file name | File name | File length | contents | */
    ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
    buffer.putInt(idx.listAll().length);
    buffer.rewind();
    channel.write(buffer);

    for(String filename : idx.listAll()){
      filePaths.add(filename);
      ByteBuffer buff = ByteBuffer.allocate(Integer.BYTES + Long.BYTES + filename.getBytes().length*Byte.BYTES);
      buff.putInt(filename.length());
      buff.put(filename.getBytes());
      buff.putLong(Files.size(Paths.get(idx.getDirectory().toString(),filename)));
      buff.rewind();
      channel.write(buff);
    }

    final ReadableByteChannel from = Channels.newChannel(combineStreams(filePaths).getInput());
    ByteStreams.copy(from, channel);
  }

  @Override
  public void write(String objectToWrite) throws IOException {
    writer.addDocument(createDocument((objectToWrite != null) ? objectToWrite : ""));
  }

  @Override
  public long getSerializedSize() {
    int size = 0;
    size += Integer.BYTES; // No of files

    try {
      for(String filename : idx.listAll()){
        size += Integer.BYTES;  // length of filename
        size += filename.getBytes().length*Byte.BYTES;  // filename
        size += Long.BYTES;     // length of content
        size += Files.size(Paths.get(idx.getDirectory().toString(),filename));  // contents
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    return size;
  }
}
