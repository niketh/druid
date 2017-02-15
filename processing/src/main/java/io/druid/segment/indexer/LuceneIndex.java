package io.druid.segment.indexer;

import io.druid.java.util.common.RE;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.BaseDirectory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class LuceneIndex
{
  private IndexReader indexReader;
  private IndexSearcher indexSearcher;

  public static LuceneIndex read(ByteBuffer buffer)
  {
    return new LuceneIndex(new DruidDirectory(buffer));
  }

  public LuceneIndex(BaseDirectory druidDirectory)
  {
    try {
      this.indexReader = DirectoryReader.open(druidDirectory);
      this.indexSearcher = new IndexSearcher(indexReader);
    } catch (IOException e) {
      throw new RE(String.format("Cannot read LuceneIndex at DruidDirectory Path[%s]", druidDirectory));
    }
  }

  public List<Integer> getLikeMatch(String value)
  {
    List<Integer> matchedDocs = new ArrayList<>();
    if (value == null) {
      value = "";
    }
    try {
      TopDocs topDocs = indexSearcher.search(
          new RegexpQuery(new Term("c", value)), indexReader.numDocs());
      ScoreDoc[] scoreDocs = topDocs.scoreDocs;
      for (int i = 0; i < scoreDocs.length; i++) {
        matchedDocs.add(scoreDocs[i].doc);
      }

    } catch (IOException e) {
      // No docs matched
    }

    return matchedDocs;
  }
}
