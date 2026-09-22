package com.google.android.exoplayer2.ext.okhttp;

import android.net.Uri;
import com.google.android.exoplayer2.C;
import com.google.android.exoplayer2.upstream.BaseDataSource;
import com.google.android.exoplayer2.upstream.DataSource;
import com.google.android.exoplayer2.upstream.DataSpec;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;

/** Parallel media GETs with a byte-exact sequential fallback. POST/SABR is passed through. */
public final class ParallelRangeDataSource extends BaseDataSource {
  private final OkHttpClient client;
  private final DataSource.Factory fallbackFactory;
  private final String userAgent;
  private final int connections;
  private ParallelRangeReader reader;
  private DataSource fallback;
  private DataSpec spec;
  private long delivered;
  private long length;
  private boolean opened;

  public ParallelRangeDataSource(OkHttpClient client, DataSource.Factory fallbackFactory,
      String userAgent, int connections) {
    super(true);
    this.client = client;
    this.fallbackFactory = fallbackFactory;
    this.userAgent = userAgent;
    this.connections = connections;
  }

  @Override public long open(DataSpec dataSpec) throws IOException {
    spec = dataSpec;
    delivered = 0;
    length = dataSpec.length;
    transferInitializing(dataSpec);
    if (eligible(dataSpec)) {
      Request.Builder request = new Request.Builder().url(dataSpec.uri.toString())
          .header("User-Agent", userAgent);
      for (Map.Entry<String, String> header : dataSpec.httpRequestHeaders.entrySet()) {
        request.header(header.getKey(), header.getValue());
      }
      reader = new ParallelRangeReader(client, request.build(), connections);
      try {
        length = reader.open(dataSpec.position, dataSpec.length);
      } catch (IOException error) {
        reader.close();
        reader = null;
        if (Thread.currentThread().isInterrupted()) throw error;
        length = openFallback(dataSpec);
      }
    } else {
      length = openFallback(dataSpec);
    }
    opened = true;
    transferStarted(dataSpec);
    return length;
  }

  @Override public int read(byte[] target, int offset, int readLength) throws IOException {
    if (readLength == 0) return 0;
    if (length != C.LENGTH_UNSET && delivered == length) return C.RESULT_END_OF_INPUT;
    int count;
    if (reader != null) {
      try {
        count = reader.read(target, offset, readLength);
      } catch (IOException error) {
        reader.close();
        reader = null;
        if (Thread.currentThread().isInterrupted() || error.getClass() == InterruptedIOException.class
            || error instanceof ParallelRangeReader.EntityChangedException) {
          throw error;
        }
        // Only bytes already delivered to the extractor count, not prefetched bytes.
        openFallback(spec.subrange(delivered,
            length == C.LENGTH_UNSET ? C.LENGTH_UNSET : length - delivered));
        count = fallback.read(target, offset, readLength);
      }
    } else {
      count = fallback.read(target, offset, readLength);
    }
    if (count > 0) {
      delivered += count;
      bytesTransferred(count);
    }
    return count;
  }

  private long openFallback(DataSpec remaining) throws IOException {
    fallback = fallbackFactory.createDataSource();
    return fallback.open(remaining);
  }

  private static boolean eligible(DataSpec spec) {
    if (spec.httpMethod != DataSpec.HTTP_METHOD_GET || spec.httpBody != null
        || (spec.length != C.LENGTH_UNSET && spec.length < 2L * ParallelRangeReader.BLOCK_SIZE)) {
      return false;
    }
    HttpUrl url = HttpUrl.parse(spec.uri.toString());
    if (url == null || url.queryParameter("range") != null) return false;
    for (String header : spec.httpRequestHeaders.keySet()) {
      if (header.equalsIgnoreCase("Range") || header.equalsIgnoreCase("If-Range")) return false;
    }
    String path = url.encodedPath();
    return spec.length != C.LENGTH_UNSET || path.endsWith("/videoplayback")
        || path.endsWith(".mp4") || path.endsWith(".webm") || path.endsWith(".m4a");
  }

  @Override public Uri getUri() {
    return reader != null && reader.getResolvedUrl() != null
        ? Uri.parse(reader.getResolvedUrl()) : fallback != null ? fallback.getUri() : null;
  }

  @Override public Map<String, List<String>> getResponseHeaders() {
    return reader != null ? reader.getResponseHeaders()
        : fallback != null ? fallback.getResponseHeaders() : Collections.emptyMap();
  }

  @Override public void close() throws IOException {
    if (reader != null) {
      reader.close();
      reader = null;
    }
    try {
      if (fallback != null) fallback.close();
    } finally {
      fallback = null;
      if (opened) {
        opened = false;
        transferEnded();
      }
    }
  }
}
