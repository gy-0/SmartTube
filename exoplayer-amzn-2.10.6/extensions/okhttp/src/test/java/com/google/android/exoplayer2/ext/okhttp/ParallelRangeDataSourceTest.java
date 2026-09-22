package com.google.android.exoplayer2.ext.okhttp;

import static org.junit.Assert.*;

import android.net.Uri;
import com.google.android.exoplayer2.C;
import com.google.android.exoplayer2.upstream.DataSource;
import com.google.android.exoplayer2.upstream.DataSpec;
import com.google.android.exoplayer2.upstream.TransferListener;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okio.Buffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.robolectric.RobolectricTestRunner;
import org.robolectric.annotation.Config;

@RunWith(RobolectricTestRunner.class)
@Config(manifest = Config.NONE, sdk = 28)
public class ParallelRangeDataSourceTest {
  private final byte[] data = new byte[1024 * 1024 + 37];
  private final AtomicInteger fallbackOpens = new AtomicInteger();
  private MockWebServer server;
  private OkHttpClient client;
  private ParallelRangeDataSource source;
  private String mode;
  private volatile String resumeRange;

  @Before public void setUp() throws Exception {
    for (int i = 0; i < data.length; i++) data[i] = (byte) (i * 7 + i / 131);
    client = new OkHttpClient.Builder().protocols(Collections.singletonList(Protocol.HTTP_1_1))
        .readTimeout(2, TimeUnit.SECONDS).build();
    server = new MockWebServer();
    server.setDispatcher(new Dispatcher() {
      @Override public MockResponse dispatch(RecordedRequest request) {
        String header = request.getHeader("Range");
        int start = 0, end = data.length - 1;
        if (header != null && !"ignored".equals(mode)) {
          String[] parts = header.substring(6).split("-");
          start = Integer.parseInt(parts[0]);
          if (parts.length > 1) end = Math.min(end, Integer.parseInt(parts[1]));
        }
        if ("fallback".equals(mode) && start >= ParallelRangeReader.BLOCK_SIZE
            && end - start + 1 <= ParallelRangeReader.BLOCK_SIZE) {
          return new MockResponse().setResponseCode(503);
        }
        if ("fallback".equals(mode) && start > 0) resumeRange = header;
        MockResponse response = new MockResponse().setBody(new Buffer().write(data, start, end - start + 1));
        if (header != null && !"ignored".equals(mode)) {
          response.setResponseCode(206).setHeader("Content-Range", "bytes " + start + "-" + end + "/" + data.length)
              .setHeader("ETag", "changed".equals(mode) && start > 0 ? "\"v2\"" : "\"v1\"");
        }
        return response;
      }
    });
    server.start();
    DataSource.Factory fallback = () -> {
      fallbackOpens.incrementAndGet();
      return new OkHttpDataSource(client, "test");
    };
    source = new ParallelRangeDataSource(client, fallback, "test", 4);
  }

  @After public void tearDown() throws Exception {
    source.close();
    server.shutdown();
    client.connectionPool().evictAll();
    client.dispatcher().executorService().shutdownNow();
  }

  private DataSpec spec(String path, long position, long length) {
    return new DataSpec(Uri.parse(server.url(path).toString()), position, length, null);
  }

  private byte[] drain() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buffer = new byte[19001];
    int read;
    while ((read = source.read(buffer, 0, buffer.length)) != C.RESULT_END_OF_INPUT) {
      out.write(buffer, 0, read);
    }
    return out.toByteArray();
  }

  @Test public void parallelBytesAndListenerLifecycle() throws Exception {
    final int[] events = new int[4];
    source.addTransferListener(new TransferListener() {
      public void onTransferInitializing(DataSource source, DataSpec spec, boolean network) { events[0]++; }
      public void onTransferStart(DataSource source, DataSpec spec, boolean network) { events[1]++; }
      public void onBytesTransferred(DataSource source, DataSpec spec, boolean network, int count) { events[2] += count; }
      public void onTransferEnd(DataSource source, DataSpec spec, boolean network) { events[3]++; }
    });
    assertEquals(data.length, source.open(spec("/video.mp4", 0, C.LENGTH_UNSET)));
    assertArrayEquals(data, drain());
    source.close();
    assertArrayEquals(new int[] {1, 1, data.length, 1}, events);
    assertEquals(0, fallbackOpens.get());
  }

  @Test public void ignoredRangesFallBackWithoutDuplicatingBytes() throws Exception {
    mode = "ignored";
    source.open(spec("/video.mp4", 197, 800000));
    assertArrayEquals(Arrays.copyOfRange(data, 197, 800197), drain());
    assertEquals(1, fallbackOpens.get());
  }

  @Test public void failedWorkerResumesAtDeliveredOffset() throws Exception {
    mode = "fallback";
    source.open(spec("/video.mp4", 0, data.length));
    assertArrayEquals(data, drain());
    assertEquals(1, fallbackOpens.get());
    assertEquals("bytes=" + ParallelRangeReader.BLOCK_SIZE + "-" + (data.length - 1), resumeRange);
  }

  @Test public void changedEntityIsNeverSplicedIntoPlayback() throws Exception {
    mode = "changed";
    source.open(spec("/video.mp4", 0, data.length));
    try {
      drain();
      fail("Changed entity must fail");
    } catch (ParallelRangeReader.EntityChangedException expected) {
      assertEquals(0, fallbackOpens.get());
    }
  }

  @Test public void postBodyAndHeadersArePassedThroughOnce() throws Exception {
    byte[] body = {10, 20, 30};
    source.open(new DataSpec(Uri.parse(server.url("/videoplayback").toString()),
        DataSpec.HTTP_METHOD_POST, body, 0, 0, C.LENGTH_UNSET, null, 0,
        Collections.singletonMap("X-Test", "preserved")));
    assertArrayEquals(data, drain());
    RecordedRequest request = server.takeRequest();
    assertEquals("POST", request.getMethod());
    assertArrayEquals(body, request.getBody().readByteArray());
    assertEquals("preserved", request.getHeader("X-Test"));
    assertEquals(1, server.getRequestCount());
    assertEquals(1, fallbackOpens.get());
  }

  @Test public void manifestUsesOriginalLoader() throws Exception {
    source.open(spec("/manifest.mpd", 0, C.LENGTH_UNSET));
    assertArrayEquals(data, drain());
    assertNull(server.takeRequest().getHeader("Range"));
    assertEquals(1, server.getRequestCount());
  }

  @Test public void smallRequestUsesOriginalLoader() throws Exception {
    source.open(spec("/video.mp4", 0, 100));
    assertArrayEquals(Arrays.copyOf(data, 100), drain());
    assertEquals(1, server.getRequestCount());
    assertEquals(1, fallbackOpens.get());
  }

  @Test public void sourceCanCloseAndReopenAtSeekPosition() throws Exception {
    source.open(spec("/video.mp4", 0, data.length));
    assertTrue(source.read(new byte[100], 0, 100) > 0);
    source.close();
    source.open(spec("/video.mp4", 55555, 777777));
    assertArrayEquals(Arrays.copyOfRange(data, 55555, 833332), drain());
  }
}
