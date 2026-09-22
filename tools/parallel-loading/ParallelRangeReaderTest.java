package com.google.android.exoplayer2.ext.okhttp;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;

/** Standalone JVM regression/integration tests, using a real per-connection throttled HTTP server. */
public final class ParallelRangeReaderTest {
  private static final byte[] DATA = new byte[1024 * 1024 + 123];
  private static final AtomicInteger ACTIVE = new AtomicInteger();
  private static final AtomicInteger PEAK = new AtomicInteger();
  private static final AtomicInteger REQUESTS = new AtomicInteger();
  private static final AtomicInteger RETRIES = new AtomicInteger();
  private static final Set<Integer> PORTS = Collections.synchronizedSet(new HashSet<>());
  private static final OkHttpClient CLIENT = new OkHttpClient.Builder()
      .protocols(Collections.singletonList(Protocol.HTTP_1_1))
      .readTimeout(3, TimeUnit.SECONDS).build();
  private static String base;
  private static int passed;

  public static void main(String[] args) throws Exception {
    for (int i = 0; i < DATA.length; i++) DATA[i] = (byte) (i * 31 + i / 251);
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    ExecutorService serverThreads = Executors.newCachedThreadPool();
    server.setExecutor(serverThreads);
    server.createContext("/", ParallelRangeReaderTest::serve);
    server.start();
    base = "http://127.0.0.1:" + server.getAddress().getPort();
    try {
      checkBytes("known length", "/ok", 0, DATA.length, DATA);
      checkBytes("unknown length and short final block", "/ok", 0, -1, DATA);
      checkBytes("seek range", "/ok", 11111, 700003, Arrays.copyOfRange(DATA, 11111, 711114));
      checkBytes("last byte", "/ok", DATA.length - 1, -1, new byte[] {DATA[DATA.length - 1]});
      checkBytes("two connections", "/ok", 0, DATA.length, DATA, 2);
      checkBytes("redirect", "/redirect", 0, DATA.length, DATA);
      checkBytes("transient worker failure retries", "/retry", 0, DATA.length, DATA);
      for (String path : new String[] {"/ignored", "/wrong-start", "/malformed", "/compressed", "/overflow", "/status"}) {
        expectFailure(path, false);
      }
      for (String path : new String[] {"/changed", "/wrong-later", "/short", "/later-status"}) {
        expectFailure(path, true);
      }
      boundedPrefetch();
      cancelRead();
      benchmark();
      System.out.println("PASS " + passed + " integration checks");
    } finally {
      server.stop(0);
      serverThreads.shutdownNow();
      CLIENT.connectionPool().evictAll();
      CLIENT.dispatcher().executorService().shutdownNow();
    }
  }

  private static ParallelRangeReader reader(String path, int connections) {
    return new ParallelRangeReader(CLIENT, new Request.Builder().url(base + path)
        .header("X-Test", "preserved").build(), connections);
  }

  private static byte[] drain(ParallelRangeReader reader) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] bytes = new byte[17003];
    int count;
    while ((count = reader.read(bytes, 0, bytes.length)) != -1) out.write(bytes, 0, count);
    return out.toByteArray();
  }

  private static void checkBytes(String name, String path, long start, long length, byte[] expected) throws Exception {
    checkBytes(name, path, start, length, expected, 4);
  }

  private static void checkBytes(String name, String path, long start, long length, byte[] expected, int connections) throws Exception {
    try (ParallelRangeReader reader = reader(path, connections)) {
      require(reader.open(start, length) == expected.length, name + " length");
      require(reader.read(new byte[0], 0, 0) == 0, "zero read");
      require(Arrays.equals(expected, drain(reader)), name + " bytes");
    }
    passed++;
    System.out.println("PASS " + name);
  }

  private static void expectFailure(String path, boolean duringRead) throws Exception {
    boolean failed = false;
    try (ParallelRangeReader reader = reader(path, 4)) {
      reader.open(0, DATA.length);
      if (duringRead) drain(reader);
    } catch (IOException expected) {
      failed = true;
    }
    require(failed, "must reject " + path);
    passed++;
    System.out.println("PASS rejects " + path);
  }

  private static void boundedPrefetch() throws Exception {
    REQUESTS.set(0);
    try (ParallelRangeReader reader = reader("/bounded", 4)) {
      reader.open(0, DATA.length);
      Thread.sleep(200);
      require(REQUESTS.get() == 4, "prefetch must stop at four blocks when consumer pauses");
    }
    passed++;
    System.out.println("PASS bounded prefetch (512 KiB)");
  }

  private static void cancelRead() throws Exception {
    ParallelRangeReader reader = reader("/stall", 4);
    ExecutorService consumer = Executors.newSingleThreadExecutor();
    try {
      reader.open(0, DATA.length);
      Future<?> blocked = consumer.submit(() -> {
        try { drain(reader); throw new AssertionError("Cancelled read succeeded"); }
        catch (IOException expected) { }
      });
      Thread.sleep(100);
      long start = System.nanoTime();
      reader.close();
      blocked.get(1, TimeUnit.SECONDS);
      require(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(1), "cancel latency");
    } finally {
      reader.close();
      consumer.shutdownNow();
    }
    passed++;
    System.out.println("PASS cancels blocked read within 1 second");
  }

  private static void benchmark() throws Exception {
    long start = System.nanoTime();
    try (Response response = CLIENT.newCall(new Request.Builder().url(base + "/slow").build()).execute()) {
      require(Arrays.equals(DATA, response.body().bytes()), "serial bytes");
    }
    double serial = (System.nanoTime() - start) / 1e9;
    PEAK.set(0);
    PORTS.clear();
    start = System.nanoTime();
    try (ParallelRangeReader reader = reader("/slow", 4)) {
      reader.open(0, DATA.length);
      require(Arrays.equals(DATA, drain(reader)), "parallel benchmark bytes");
    }
    double parallel = (System.nanoTime() - start) / 1e9;
    require(PEAK.get() >= 3, "requests must overlap");
    require(PORTS.size() >= 4, "must use independent TCP connections");
    require(serial / parallel > 1.8, "parallel speedup should exceed 1.8x");
    passed++;
    System.out.printf("PASS throttled transfer: single %.3fs, four %.3fs, %.2fx, peak %d, TCP connections %d%n",
        serial, parallel, serial / parallel, PEAK.get(), PORTS.size());
  }

  private static void serve(HttpExchange exchange) throws IOException {
    String path = exchange.getRequestURI().getPath();
    if (path.equals("/redirect")) {
      exchange.getResponseHeaders().set("Location", base + "/ok");
      exchange.sendResponseHeaders(302, -1);
      exchange.close();
      return;
    }
    String range = exchange.getRequestHeaders().getFirst("Range");
    int start = 0, end = DATA.length - 1;
    if (range != null && !path.equals("/ignored")) {
      String[] parts = range.substring(6).split("-");
      start = Integer.parseInt(parts[0]);
      end = Math.min(Integer.parseInt(parts[1]), end);
    }
    if (path.equals("/status") || (path.equals("/later-status") && start > 0)) {
      exchange.sendResponseHeaders(503, -1);
      exchange.close();
      return;
    }
    if (path.equals("/retry") && start == ParallelRangeReader.BLOCK_SIZE && RETRIES.getAndIncrement() == 0) {
      exchange.sendResponseHeaders(503, -1);
      exchange.close();
      return;
    }
    if (path.equals("/bounded")) REQUESTS.incrementAndGet();
    if (range != null && !path.equals("/ignored")) {
      String contentRange = "bytes " + start + "-" + end + "/" + DATA.length;
      if (path.equals("/wrong-start") || (path.equals("/wrong-later") && start > 0)) {
        contentRange = "bytes " + (start + 1) + "-" + (end + 1) + "/" + DATA.length;
      }
      if (path.equals("/malformed")) contentRange = "nonsense";
      if (path.equals("/overflow")) contentRange = "bytes 0-999999999999999999999999999/9999999999999999999999999999999";
      exchange.getResponseHeaders().set("Content-Range", contentRange);
      exchange.getResponseHeaders().set("ETag", path.equals("/changed") && start > 0 ? "\"v2\"" : "\"v1\"");
    }
    if (path.equals("/compressed")) exchange.getResponseHeaders().set("Content-Encoding", "gzip");
    exchange.sendResponseHeaders(range == null || path.equals("/ignored") ? 200 : 206, end - start + 1);
    int active = ACTIVE.incrementAndGet();
    if (path.equals("/slow")) {
      PEAK.accumulateAndGet(active, Math::max);
      PORTS.add(exchange.getRemoteAddress().getPort());
    }
    try {
      if (path.equals("/stall")) Thread.sleep(1500);
      if (path.equals("/short") && start > 0) end = start + 10;
      for (int offset = start; offset <= end; offset += 8192) {
        exchange.getResponseBody().write(DATA, offset, Math.min(8192, end - offset + 1));
        exchange.getResponseBody().flush();
        if (path.equals("/slow")) Thread.sleep(12);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (IOException ignored) { // Expected when the reader cancels outstanding requests.
    } finally {
      ACTIVE.decrementAndGet();
      exchange.close();
    }
  }

  private static void require(boolean condition, String message) {
    if (!condition) throw new AssertionError(message);
  }
}
