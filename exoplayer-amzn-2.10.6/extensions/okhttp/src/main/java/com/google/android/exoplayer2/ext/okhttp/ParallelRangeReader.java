package com.google.android.exoplayer2.ext.okhttp;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.CancellationException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

/** Bounded, ordered HTTP range prefetcher. A reader belongs to exactly one open operation. */
public final class ParallelRangeReader implements AutoCloseable {
  public static final class EntityChangedException extends IOException {
    EntityChangedException() { super("Media entity changed during playback"); }
  }
  public static final int BLOCK_SIZE = 128 * 1024;
  private static final Pattern CONTENT_RANGE = Pattern.compile("bytes (\\d+)-(\\d+)/(\\d+)");
  private final OkHttpClient client;
  private final Request original;
  private final int connections;
  private final ExecutorService executor;
  private final Set<Call> calls = new HashSet<>();
  private final ArrayDeque<Future<byte[]>> pending = new ArrayDeque<>();
  private Future<byte[]> waiting;
  private volatile boolean closed;
  private long nextPosition;
  private long endPosition;
  private long totalSize;
  private String validator;
  private String validatorHeader;
  private byte[] current;
  private int currentOffset;
  private Map<String, List<String>> responseHeaders = Collections.emptyMap();
  private String resolvedUrl;

  public ParallelRangeReader(OkHttpClient client, Request original, int connections) {
    if (connections != 2 && connections != 4) {
      throw new IllegalArgumentException("Expected 2 or 4 connections");
    }
    this.client = client;
    this.original = original;
    this.connections = connections;
    executor = Executors.newFixedThreadPool(connections, runnable -> {
      Thread thread = new Thread(runnable, "SmartTube-range");
      thread.setDaemon(true);
      return thread;
    });
  }

  /** Opens the first range, checks range support, then starts the remaining workers. */
  public long open(long position, long length) throws IOException {
    if (position < 0 || length == 0 || length < -1
        || (length > 0 && position > Long.MAX_VALUE - length)) {
      throw new IOException("Invalid requested range");
    }
    long firstLength = length == -1 ? BLOCK_SIZE : Math.min(BLOCK_SIZE, length);
    if (position > Long.MAX_VALUE - firstLength) {
      throw new IOException("Range overflow");
    }
    Call call = newCall(position, position + firstLength - 1);
    Response response = null;
    try {
      response = call.execute();
      long[] range = parseRange(response);
      totalSize = range[2];
      endPosition = length == -1 ? totalSize : position + length;
      if (endPosition > totalSize || range[0] != position
          || range[1] != Math.min(position + firstLength, totalSize) - 1) {
        throw new IOException("Server returned a different range");
      }
      responseHeaders = response.headers().toMultimap();
      resolvedUrl = response.request().url().toString();
      String etag = response.header("ETag");
      if (etag != null && !etag.startsWith("W/")) {
        validator = etag;
        validatorHeader = "ETag";
      } else {
        validator = response.header("Last-Modified");
        validatorHeader = "Last-Modified";
      }
      int firstSize = (int) (range[1] - position + 1);
      final Response firstResponse = response;
      synchronized (this) {
        checkOpen();
        pending.add(executor.submit(() -> readBody(call, firstResponse, firstSize)));
        response = null; // Worker now owns the response.
        nextPosition = position + firstSize;
        fillWindow();
      }
      return endPosition - position;
    } finally {
      if (response != null) {
        response.close();
        removeCall(call);
      }
    }
  }

  public int read(byte[] target, int offset, int length) throws IOException {
    checkOpen();
    if (length == 0) return 0;
    if (current == null || currentOffset == current.length) {
      Future<byte[]> future;
      synchronized (this) {
        // Keep at most N blocks in memory, including the block consumed by the player.
        current = null;
        fillWindow();
        future = pending.poll();
        waiting = future;
      }
      if (future == null) return -1;
      try {
        current = future.get();
        currentOffset = 0;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new InterruptedIOException("Range read interrupted");
      } catch (CancellationException e) {
        throw new InterruptedIOException("Range read cancelled");
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof IOException) throw (IOException) cause;
        throw new IOException("Range worker failed", cause);
      } finally {
        synchronized (this) { waiting = null; }
      }
    }
    checkOpen();
    int count = Math.min(length, current.length - currentOffset);
    System.arraycopy(current, currentOffset, target, offset, count);
    currentOffset += count;
    return count;
  }

  public Map<String, List<String>> getResponseHeaders() { return responseHeaders; }
  public String getResolvedUrl() { return resolvedUrl; }

  private void fillWindow() throws IOException {
    checkOpen();
    while (pending.size() < connections && nextPosition < endPosition) {
      long start = nextPosition;
      int size = (int) Math.min(BLOCK_SIZE, endPosition - start);
      nextPosition += size;
      pending.add(executor.submit(() -> fetch(start, size)));
    }
  }

  private byte[] fetch(long start, int size) throws IOException {
    Call call = newCall(start, start + size - 1);
    Response response = null;
    try {
      response = call.execute();
      long[] range = parseRange(response);
      if (range[2] != totalSize
          || (validator != null && !validator.equals(response.header(validatorHeader)))) {
        throw new EntityChangedException();
      }
      if (range[0] != start || range[1] != start + size - 1) {
        throw new IOException("Server returned a different range");
      }
      return readBody(call, response, size);
    } finally {
      if (response != null) response.close();
      removeCall(call);
    }
  }

  private Call newCall(long start, long end) throws IOException {
    Request.Builder builder = original.newBuilder()
        .header("Range", "bytes=" + start + "-" + end)
        .header("Accept-Encoding", "identity");
    if (validator != null) builder.header("If-Range", validator);
    Call call = client.newCall(builder.build());
    synchronized (this) {
      checkOpen();
      calls.add(call);
    }
    return call;
  }

  private byte[] readBody(Call call, Response response, int size) throws IOException {
    try (Response ignored = response) {
      if (response.body() == null) throw new EOFException("Empty range body");
      byte[] data = new byte[size];
      InputStream input = response.body().byteStream();
      int offset = 0;
      while (offset < size) {
        checkOpen();
        int read = input.read(data, offset, size - offset);
        if (read == -1) throw new EOFException("Truncated range body");
        offset += read;
      }
      return data;
    } finally {
      removeCall(call);
    }
  }

  private static long[] parseRange(Response response) throws IOException {
    String encoding = response.header("Content-Encoding");
    if (response.code() != 206 || (encoding != null && !encoding.equalsIgnoreCase("identity"))) {
      throw new IOException("Server does not support uncompressed byte ranges");
    }
    String header = response.header("Content-Range");
    Matcher matcher = CONTENT_RANGE.matcher(header == null ? "" : header);
    if (!matcher.matches()) throw new IOException("Missing or invalid Content-Range");
    try {
      long start = Long.parseLong(matcher.group(1));
      long end = Long.parseLong(matcher.group(2));
      long total = Long.parseLong(matcher.group(3));
      if (end < start || end >= total) throw new IOException("Invalid Content-Range bounds");
      long bodyLength = response.body() == null ? -1 : response.body().contentLength();
      if (bodyLength != -1 && bodyLength != end - start + 1) {
        throw new IOException("Content-Length disagrees with Content-Range");
      }
      return new long[] {start, end, total};
    } catch (NumberFormatException e) {
      throw new IOException("Content-Range overflow", e);
    }
  }

  private void checkOpen() throws InterruptedIOException {
    if (closed || Thread.currentThread().isInterrupted()) {
      throw new InterruptedIOException("Range reader closed");
    }
  }

  private synchronized void removeCall(Call call) { calls.remove(call); }

  @Override public void close() {
    List<Call> active;
    synchronized (this) {
      if (closed) return;
      closed = true;
      active = new ArrayList<>(calls);
      if (waiting != null) waiting.cancel(true);
      for (Future<byte[]> future : pending) future.cancel(true);
      pending.clear();
    }
    for (Call call : active) call.cancel();
    executor.shutdownNow();
  }
}
