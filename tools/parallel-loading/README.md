# Parallel media loading

The experimental debug APK is named **SmartTube Parallel** and uses
`org.smarttube.stable.parallel`, allowing a separate installation. In Settings →
Video player → Parallel video loading choose Off, 2 or 4 connections. The default
in this branch is four connections; a change applies to newly opened sources.

Eligible media GET requests are downloaded in ordered 128 KiB byte ranges over
HTTP/1.1. Each source retains at most four blocks (512 KiB) of read-ahead data.
Small requests, explicit URL/header ranges, manifests, and POST/SABR requests
retain the existing loader. No support for parallel SABR or guaranteed speedup
over a VPN is claimed.

Every range must return 206 with matching Content-Range, Content-Length (when
provided), representation length and entity validator (when provided). A failed
range falls back at the exact byte offset already delivered to the extractor.
An entity change is propagated as an error instead of splicing representations.
Closing or interrupting a reader cancels its calls and worker futures.

Run `bash tools/parallel-loading/test.sh` with JDK 17. It uses a loopback HTTP
server, covers byte identity, offsets, unknown length, redirects, invalid ranges,
truncation, bounded prefetch, cancellation and independent TCP concurrency, then
compares one vs four connections under a per-connection throttle. This is a
controlled transport test, not proof of YouTube/VPN performance on a television.

The dedicated GitHub Actions workflow builds a debug APK without release secrets.
