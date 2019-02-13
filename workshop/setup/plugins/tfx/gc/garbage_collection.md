# Handling garbage collection for TFX

Currently we support span-based garbage collection marking for Artifacts
produced by TFX.

Examples:

Specify the maximum num of spans to keep for the garbage collection.

```
  garbage_collector = SpanBasedGarbageCollector(
      num_spans_to_keep=5,
      log_root='/var/tmp/')
```

Mark artifacts as 'DELETING' in ML metadata. The artifacts marked will not be
valid for use. However users should handle real file deletion after marking
artifacts as 'DELETING'.

```
  garbage_collector.do_garbage_collection()
```
