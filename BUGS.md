# Known Bugs

## Noisy warning on stale sockets during polling

**Location**: `connector.py:462-465`

**Symptom**: When a client polls `get_active_runs()` repeatedly (e.g. `taro live`), a full traceback is printed
to stderr each time a recently finished job's node socket is contacted:

```
[instance_call_error] op=[collect_active_runs] server=[/tmp/.../server-rpc.sock]
Traceback (most recent call last):
  ...
BrokenPipeError: [Errno 32] Broken pipe
```

**Root cause**: When a job ends, the node process shuts down and closes its socket, but the socket file on disk
lingers briefly. `collect_active_runs()` discovers the stale file, tries to send a message, and gets
`BrokenPipeError`. The error is handled correctly (skipped, results from other servers returned), but
`log.warning(..., exc_info=result.error)` prints the full traceback.

**Why it wasn't visible before**: `taro ps` fetches once and exits, so the race window is tiny. `taro live` polls
every second, making it nearly guaranteed to hit the gap between node shutdown and socket file cleanup.

**Possible fixes**:
1. Downgrade to `log.debug()` for `BrokenPipeError` / `ConnectionRefusedError` specifically
2. Clean up the socket file synchronously before the node process exits
3. Add a quiet/suppress option to `get_active_runs()` for polling use cases
4. Remove `exc_info` from the warning (log the one-liner, skip the traceback)
