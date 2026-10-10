# Tape9 Go SDK

This module is the redistributable Go client for Tape9. It owns the public client-side wire contract while the
Tape9 server implementation remains private.

Install it with:

```sh
go get github.com/sys9-ai/tape9-sdk-go
```

The SDK depends only on public Go modules.

API documentation is available on
[`pkg.go.dev`](https://pkg.go.dev/github.com/sys9-ai/tape9-sdk-go) or locally:

```sh
go doc github.com/sys9-ai/tape9-sdk-go
```

Create a client with `tape9.New`, then use `Append`, `Read`, `Pull`, or `Follow`
against a space and tape. `CloseTape` stops future writes while keeping existing
content readable. `Read` and `Pull` report `Closed` and the incarnation's stored
`TotalBytes`; `Follow` writes the final content and returns when the tape closes. See the package examples for complete call shapes.

The SDK is available under the MIT License in [`LICENSE`](LICENSE).

## Conditional batches

Use `AppendAfter` for a finite batch that must follow one exact previous batch.
It works on any Tape, including one with ordinary content. There is no write mode:
ordinary `Append` and `Capture` stay allowed and never check or change the conditional
tail. To order all content, your application must exclude ordinary writers.

```go
result, err := client.AppendAfter(ctx, spaceID, tapeID, payload, tape9.AppendAfterOptions{
    After: previousBatchID, // empty for the first batch
    AppendID: pendingBatchID,
})
```

Persist a unique `pendingBatchID`, predecessor and immutable payload before sending
when recovery must survive process replacement. Keep one unacknowledged batch.
An exact retry of the latest batch succeeds; older or changed batches return an
`AppendConflictError` with `TailID`. Do not change the predecessor to bypass a
conflict. `AppendState` reads the last conditional batch ID without reading content.
An empty tail means no conditional batch has succeeded, not that the Tape is empty.
Ordinary writes between conditional batches do not invalidate the latest retry.

An omitted `AppendID` is generated once and returned in `result`, including on
error. Only a nil error acknowledges acceptance. `AppendAfter` accepts at most
32 MiB of raw bytes, never splits the atomic batch, and retries the same encoded
request. Empty bytes still advance the batch identity. Compression uses the
existing framed-zstd format; reads remain raw bytes. Retention never erases tail
metadata. Once a conditional batch succeeds (even with empty bytes), its Tape ID
cannot be reused after deletion: replacement history needs a new Tape ID. Before
that first success, ordinary delete/recreate behavior remains, including for a
failed first conditional request. A delayed empty-predecessor request can then
reach the recreated Tape; applications own safe handoff from legacy writers.
