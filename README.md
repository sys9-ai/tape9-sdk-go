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
against a space and tape. See the package examples for complete call shapes.

The SDK is available under the MIT License in [`LICENSE`](LICENSE).

## Conditional batches

Use `AppendAfter` for a finite batch that must follow one exact previous batch.
The first call creates a conditional Tape, which rejects ordinary streaming writes:

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
conflict. `AppendState` reads mode and tail without reading content.

An omitted `AppendID` is generated once and returned in `result`, including on
error. Only a nil error acknowledges acceptance. `AppendAfter` accepts at most
32 MiB of raw bytes, never splits the atomic batch, and retries the same encoded
request. Empty bytes still advance the batch identity. Compression uses the
existing framed-zstd format; reads remain raw bytes. Retention never erases tail
metadata. Deleted conditional Tape IDs cannot be reused: replacement history
needs a new Tape ID. Ordinary `Append` and `Capture` remain unchanged.
