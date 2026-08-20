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
