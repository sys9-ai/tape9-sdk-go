package tape9

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"
)

// CaptureOptions configures Client.Capture.
type CaptureOptions struct {
	// RetainMode applies the tape-level retention mode only if Capture
	// creates the target tape. Existing tapes keep their current mode.
	RetainMode RetainMode

	// Compression controls whether the SDK transforms bytes before storing them.
	// When empty, payload bytes are stored as-is.
	Compression Compression

	// UsageScope is an opaque caller-owned identifier used to aggregate related
	// tapes. A live tape can be bound once; recreate starts a new incarnation.
	UsageScope string

	// IdempotencyKey reuses one capture session across retryable attempts when
	// provided. When empty, the SDK generates one internally.
	IdempotencyKey string

	// PollInterval controls how frequently the uploader checks for new bytes
	// appended to the local spool file.
	//
	// When <= 0, a default is used.
	PollInterval time.Duration

	// SpoolDir is the directory where the local spool file is created.
	//
	// When empty, the OS default temp directory is used.
	SpoolDir string

	// Stdin is connected to the child process stdin.
	//
	// When nil, the child process gets a nil stdin.
	Stdin io.Reader
}

// CaptureResult describes one completed process capture.
type CaptureResult struct {
	// TapeID is the tape that received the captured process output.
	TapeID string

	// ExitCode is the child process exit code.
	ExitCode int

	// AppendErr is set when creating/reusing the target tape or appending
	// captured bytes fails after the process starts.
	AppendErr error

	// SpoolErr is set when writing captured bytes to the local spool file fails.
	SpoolErr error
}

// Capture runs a local process and appends its merged stdout/stderr output into a tape.
//
// When opts.IdempotencyKey is empty, the SDK generates one internally and
// reuses it across retryable attempts within the call. The same key can also
// replay a completed session whose success response was lost: the server
// acknowledges already committed chunks and close is idempotent. If a
// caller-provided key fails after stream creation, the stream stays open so the
// same key can retry the session. If the SDK generated the key, it best-effort
// closes the stream before returning the failure.
//
// It is a best-effort wrapper around process execution + append:
//   - Process start errors are fatal and do not create or append to the target tape.
//   - Remote create/append errors after process start do not affect child process execution and are returned in CaptureResult.AppendErr.
//   - The process exit code is returned via CaptureResult.ExitCode (non-zero exits are not treated as errors).
//   - If ctx is canceled while the child process is running, Capture terminates the child and returns ctx.Err(). When
//     the remote append path is still unfinished at that point, CaptureResult.AppendErr also reflects ctx.Err() unless
//     a more specific append failure is already known.
func (c *Client) Capture(ctx context.Context, spaceID string, tapeID string, procArgs []string, forward io.Writer, opts CaptureOptions) (CaptureResult, error) {
	if len(procArgs) == 0 {
		return CaptureResult{}, fmt.Errorf("missing process")
	}
	if !IsValidID(spaceID) {
		return CaptureResult{}, fmt.Errorf("invalid space_id: %q", spaceID)
	}
	if !IsValidID(tapeID) {
		return CaptureResult{}, fmt.Errorf("invalid tape_id: %q", tapeID)
	}
	if forward == nil {
		forward = io.Discard
	}
	selectedFormat, err := opts.Compression.payloadFormat()
	if err != nil {
		return CaptureResult{}, err
	}

	cleanupOnFailure := opts.IdempotencyKey == ""
	idempotencyKey := opts.IdempotencyKey
	if idempotencyKey == "" {
		key, err := NewIdempotencyKey()
		if err != nil {
			return CaptureResult{}, err
		}
		idempotencyKey = key
	}

	pollInterval := opts.PollInterval
	if pollInterval <= 0 {
		pollInterval = 50 * time.Millisecond
	}

	spoolFile, err := os.CreateTemp(opts.SpoolDir, "tape9-capture-*")
	if err != nil {
		return CaptureResult{}, err
	}
	spoolPath := spoolFile.Name()
	spoolReader, err := os.Open(spoolPath)
	if err != nil {
		_ = spoolFile.Close()
		_ = os.Remove(spoolPath)
		return CaptureResult{}, err
	}

	doneWriting := make(chan struct{})
	output := &procOutput{
		forward: forward,
		spool:   spoolFile,
	}

	cmd := exec.CommandContext(ctx, procArgs[0], procArgs[1:]...) //nolint:gosec // user-controlled by design
	cmd.Stdout = output
	cmd.Stderr = output
	if opts.Stdin != nil {
		cmd.Stdin = opts.Stdin
	}

	if err := cmd.Start(); err != nil {
		_ = spoolReader.Close()
		_ = spoolFile.Close()
		_ = os.Remove(spoolPath)

		out := CaptureResult{
			ExitCode: 1,
			SpoolErr: output.spoolErr,
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return out, ctxErr
		}
		return out, err
	}

	uploadDone := make(chan struct{})
	spoolReady := make(chan struct{})
	var uploadState struct {
		mu        sync.Mutex
		tapeID    string
		appendErr error
	}

	// A failed process start must not create an empty tape. Start the remote
	// append path only after the child is running; failures from here are
	// reported as append failures without changing the child result.
	go func() {
		defer close(uploadDone)
		defer os.Remove(spoolPath)
		defer spoolReader.Close()

		gotTapeID, uploadFormat, err := c.createOrReuseAppendTape(ctx, spaceID, tapeID, createTapeOptions{
			RetainMode:    opts.RetainMode,
			PayloadFormat: selectedFormat,
			UsageScope:    opts.UsageScope,
		})
		uploadState.mu.Lock()
		uploadState.tapeID = gotTapeID
		uploadState.mu.Unlock()
		if err != nil {
			uploadState.mu.Lock()
			uploadState.appendErr = err
			uploadState.mu.Unlock()
			return
		}

		close(spoolReady)

		tr := &tailReader{
			f:             spoolReader,
			done:          doneWriting,
			flushInterval: c.framedZstdUploadFlushInterval,
			pollInterval:  pollInterval,
		}

		reportFailure := func(err error) {
			if err == nil {
				return
			}

			uploadState.mu.Lock()
			if uploadState.appendErr == nil {
				uploadState.appendErr = err
			}
			uploadState.mu.Unlock()
		}

		_, err = c.uploadWithPayloadFormat(uploadRequest{
			ctx:              ctx,
			teardownCtx:      context.Background(),
			spaceID:          spaceID,
			tapeID:           gotTapeID,
			reader:           tr,
			format:           uploadFormat,
			idempotencyKey:   idempotencyKey,
			cleanupOnFailure: cleanupOnFailure,
			reportFailure:    reportFailure,
			tapeOptions: createTapeOptions{
				RetainMode:    opts.RetainMode,
				PayloadFormat: selectedFormat,
				UsageScope:    opts.UsageScope,
			},
		})
		if err == nil {
			return
		}

		uploadState.mu.Lock()
		if uploadState.appendErr == nil {
			uploadState.appendErr = err
		}
		uploadState.mu.Unlock()
	}()

	runErr := cmd.Wait()

	exitCode := 0
	var exitErr *exec.ExitError
	switch {
	case runErr == nil:
		exitCode = 0
	case errors.As(runErr, &exitErr):
		exitCode = exitErr.ExitCode()
		if exitCode < 0 {
			exitCode = 1
		}
	default:
		exitCode = 1
	}

	_ = spoolFile.Close()
	close(doneWriting)

	if ctxErr := ctx.Err(); ctxErr != nil {
		interruptedByContext := false
		if runErr != nil {
			var exitErr *exec.ExitError
			interruptedByContext = !errors.As(runErr, &exitErr) || exitErr.ExitCode() < 0
		}
		if !interruptedByContext {
			goto waitUpload
		}
		uploadFinished := false
		select {
		case <-uploadDone:
			uploadFinished = true
		default:
		}

		uploadState.mu.Lock()
		gotTapeID := uploadState.tapeID
		appendErr := uploadState.appendErr
		uploadState.mu.Unlock()

		if appendErr == nil && !uploadFinished {
			appendErr = ctxErr
		}
		if gotTapeID != "" {
			select {
			case <-spoolReady:
				_ = os.Remove(spoolPath)
			default:
			}
		}
		out := CaptureResult{
			TapeID:    gotTapeID,
			ExitCode:  exitCode,
			AppendErr: appendErr,
			SpoolErr:  output.spoolErr,
		}
		return out, ctxErr
	}

waitUpload:
	<-uploadDone
	uploadState.mu.Lock()
	gotTapeID := uploadState.tapeID
	appendErr := uploadState.appendErr
	uploadState.mu.Unlock()

	out := CaptureResult{
		TapeID:    gotTapeID,
		ExitCode:  exitCode,
		AppendErr: appendErr,
		SpoolErr:  output.spoolErr,
	}

	if runErr != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return out, ctxErr
		}
	}

	if runErr != nil && !errors.As(runErr, &exitErr) {
		return out, runErr
	}
	return out, nil
}

type procOutput struct {
	mu       sync.Mutex
	forward  io.Writer
	spool    *os.File
	spoolErr error
}

// Write implements io.Writer by mirroring process output to the forward writer
// and the local spool file.
func (w *procOutput) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	n, err := w.forward.Write(p)
	if err != nil {
		return n, err
	}
	if n != len(p) {
		return n, io.ErrShortWrite
	}

	w.writeToSpoolLocked(p)
	return n, nil
}

func (w *procOutput) writeToSpoolLocked(p []byte) {
	if w.spool == nil {
		return
	}
	n, err := w.spool.Write(p)
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	if err != nil {
		if w.spoolErr == nil {
			w.spoolErr = err
		}
		w.spool = nil
	}
}

type tailReader struct {
	f             *os.File
	done          <-chan struct{}
	flushInterval time.Duration
	pollInterval  time.Duration
}

// Read implements io.Reader by tailing the local spool file until writing is done.
func (r *tailReader) Read(p []byte) (int, error) {
	writingDone := false
	for {
		n, err := r.f.Read(p)
		if n > 0 {
			return n, nil
		}
		if err == nil {
			continue
		}
		if !errors.Is(err, io.EOF) {
			return 0, err
		}
		if writingDone {
			return 0, io.EOF
		}

		select {
		case <-r.done:
			writingDone = true
			continue
		default:
		}

		if !r.waitForMore(r.pollInterval) {
			writingDone = true
		}
	}
}

// ReadUploadChunk returns the currently available spool bytes and waits up to
// one flush interval for more before sealing the current framed-zstd chunk.
func (r *tailReader) ReadUploadChunk(p []byte) (int, error) {
	var flushDeadline time.Time
	writingDone := false
	n := 0
	for {
		readN, err := r.f.Read(p[n:])
		n += readN
		if n == len(p) {
			return n, nil
		}
		if err == nil {
			continue
		}
		if !errors.Is(err, io.EOF) {
			if n > 0 {
				return n, err
			}
			return 0, err
		}
		if writingDone {
			return n, io.EOF
		}

		select {
		case <-r.done:
			writingDone = true
			continue
		default:
		}

		if n == 0 {
			if !r.waitForMore(r.pollInterval) {
				writingDone = true
			}
			continue
		}
		if flushDeadline.IsZero() {
			flushDeadline = time.Now().Add(r.flushInterval)
		}
		if !time.Now().Before(flushDeadline) {
			return n, nil
		}

		wait := r.pollInterval
		remaining := time.Until(flushDeadline)
		if wait <= 0 || wait > remaining {
			wait = remaining
		}
		if !r.waitForMore(wait) {
			writingDone = true
		}
	}
}

func (r *tailReader) waitForMore(wait time.Duration) bool {
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-r.done:
		return false
	case <-timer.C:
		return true
	}
}
