package worker

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// submitBackgroundJob sends one SUBMIT_JOB_BG and waits for JOB_CREATED,
// speaking the protocol directly rather than importing this module's own
// client package - see the comment at the top of example_test.go for why a
// test here must never import github.com/nook24/gearman-go/...
func submitBackgroundJob(t *testing.T, addr, fn, unique string, workload []byte) {
	t.Helper()

	conn, err := net.DialTimeout(Network, addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial job server: %v", err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}

	body := append([]byte(fn), 0)
	body = append(body, []byte(unique)...)
	body = append(body, 0)
	body = append(body, workload...)

	packet := make([]byte, 12+len(body))
	copy(packet[:4], reqStr)
	binary.BigEndian.PutUint32(packet[4:8], 18) // SUBMIT_JOB_BG
	binary.BigEndian.PutUint32(packet[8:12], uint32(len(body)))
	copy(packet[12:], body)

	if _, err := conn.Write(packet); err != nil {
		t.Fatalf("submit job: %v", err)
	}

	header := make([]byte, 12)
	if _, err := io.ReadFull(conn, header); err != nil {
		t.Fatalf("read job response: %v", err)
	}
	if dt := binary.BigEndian.Uint32(header[4:8]); dt != dtJobCreated {
		t.Fatalf("expected JOB_CREATED (%d), got packet type %d", dtJobCreated, dt)
	}
	if size := binary.BigEndian.Uint32(header[8:12]); size > 0 {
		if _, err := io.ReadFull(conn, make([]byte, size)); err != nil {
			t.Fatalf("read job handle: %v", err)
		}
	}
}

// newTestWorker builds a worker registered for fn on the local job server.
func newTestWorker(t *testing.T, fn string, handler JobFunc) *Worker {
	t.Helper()

	w := New(Unlimited)
	w.ErrorHandler = func(e error) { t.Logf("worker error: %v", e) }
	if err := w.AddServer(Network, "127.0.0.1:4730"); err != nil {
		t.Fatalf("add server: %v", err)
	}
	if err := w.AddFunc(fn, handler, 0); err != nil {
		t.Fatalf("add func: %v", err)
	}
	if err := w.Ready(); err != nil {
		t.Fatalf("ready: %v", err)
	}
	go w.Work()
	return w
}

// TestCloseWaitsForRunningJobToAcknowledge is the regression test for the
// reason this fork drains on Close.
//
// A job that is still running when Close is called used to finish its work
// and then skip its WORK_COMPLETE, because exec only writes the response
// while running is true and Close cleared that flag first. The job server
// never learned the job had finished, so on disconnect it re-queued it and
// handed it to the next worker - the job ran twice, and for a background
// job nobody ever found out.
//
// The assertion is behavioural rather than a peek at internal state: after
// Close returns, a second worker subscribes to the same function and must
// be handed nothing.
func TestCloseWaitsForRunningJobToAcknowledge(t *testing.T) {
	if !runIntegrationTests {
		t.Skip("To run this test, use: go test -integration")
	}

	// Unique per run: a leftover job from an earlier run on a shared job
	// server would be delivered to the second worker below and look
	// exactly like the failure this test reports.
	fn := fmt.Sprintf("drain_test_fn_%d", time.Now().UnixNano())

	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once

	first := newTestWorker(t, fn, func(job Job) ([]byte, error) {
		once.Do(func() { close(started) })
		<-release
		return nil, nil
	})

	submitBackgroundJob(t, "127.0.0.1:4730", fn, "", []byte("payload"))

	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("job was never delivered to the first worker")
	}

	// Close must not return before the handler does. Releasing it only
	// after Close has been called for a moment is what proves the wait:
	// without draining, Close returns immediately and closed is set while
	// the handler is still parked.
	closed := make(chan struct{})
	go func() {
		first.Close()
		close(closed)
	}()

	select {
	case <-closed:
		t.Fatal("Close returned while a job was still running")
	case <-time.After(500 * time.Millisecond):
	}

	close(release)

	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return after the job finished")
	}

	// If the acknowledgement was lost, the server re-queues the job on
	// disconnect and the next worker for this function gets it.
	redelivered := make(chan struct{})
	second := newTestWorker(t, fn, func(job Job) ([]byte, error) {
		close(redelivered)
		return nil, nil
	})
	defer second.Close()

	select {
	case <-redelivered:
		t.Fatal("job was handed out a second time - Close did not let it acknowledge")
	case <-time.After(3 * time.Second):
	}
}

// TestCloseIsIdempotentWhileDraining covers the guard the drain added: the
// running flag now stays true until the drain finishes, so a second Close
// arriving in that window can no longer fall through into a second
// teardown - closing the agents and worker.in twice.
func TestCloseIsIdempotentWhileDraining(t *testing.T) {
	if !runIntegrationTests {
		t.Skip("To run this test, use: go test -integration")
	}

	fn := fmt.Sprintf("drain_idem_fn_%d", time.Now().UnixNano())

	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once

	w := newTestWorker(t, fn, func(job Job) ([]byte, error) {
		once.Do(func() { close(started) })
		<-release
		return nil, nil
	})

	submitBackgroundJob(t, "127.0.0.1:4730", fn, "", []byte("payload"))

	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("job was never delivered")
	}

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.Close()
		}()
	}

	time.Sleep(200 * time.Millisecond)
	close(release)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent Close calls did not all return")
	}
}

// TestDrainTimeoutDoesNotHangClose makes sure the bound actually bounds: a
// handler that never returns must cost the shutdown DrainTimeout, not the
// process.
func TestDrainTimeoutDoesNotHangClose(t *testing.T) {
	if !runIntegrationTests {
		t.Skip("To run this test, use: go test -integration")
	}

	original := DrainTimeout
	DrainTimeout = 300 * time.Millisecond
	defer func() { DrainTimeout = original }()

	fn := fmt.Sprintf("drain_timeout_fn_%d", time.Now().UnixNano())

	started := make(chan struct{})
	stuck := make(chan struct{})
	defer close(stuck) // let the handler go when the test ends
	var once sync.Once

	var timedOut bool
	var mu sync.Mutex

	w := New(Unlimited)
	w.ErrorHandler = func(e error) {
		if e == ErrDrainTimeout {
			mu.Lock()
			timedOut = true
			mu.Unlock()
		}
	}
	if err := w.AddServer(Network, "127.0.0.1:4730"); err != nil {
		t.Fatalf("add server: %v", err)
	}
	err := w.AddFunc(fn, func(job Job) ([]byte, error) {
		once.Do(func() { close(started) })
		<-stuck
		return nil, nil
	}, 0)
	if err != nil {
		t.Fatalf("add func: %v", err)
	}
	if err := w.Ready(); err != nil {
		t.Fatalf("ready: %v", err)
	}
	go w.Work()

	submitBackgroundJob(t, "127.0.0.1:4730", fn, "", []byte("payload"))

	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("job was never delivered")
	}

	closed := make(chan struct{})
	go func() {
		w.Close()
		close(closed)
	}()

	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close hung on a handler that never returns")
	}

	mu.Lock()
	defer mu.Unlock()
	if !timedOut {
		t.Error("expected ErrDrainTimeout to be reported to the ErrorHandler")
	}
}
