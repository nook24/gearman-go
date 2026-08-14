# gearman-go

[![CI](https://github.com/nook24/gearman-go/actions/workflows/ci.yml/badge.svg)](https://github.com/nook24/gearman-go/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/nook24/gearman-go.svg)](https://pkg.go.dev/github.com/nook24/gearman-go)

A [Gearman](http://gearman.org/) API for the [Go Programming Language](http://golang.org),
with the protocol implemented in pure Go. It contains two sub-packages:

The **client** package is used for sending jobs to the Gearman job server,
and getting responses from the server.

	import "github.com/nook24/gearman-go/client"

The **worker** package will help developers in developing Gearman worker
services easily.

	import "github.com/nook24/gearman-go/worker"

This is a maintained fork of [mikespook/gearman-go](https://github.com/mikespook/gearman-go).

## Why this fork exists

Upstream has had no commits since May 2022. This fork carries the fixes that
came out of running the library in production. Two of them are in
`Worker.Close()`, which is where the interesting bugs turned out to live; the
third is the reason nobody had found them earlier.

### Jobs still running were never acknowledged

The one with visible consequences for your data. `Close()` cleared `running`
and closed every agent connection *before* waiting for anything, and `exec`
writes a job's `WORK_COMPLETE` only while `running` is true. Every job still
executing at that moment therefore finished its work and then silently skipped
its acknowledgement.

The job server never learned those jobs had completed, so on disconnect it
re-queued them and handed them to the next worker that connected — running them
a second time, and for a background job without the client ever finding out.
The count is bounded by the concurrency limit, so a worker at its cap re-runs
exactly that many jobs on every restart.

That is not academic. In the pipeline this fork is maintained for, a `SIGTERM`
under load re-queued exactly 64 jobs — the configured cap — whose rows were
already committed to MySQL; the redelivery then collided on a primary key,
MySQL rejected the entire multi-row `INSERT` those rows shared, and 1.1% of all
events were lost across the restart.

The fix ([`494d20b`](https://github.com/nook24/gearman-go/commit/494d20b)):
`Close()` now sets a `draining` flag first, which stops new jobs from starting
and stops asking the servers for more, while the connections stay open and
`running` stays true. It waits for the jobs already in flight — so they can
report their result — and only then clears `running` and disconnects. The wait
is bounded by `DrainTimeout` (30s, a `var` so callers can shorten it), because
a handler that never returns must not turn a shutdown into a hang; on expiry
`ErrDrainTimeout` goes to the `ErrorHandler` and the old behaviour applies.

### A data race on the job channel

The one that started the fork. `Close()` closed the `worker.in` channel while
the per-connection agent goroutines were still sending job packets on it. The
race detector flags this
reliably, and losing the race means a `send on closed channel` panic on a
goroutine the caller does not own and therefore cannot recover from. In
practice `agent.work` recovers it into `ErrorHandler`, so it usually costs one
dead agent goroutine rather than the process — but that is the runtime's
reaction to undefined behaviour, not a guarantee.

The fix ([`7118c0a`](https://github.com/nook24/gearman-go/commit/7118c0a)):

* A `sync.WaitGroup` counts the agent goroutines, incremented in both places
  that spawn them (`agent.Connect` and `agent.reconnect`). `Close()` waits on
  it before closing the channel, so no agent can still be sending by then.
* Waiting alone can stall — an agent parked in `worker.in <- inpack` only
  proceeds once somebody receives, and the `Work()` loop may itself be parked
  on the concurrency limit. `Close()` therefore drains `worker.in` while it
  waits. Packets picked up that way are dropped, which is what closing the
  worker means.
* `Worker.running` became an `atomic.Bool`. It is written by `Close()` under
  the worker mutex but read by `exec` on the job goroutines without it — a
  second race, previously masked by the first.

### A test flag that disabled the tests it gated

`-integration` was read before it was parsed: `TestMain` dereferenced the
pointer `flag.Bool` returns straight away, but `m.Run` is what calls
`flag.Parse`. The value was therefore always the default, and every integration
test in the `worker` package skipped no matter what was passed on the command
line — including in CI, which had been passing the flag all along. Fixed in
[`2504393`](https://github.com/nook24/gearman-go/commit/2504393); running those
tests for the first time immediately turned up a data race on `Worker.ready`,
fixed in [`b866d45`](https://github.com/nook24/gearman-go/commit/b866d45).

Beyond that, the fork adds a `go.mod` so it can be consumed as a module, keeps
CI running (see below), and fixes small warts such as the example worker
depending on an unrelated third-party package.

**There are no breaking API changes.** This is a drop-in replacement for the
upstream package; the goal is maintenance, not new features. `v1.1.0` adds two
exported names, both of them optional: `DrainTimeout` and `ErrDrainTimeout`.
Existing code compiles and behaves as before, except that `Close()` now waits
for running jobs instead of abandoning them.

## Installation

Go 1.19 or newer is required (`worker` uses `atomic.Bool`). The module declares
itself as `github.com/nook24/gearman-go`, so there are two ways to use it.

### As a replacement for the upstream module (recommended)

Your imports stay on the upstream path and nothing in your code changes. In
your `go.mod`:

	require github.com/mikespook/gearman-go v0.0.0-... // keep whatever you have

	replace github.com/mikespook/gearman-go => github.com/nook24/gearman-go v1.1.0

To pick up a newer release later:

	go get github.com/nook24/gearman-go@latest

and copy the version it resolves into the `replace` line.

The module deliberately does *not* declare the upstream path: Go verifies that
a replacement module's `go.mod` names the path it is hosted under, so a fork
here that still called itself `github.com/mikespook/gearman-go` could not be
used as a replace target at all.

### Directly

	go get github.com/nook24/gearman-go

and change your imports to `github.com/nook24/gearman-go/client` and
`github.com/nook24/gearman-go/worker`.

## Usage

### Worker

```go
// Limit number of concurrent jobs execution.
// Use worker.Unlimited (0) if you want no limitation.
w := worker.New(worker.OneByOne)
w.ErrHandler = func(e error) {
	log.Println(e)
}
w.AddServer("tcp4", "127.0.0.1:4730")
// Use worker.Unlimited (0) if you want no timeout
w.AddFunc("ToUpper", ToUpper, worker.Unlimited)
// This will give a timeout of 5 seconds
w.AddFunc("ToUpperTimeOut5", ToUpper, 5)

if err := w.Ready(); err != nil {
	log.Fatal(err)
	return
}
go w.Work()
```

### Client

```go
// ...
c, err := client.New("tcp4", "127.0.0.1:4730")
// ... error handling
defer c.Close()
c.ErrorHandler = func(e error) {
	log.Println(e)
}
echo := []byte("Hello\x00 world")
echomsg, err := c.Echo(echo)
// ... error handling
log.Println(string(echomsg))
jobHandler := func(resp *client.Response) {
	log.Printf("%s", resp.Data)
}
handle, err := c.Do("ToUpper", echo, client.JobNormal, jobHandler)
// ...
```

Runnable examples live in [`example/`](example/), and `worker/example_test.go`
shows a complete worker including the job and error handlers.

## Testing

The unit tests need nothing but a Go toolchain:

	go test -race ./...

The integration tests are opt-in via an `-integration` flag and need a Gearman
job server on `127.0.0.1:4730`:

	sudo apt-get install -y gearman-job-server   # or: gearmand -d
	go test -race ./worker/... -integration

Note the flag position. `-integration` is a flag of the *test binary*, not of
`go test`, and an unknown flag ends the package list — `go test -integration
./worker/...` tests the current directory and silently runs nothing.

The client-side integration tests (`client/client_test.go`,
`client/pool_test.go`) additionally expect workers registered for the functions
they call; `TestClientMultiDo` needs [`example/pl/worker_multi.pl`](example/pl/worker_multi.pl)
running. Without those they block, which is why CI runs the integration suite
for the worker package only.

## Status and maintenance

The library has been in production use for over six years. The API is stable
and existing code will not be broken here, which is what `v1.0.0` — the first
tagged release, upstream never had any — was meant to say. `v1.1.0` keeps that
promise: it fixes the acknowledgement bug above and adds two optional names,
nothing else.

* Bug fixes: yes, especially anything the race detector finds.
* New features: unlikely, but issues and pull requests are welcome.
* Upstream: if `mikespook/gearman-go` ever sees commits again, they will be
  merged in.

## Changes in this fork

### v1.1.0

* [`494d20b`](https://github.com/nook24/gearman-go/commit/494d20b) — let jobs
  that are still running acknowledge before `Close` disconnects, so the server
  stops handing them out a second time.
* [`b866d45`](https://github.com/nook24/gearman-go/commit/b866d45) — make
  `Worker.ready` an `atomic.Bool`.
* [`2504393`](https://github.com/nook24/gearman-go/commit/2504393) — make the
  `-integration` flag actually enable the integration tests.

### v1.0.0

* [`7118c0a`](https://github.com/nook24/gearman-go/commit/7118c0a) — fix two
  data races in `Worker.Close`.
* [`aeb5ea9`](https://github.com/nook24/gearman-go/commit/aeb5ea9) — add
  `go.mod` so the fork can be required as a module.
* [`304c61a`](https://github.com/nook24/gearman-go/commit/304c61a) — keep the
  worker example test inside `package worker`, so the module no longer imports
  itself and can be used through a `replace` directive.
* Documentation and CI: GitHub Actions replaces the retired Travis setup, the
  example worker no longer depends on `github.com/mikespook/golib`, and
  `go vet ./...` is clean.

## Contributors

Great thanks to all of you for your support and interest!

(_Alphabetic order_)

 * [Alex Zylman](https://github.com/azylman)
 * [C.R. Kirkwood-Watts](https://github.com/kirkwood)
 * [Damian Gryski](https://github.com/dgryski)
 * [Gabriel Cristian Alecu](https://github.com/AzuraMeta)
 * [Graham Barr](https://github.com/gbarr)
 * [Ingo Oeser](https://github.com/nightlyone)
 * [jake](https://github.com/jbaikge)
 * [Joe Higton](https://github.com/draxil)
 * [Jonathan Wills](https://github.com/runningwild)
 * [Kevin Darlington](https://github.com/kdar)
 * [miraclesu](https://github.com/miraclesu)
 * [Paul Mach](https://github.com/paulmach)
 * [Randall McPherson](https://github.com/rlmcpherson)
 * [Sam Grimee](https://github.com/sgrimee)

## Maintainer

Original author and maintainer of gearman-go:

 * [Xing Xing](http://mikespook.com) &lt;<mikespook@gmail.com>&gt; [@Twitter](http://twitter.com/mikespook)

This fork is maintained by:

 * [nook24](https://github.com/nook24)

## Open Source - MIT Software License

Copyright (C) 2011 by Xing Xing. The license is unchanged from upstream — see
[LICENSE](LICENSE).
