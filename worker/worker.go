// The worker package helps developers to develop Gearman's worker
// in an easy way.
package worker

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Unlimited = iota
	OneByOne
)

// Worker is the only structure needed by worker side developing.
// It can connect to multi-server and grab jobs.
type Worker struct {
	sync.Mutex
	agents []*agent
	funcs  jobFuncs
	in     chan *inPack
	// ready is written by Ready and read by Work, which callers routinely
	// run on different goroutines - Work calls Ready itself when it was
	// not called beforehand. Atomic for the same reason running is: a
	// plain bool here is a data race the detector flags as soon as
	// anything observes the worker coming up.
	ready atomic.Bool

	// running is read from exec on the job goroutines while Close writes
	// it, so it cannot be a plain bool guarded only by the worker mutex -
	// exec does not hold that mutex.
	running atomic.Bool

	// agentWG counts the per-connection agent goroutines that send on
	// worker.in. Close waits on it before closing that channel: without
	// it, closing races the agents' sends, which is a data race and a
	// "send on closed channel" panic in a goroutine the caller does not
	// own and cannot recover from.
	agentWG sync.WaitGroup

	// draining is set by Close before anything is torn down. While it is
	// set, handleInPack starts no new jobs and stops asking the servers
	// for more, but the connections stay open so the jobs already running
	// can still report their result.
	draining atomic.Bool

	// execWG counts the job goroutines handleInPack starts. Close waits on
	// it while running is still true, which is what allows exec to send
	// WORK_COMPLETE for those jobs.
	//
	// Without this, a job in flight when Close is called finishes its work
	// and then silently skips its acknowledgement, because exec only
	// writes the response while running is true. The server never hears
	// that the job completed, so on disconnect it re-queues it and hands
	// it to the next worker that connects. The job is then executed twice
	// - once invisibly - which for anything that is not idempotent means
	// duplicated or, if the second run collides with the first, lost work.
	execWG sync.WaitGroup

	Id           string
	ErrorHandler ErrorHandler
	JobHandler   JobHandler
	limit        chan bool
}

// New returns a worker.
//
// If limit is set to Unlimited(=0), the worker will grab all jobs
// and execute them parallelly.
// If limit is greater than zero, the number of paralled executing
// jobs are limited under the number. If limit is assgined to
// OneByOne(=1), there will be only one job executed in a time.
func New(limit int) (worker *Worker) {
	worker = &Worker{
		agents: make([]*agent, 0, limit),
		funcs:  make(jobFuncs),
		in:     make(chan *inPack, queueSize),
	}
	if limit != Unlimited {
		worker.limit = make(chan bool, limit-1)
	}
	return
}

// inner error handling
func (worker *Worker) err(e error) {
	if worker.ErrorHandler != nil {
		worker.ErrorHandler(e)
	}
}

// AddServer adds a Gearman job server.
//
// addr should be formated as 'host:port'.
func (worker *Worker) AddServer(net, addr string) (err error) {
	// Create a new job server's client as a agent of server
	a, err := newAgent(net, addr, worker)
	if err != nil {
		return err
	}
	worker.agents = append(worker.agents, a)
	return
}

// Broadcast an outpack to all Gearman server.
func (worker *Worker) broadcast(outpack *outPack) {
	for _, v := range worker.agents {
		v.Write(outpack)
	}
}

// AddFunc adds a function.
// Set timeout as Unlimited(=0) to disable executing timeout.
func (worker *Worker) AddFunc(funcname string,
	f JobFunc, timeout uint32) (err error) {
	worker.Lock()
	defer worker.Unlock()
	if _, ok := worker.funcs[funcname]; ok {
		return fmt.Errorf("The function already exists: %s", funcname)
	}
	worker.funcs[funcname] = &jobFunc{f: f, timeout: timeout}
	if worker.running.Load() {
		worker.addFunc(funcname, timeout)
	}
	return
}

// inner add
func (worker *Worker) addFunc(funcname string, timeout uint32) {
	outpack := prepFuncOutpack(funcname, timeout)
	worker.broadcast(outpack)
}

func prepFuncOutpack(funcname string, timeout uint32) *outPack {
	outpack := getOutPack()
	if timeout == 0 {
		outpack.dataType = dtCanDo
		outpack.data = []byte(funcname)
	} else {
		outpack.dataType = dtCanDoTimeout
		l := len(funcname)

		timeoutString := strconv.FormatUint(uint64(timeout), 10)
		outpack.data = getBuffer(l + len(timeoutString) + 1)
		copy(outpack.data, []byte(funcname))
		outpack.data[l] = '\x00'
		copy(outpack.data[l+1:], []byte(timeoutString))
	}
	return outpack
}

// RemoveFunc removes a function.
func (worker *Worker) RemoveFunc(funcname string) (err error) {
	worker.Lock()
	defer worker.Unlock()
	if _, ok := worker.funcs[funcname]; !ok {
		return fmt.Errorf("The function does not exist: %s", funcname)
	}
	delete(worker.funcs, funcname)
	if worker.running.Load() {
		worker.removeFunc(funcname)
	}
	return
}

// inner remove
func (worker *Worker) removeFunc(funcname string) {
	outpack := getOutPack()
	outpack.dataType = dtCantDo
	outpack.data = []byte(funcname)
	worker.broadcast(outpack)
}

// inner package handling
func (worker *Worker) handleInPack(inpack *inPack) {
	switch inpack.dataType {
	case dtNoJob:
		inpack.a.PreSleep()
	case dtNoop:
		inpack.a.Grab()
	case dtJobAssign, dtJobAssignUniq:
		// Close is already draining: do not start this job, and do not ask
		// for another. The job was never begun, so leaving it unanswered is
		// correct - the server re-queues it and a later worker runs it once.
		if worker.draining.Load() {
			return
		}
		worker.execWG.Add(1)
		go func() {
			defer worker.execWG.Done()
			if err := worker.exec(inpack); err != nil {
				worker.err(err)
			}
		}()
		if worker.limit != nil {
			worker.limit <- true
		}
		inpack.a.Grab()
	case dtError:
		worker.err(inpack.Err())
		fallthrough
	case dtEchoRes:
		fallthrough
	default:
		worker.customeHandler(inpack)
	}
}

// Connect to Gearman server and tell every server
// what can this worker do.
func (worker *Worker) Ready() (err error) {
	if len(worker.agents) == 0 {
		return ErrNoneAgents
	}
	if len(worker.funcs) == 0 {
		return ErrNoneFuncs
	}
	for _, a := range worker.agents {
		if err = a.Connect(); err != nil {
			return
		}
	}
	for funcname, f := range worker.funcs {
		worker.addFunc(funcname, f.timeout)
	}
	worker.ready.Store(true)
	return
}

// Work start main loop (blocking)
// Most of time, this should be evaluated in goroutine.
func (worker *Worker) Work() {
	if !worker.ready.Load() {
		// didn't run Ready beforehand, so we'll have to do it:
		err := worker.Ready()
		if err != nil {
			panic(err)
		}
	}

	worker.Lock()
	worker.running.Store(true)
	worker.Unlock()

	for _, a := range worker.agents {
		a.Grab()
	}
	var inpack *inPack
	for inpack = range worker.in {
		worker.handleInPack(inpack)
	}
}

// custome handling warper
func (worker *Worker) customeHandler(inpack *inPack) {
	if worker.JobHandler != nil {
		if err := worker.JobHandler(inpack); err != nil {
			worker.err(err)
		}
	}
}

// DrainTimeout bounds how long Close waits for jobs that are already
// running to report their result. A handler that never returns must not
// turn a shutdown into a hang; once this elapses the remaining jobs lose
// their acknowledgement and the server will hand them out again, which is
// exactly what happened to every in-flight job before draining existed.
var DrainTimeout = 30 * time.Second

// Close stops the worker: it lets the jobs that are already running finish
// and acknowledge, then disconnects and exits the main loop.
//
// The order matters and is the opposite of what it used to be. Marking the
// worker as not running, or closing the connections, before those jobs
// report back means exec silently skips their WORK_COMPLETE - the server
// never learns they finished, re-queues them on disconnect and hands them
// to the next worker, so each is executed twice with the first run
// invisible to the client.
func (worker *Worker) Close() {
	worker.Lock()
	if !worker.running.Load() || worker.draining.Load() {
		worker.Unlock()
		return
	}
	// Not running=false yet: exec checks that flag before writing a job's
	// response, so it has to stay true until the jobs below are done.
	// draining is what stops new ones from starting in the meantime, and
	// it doubles as the guard against a second concurrent Close.
	worker.draining.Store(true)
	agents := make([]*agent, len(worker.agents))
	copy(agents, worker.agents)
	worker.Unlock()

	if !waitTimeout(&worker.execWG, DrainTimeout) {
		worker.err(ErrDrainTimeout)
	}

	worker.running.Store(false)

	// Closing an agent's connection makes its read fail, which is how
	// its work() goroutine breaks out of the read loop and returns.
	for _, a := range agents {
		a.Close()
	}

	// An agent sitting in "worker.in <- inpack" can only return once
	// somebody receives that packet. Work() normally does, but it may
	// itself be parked on the concurrency limit, so drain here too -
	// otherwise the wait below could sit out an in-flight job's runtime.
	// Packets picked up here are dropped, which is what closing means.
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for range worker.in {
		}
	}()

	// The whole point: no agent can still be sending by the time the
	// channel is closed.
	worker.agentWG.Wait()
	close(worker.in)
	<-drained
}

// waitTimeout waits for wg, reporting false if d elapses first.
//
// The helper goroutine outlives a timed-out call until the WaitGroup does
// reach zero. That is deliberate: it only holds a channel, and blocking
// Close on a job that may never return would be the worse trade.
func waitTimeout(wg *sync.WaitGroup, d time.Duration) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

// Echo
func (worker *Worker) Echo(data []byte) {
	outpack := getOutPack()
	outpack.dataType = dtEchoReq
	outpack.data = data
	worker.broadcast(outpack)
}

// Reset removes all of functions.
// Both from the worker and job servers.
func (worker *Worker) Reset() {
	outpack := getOutPack()
	outpack.dataType = dtResetAbilities
	worker.broadcast(outpack)
	worker.funcs = make(jobFuncs)
}

// Set the worker's unique id.
func (worker *Worker) SetId(id string) {
	worker.Id = id
	outpack := getOutPack()
	outpack.dataType = dtSetClientId
	outpack.data = []byte(id)
	worker.broadcast(outpack)
}

// inner job executing
func (worker *Worker) exec(inpack *inPack) (err error) {
	defer func() {
		if worker.limit != nil {
			<-worker.limit
		}
		if r := recover(); r != nil {
			if e, ok := r.(error); ok {
				err = e
			} else {
				err = ErrUnknown
			}
		}
	}()
	f, ok := worker.funcs[inpack.fn]
	if !ok {
		return fmt.Errorf("The function does not exist: %s", inpack.fn)
	}
	var r *result
	if f.timeout == 0 {
		d, e := f.f(inpack)
		r = &result{data: d, err: e}
	} else {
		r = execTimeout(f.f, inpack, time.Duration(f.timeout)*time.Second)
	}
	if worker.running.Load() {
		outpack := getOutPack()
		if r.err == nil {
			outpack.dataType = dtWorkComplete
		} else {
			if len(r.data) == 0 {
				outpack.dataType = dtWorkFail
			} else {
				outpack.dataType = dtWorkException
			}
			err = r.err
		}
		outpack.handle = inpack.handle
		outpack.data = r.data
		inpack.a.Write(outpack)
	}
	return
}
func (worker *Worker) reRegisterFuncsForAgent(a *agent) {
	worker.Lock()
	defer worker.Unlock()
	for funcname, f := range worker.funcs {
		outpack := prepFuncOutpack(funcname, f.timeout)
		a.write(outpack)
	}

}

// inner result
type result struct {
	data []byte
	err  error
}

// executing timer
func execTimeout(f JobFunc, job Job, timeout time.Duration) (r *result) {
	rslt := make(chan *result)
	defer close(rslt)
	go func() {
		defer func() { recover() }()
		d, e := f(job)
		rslt <- &result{data: d, err: e}
	}()
	select {
	case r = <-rslt:
	case <-time.After(timeout):
		return &result{err: ErrTimeOut}
	}
	return r
}

// Error type passed when a worker connection disconnects
type WorkerDisconnectError struct {
	err   error
	agent *agent
}

func (e *WorkerDisconnectError) Error() string {
	return e.err.Error()
}

// Responds to the error by asking the worker to reconnect
func (e *WorkerDisconnectError) Reconnect() (err error) {
	return e.agent.reconnect()
}

// Which server was this for?
func (e *WorkerDisconnectError) Server() (net string, addr string) {
	return e.agent.net, e.agent.addr
}
