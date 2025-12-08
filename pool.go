package pgc

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sivaosorg/loggy"
)

// PoolState represents the current operational state of the worker pool.
//
// States:
//   - PoolStateIdle:     Pool is created but not started.
//   - PoolStateRunning:  Pool is actively processing jobs.
//   - PoolStateStopping: Pool is in the process of shutting down.
//   - PoolStateStopped:  Pool has been fully stopped.
type PoolState int32

const (
	PoolStateIdle PoolState = iota
	PoolStateRunning
	PoolStateStopping
	PoolStateStopped
)

// String returns the string representation of a PoolState.
func (s PoolState) String() string {
	switch s {
	case PoolStateIdle:
		return "idle"
	case PoolStateRunning:
		return "running"
	case PoolStateStopping:
		return "stopping"
	case PoolStateStopped:
		return "stopped"
	default:
		return "unknown"
	}
}

// PoolConf holds configuration options for a worker pool.
//
// Fields:
//   - Workers:      Number of worker goroutines (default: runtime.NumCPU()).
//   - QueueSize:    Size of the job queue buffer (default: 1024).
//   - DropOnFull:   If true, drops jobs when queue is full; otherwise blocks.
//   - GracePeriod:  Maximum duration to wait for graceful shutdown.
type PoolConf struct {
	Workers     int
	QueueSize   int
	DropOnFull  bool
	GracePeriod time.Duration
}

// DefaultPoolConf returns a PoolConf with sensible default values.
//
// Defaults:
//   - Workers:      runtime.NumCPU()
//   - QueueSize:    1024
//   - DropOnFull:   true
//   - GracePeriod:  5 seconds
func DefaultPoolConf() PoolConf {
	return PoolConf{
		Workers:     runtime.NumCPU(),
		QueueSize:   1024,
		DropOnFull:  true,
		GracePeriod: 5 * time.Second,
	}
}

// PoolStats holds runtime statistics for a worker pool.
//
// Fields:
//   - Submitted:  Total number of jobs submitted.
//   - Completed:  Total number of jobs successfully completed.
//   - Dropped:    Total number of jobs dropped due to full queue.
//   - Panics:     Total number of panics recovered in workers.
//   - Pending:    Current number of jobs waiting in queue.
type PoolStats struct {
	Submitted uint64
	Completed uint64
	Dropped   uint64
	Panics    uint64
	Pending   int
}

// Job represents a unit of work to be executed by a worker.
// It encapsulates a function that performs the actual task.
type Job func()

// Pool represents a worker pool that manages a fixed number of goroutines
// to process jobs concurrently.  It provides backpressure handling, graceful
// shutdown, and runtime statistics.
//
// Fields:
//   - conf:       Pool configuration.
//   - jobs:       Buffered channel for job queue.
//   - wg:         WaitGroup to track active workers.
//   - ctx:        Context for cancellation.
//   - cancel:     Cancel function for context.
//   - state:      Current pool state (atomic).
//   - submitted:  Counter for submitted jobs (atomic).
//   - completed:  Counter for completed jobs (atomic).
//   - dropped:    Counter for dropped jobs (atomic).
//   - panics:     Counter for recovered panics (atomic).
//   - mu:         Mutex for state transitions.
type Pool struct {
	conf      PoolConf
	jobs      chan Job
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	state     int32
	submitted uint64
	completed uint64
	dropped   uint64
	panics    uint64
	mu        sync.Mutex
}

// NewPool creates a new worker pool with the given configuration.
// The pool is created in idle state and must be started with Start().
//
// Parameters:
//   - conf: Configuration options for the pool.
//
// Returns:
//   - A pointer to a new Pool instance.
//
// Example:
//
//	conf := pgc.DefaultPoolConf()
//	conf.Workers = 8
//	pool := pgc.NewPool(conf)
//	pool.Start()
func NewPool(conf PoolConf) *Pool {
	if conf.Workers <= 0 {
		conf.Workers = runtime.NumCPU()
	}
	if conf.QueueSize <= 0 {
		conf.QueueSize = 1024
	}
	if conf.GracePeriod <= 0 {
		conf.GracePeriod = 5 * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &Pool{
		conf:   conf,
		jobs:   make(chan Job, conf.QueueSize),
		ctx:    ctx,
		cancel: cancel,
		state:  int32(PoolStateIdle),
	}
}

// Start initializes and starts all worker goroutines.
// It transitions the pool from idle to running state.
// Calling Start on an already running pool has no effect.
//
// Example:
//
//	pool := pgc.NewPool(pgc.DefaultPoolConf())
//	pool.Start()
//	defer pool.Stop()
func (p *Pool) Start() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if PoolState(atomic.LoadInt32(&p.state)) != PoolStateIdle {
		return
	}

	atomic.StoreInt32(&p.state, int32(PoolStateRunning))

	for i := 0; i < p.conf.Workers; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}
}

// worker is the main loop for a worker goroutine.
// It continuously pulls jobs from the queue and executes them.
//
// Parameters:
//   - id: The worker's unique identifier for logging.
func (p *Pool) worker(id int) {
	defer p.wg.Done()

	for {
		select {
		case <-p.ctx.Done():
			// Drain remaining jobs before exit
			p.drain()
			return
		case job, ok := <-p.jobs:
			if !ok {
				return
			}
			p.execute(job)
		}
	}
}

// drain processes any remaining jobs in the queue during shutdown.
func (p *Pool) drain() {
	for {
		select {
		case job, ok := <-p.jobs:
			if !ok {
				return
			}
			p.execute(job)
		default:
			return
		}
	}
}

// execute safely runs a job with panic recovery.
//
// Parameters:
//   - job: The job function to execute.
func (p *Pool) execute(job Job) {
	defer func() {
		if r := recover(); r != nil {
			atomic.AddUint64(&p.panics, 1)
			loggy.Errorf("[pgc.pool] panic recovered in worker: %v", r)
		}
	}()

	job()
	atomic.AddUint64(&p.completed, 1)
}

// Submit adds a job to the pool's queue for execution.
// Behavior when queue is full depends on DropOnFull configuration:
//   - If true: Job is dropped and method returns false.
//   - If false: Method blocks until space is available.
//
// Parameters:
//   - job: The job function to submit.
//
// Returns:
//   - true if job was successfully queued.
//   - false if job was dropped (only when DropOnFull is true).
//
// Example:
//
//	pool. Submit(func() {
//	    // perform work
//	})
func (p *Pool) Submit(job Job) bool {
	if job == nil {
		return false
	}

	state := PoolState(atomic.LoadInt32(&p.state))
	if state != PoolStateRunning {
		return false
	}

	atomic.AddUint64(&p.submitted, 1)

	if p.conf.DropOnFull {
		select {
		case p.jobs <- job:
			return true
		default:
			atomic.AddUint64(&p.dropped, 1)
			return false
		}
	}

	select {
	case p.jobs <- job:
		return true
	case <-p.ctx.Done():
		atomic.AddUint64(&p.dropped, 1)
		return false
	}
}

// Stop gracefully shuts down the pool, waiting for all workers to complete.
// It waits up to GracePeriod for workers to finish before forcing shutdown.
//
// Returns:
//   - true if shutdown completed within grace period.
//   - false if shutdown was forced due to timeout.
//
// Example:
//
//	if pool.Stop() {
//	    loggy.Info("Pool stopped gracefully")
//	} else {
//	    loggy. Warn("Pool shutdown timed out")
//	}
func (p *Pool) Stop() bool {
	p.mu.Lock()
	state := PoolState(atomic.LoadInt32(&p.state))
	if state != PoolStateRunning {
		p.mu.Unlock()
		return true
	}
	atomic.StoreInt32(&p.state, int32(PoolStateStopping))
	p.mu.Unlock()

	p.cancel()

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		atomic.StoreInt32(&p.state, int32(PoolStateStopped))
		close(p.jobs)
		return true
	case <-time.After(p.conf.GracePeriod):
		atomic.StoreInt32(&p.state, int32(PoolStateStopped))
		return false
	}
}

// Stats returns the current runtime statistics of the pool.
//
// Returns:
//   - PoolStats containing submitted, completed, dropped, panics, and pending counts.
//
// Example:
//
//	stats := pool.Stats()
//	loggy. Infof("Completed: %d, Dropped: %d", stats.Completed, stats.Dropped)
func (p *Pool) Stats() PoolStats {
	return PoolStats{
		Submitted: atomic.LoadUint64(&p.submitted),
		Completed: atomic.LoadUint64(&p.completed),
		Dropped:   atomic.LoadUint64(&p.dropped),
		Panics:    atomic.LoadUint64(&p.panics),
		Pending:   len(p.jobs),
	}
}

// State returns the current state of the pool.
//
// Returns:
//   - PoolState indicating the current operational state.
func (p *Pool) State() PoolState {
	return PoolState(atomic.LoadInt32(&p.state))
}

// IsRunning returns true if the pool is currently running.
func (p *Pool) IsRunning() bool {
	return p.State() == PoolStateRunning
}

// Pending returns the number of jobs currently waiting in the queue.
func (p *Pool) Pending() int {
	return len(p.jobs)
}
