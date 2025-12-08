package pgc

import "time"

// SetWorkers sets the number of worker goroutines.
//
// Parameters:
//   - n: Number of workers (must be > 0).
//
// Returns:
//   - Pointer to PoolConf for method chaining.
func (c *PoolConf) SetWorkers(n int) *PoolConf {
	if n > 0 {
		c.Workers = n
	}
	return c
}

// SetQueueSize sets the job queue buffer size.
//
// Parameters:
//   - size: Queue size (must be > 0).
//
// Returns:
//   - Pointer to PoolConf for method chaining.
func (c *PoolConf) SetQueueSize(size int) *PoolConf {
	if size > 0 {
		c.QueueSize = size
	}
	return c
}

// SetDropOnFull configures whether to drop jobs when queue is full.
//
// Parameters:
//   - drop: If true, jobs are dropped; if false, Submit blocks.
//
// Returns:
//   - Pointer to PoolConf for method chaining.
func (c *PoolConf) SetDropOnFull(drop bool) *PoolConf {
	c.DropOnFull = drop
	return c
}

// SetGracePeriod sets the maximum duration for graceful shutdown.
//
// Parameters:
//   - d: Duration to wait before forcing shutdown.
//
// Returns:
//   - Pointer to PoolConf for method chaining.
func (c *PoolConf) SetGracePeriod(d time.Duration) *PoolConf {
	if d > 0 {
		c.GracePeriod = d
	}
	return c
}
