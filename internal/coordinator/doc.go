// Package coordinator reserves the deterministic execution boundary. In M2 it
// will own durable assignments, tasks, attempts, worker allocations and events.
// Supervisor judgment stays outside this package. M1 supplies no running service.
package coordinator
