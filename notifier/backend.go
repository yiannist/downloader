package notifier

import "github.com/skroutz/downloader/job"

// Backend is the interface that wraps the basic Notify method.
//
// Backend implementations are responsible for notifying about a job
// completion through some notification channel, denoted by job.CallbackDst.
//
// Users should call Setup() once at the beginning before any calls to Notify.
type Backend interface {
	// TODO
	ID() string

	// Start() initializes the backend. Start() must be called once, before
	// any calls to Notify.
	Start() error

	// Notify notifies about a job completion.
	Notify(*job.Job) error
}
