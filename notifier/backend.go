package notifier

import "github.com/skroutz/downloader/job"

// Backend is the interface that wraps the basic Notify method.
//
// Backend implementations are responsible for notifying about a job
// completion through some notification channel (eg. HTTP, Kafka).
type Backend interface {
	// Start() initializes the backend. Start() must be called once, before
	// any calls to Notify.
	Start() error

	// Notify notifies about a job completion. Depending on the underlying
	// implementation, Notify might be an asynchronous operation so a nil
	// error does NOT necessarily mean the notification was delivered.
	//
	// Implementations should peek job.CallbackDst to determine the
	// destination of the notification.
	Notify(*job.Job) error

	// ID returns a constant string used as an identifier for the
	// concrete backend implementation.
	ID() string

	// Errors returns a channel on which notification delivery errors are
	// emmitted.
	//Errors() <-chan error
}
