package backend

import (
	"context"

	"github.com/skroutz/downloader/job"
)

// Backend is the interface that wraps the basic Notify method.
//
// Backend implementations are responsible for notifying about a job
// completion through some notification channel (eg. HTTP, Kafka).
type Backend interface {
	// Start() initializes the backend. Start() must be called once, before
	// any calls to Notify.
	Start(context.Context, map[string]interface{}) error

	// Notify notifies about a job completion. Depending on the underlying
	// implementation, Notify might be an asynchronous operation so a nil
	// error does NOT necessarily mean the notification was delivered.
	//
	// Implementations should peek job.CallbackDst to determine the
	// destination of the notification.
	Notify(string, []byte) error

	// ID returns a constant string used as an identifier for the
	// concrete backend implementation.
	ID() string

	// DeliveryReports returns a channel of callback info objects signifying
	// deliveries of jobs callbacks. Even if a message recieved from
	// this channel is successful that does not mean that the callback
	// has been consumend on the other end.
	DeliveryReports() <-chan job.CallbackInfo

	// Finalize closes the delivery reports channel and performs finalization
	// actions.
	Finalize() error

	// Errors returns a channel on which notification delivery errors are
	// emmitted.
	//Errors() <-chan error
}
