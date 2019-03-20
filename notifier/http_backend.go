package notifier

import (
	"net/http"

	"github.com/skroutz/downloader/job"
)

// HttpBackend notifies about a job completion by executing an HTTP request.
type HttpBackend struct {
	client *http.Client
}

func (b *HttpBackend) ID() string {
	return "http"
}

func (b *HttpBackend) Start() error {
	// TODO: initialize client
	return nil
}

func (b *HttpBackend) Notify(j *job.Job) error {
	return nil
}
