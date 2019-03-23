package backend

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/skroutz/downloader/job"
)

var (
	// Based on http.DefaultTransport
	//
	// See https://golang.org/pkg/net/http/#RoundTripper
	httpBackendTransport = &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second, // was 30 * time.Second
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
)

// HTTPBackend notifies about a job completion by executing an HTTP request.
type HTTPBackend struct {
	client  *http.Client
	reports chan job.CallbackInfo
}

// ID returns the identifier code of the respective backend
// which in this case is 'http'
func (b *HTTPBackend) ID() string {
	return "http"
}

// Start initializes the backend by setting an http client
// and a channel for reports delivering.
func (b *HTTPBackend) Start(ctx context.Context, cfg map[string]interface{}) error {
	b.client = &http.Client{
		Transport: httpBackendTransport,
		Timeout:   30 * time.Second, // Larger than Dial + TLS timeouts
	}

	b.reports = make(chan job.CallbackInfo)

	return nil
}

// Notify sends the appropriate payload using HTTP
// to the instructed destination
func (b *HTTPBackend) Notify(destination string, payload []byte) error {
	res, err := b.client.Post(destination, "application/json", bytes.NewBuffer(payload))
	if err != nil || res.StatusCode < 200 || res.StatusCode >= 300 {
		if err == nil {
			err = fmt.Errorf("Received Status: %s", res.Status)
		}
		return err
	}

	if res.StatusCode == http.StatusAccepted || res.StatusCode == http.StatusOK {
		var cbInfo job.CallbackInfo
		json.Unmarshal(payload, &cbInfo)
		b.reports <- cbInfo
	}

	return nil
}

// DeliveryReports returns a channel of successfully emmited callbacks
func (b *HTTPBackend) DeliveryReports() <-chan job.CallbackInfo {
	return b.reports
}

// Finalize closes the reports channel
func (b *HTTPBackend) Finalize() error {
	close(b.reports)
	return nil
}
