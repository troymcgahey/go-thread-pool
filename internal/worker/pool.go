package worker

import (
	"context"
	"net/http"
	"time"
)

type Job struct {
	Ctx      context.Context
	URL      string
	Response chan Result
}

type Result struct {
	StatusCode int
	error      error
}

type Pool struct {
	jobs   chan Job
	client *http.Client
}

func NewPool(workerCount int, queueSize int) *Pool {
	p := &Pool{
		jobs: make(chan Job, queueSize),
		client: &http.Client{
			Timeout: 5 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 20,
				MaxConnsPerHost:     50,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}

	for i := 0; i < workerCount; i++ {
		go p.worker()
	}

	return p
}

func (p *Pool) worker() {
	for job := range p.jobs {
		req, err := http.NewRequestWithContext(job.Ctx, http.MethodGet, job.URL, nil)
		if err != nil {
			job.Response <- Result{Err: err}
			continue
		}

		resp, err := p.client.Do(req)
		if err != nil {
			job.Response <- Result{Err: err}
			continue
		}

		resp.Body.Close()

		job.Response <- Result{
			StatusCode: resp.StatusCode,
		}
	}
}
