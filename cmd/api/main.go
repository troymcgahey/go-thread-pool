package main

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/troymcgahey/go-thread-pool/internal/worker"
)

func main() {
	pool := worker.NewPool(
		10,  //Number of workers
		100, //Queue size
	)

	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	//Prometheus metrics endpoint
	mux.Handle("/metrics", promhttp.Handler())

	mux.HandleFunc("/submit-request", func(w http.ResponseWriter, r *http.Request) {
		resultChan := make(chan worker.Result, 1)

		log.Println("On Submit Request Handler")

		job := worker.Job{
			Ctx:      r.Context(),
			URL:      "https://httpbin.org/status/200",
			Response: resultChan,
		}

		err := pool.Submit(job)
		if err != nil {
			http.Error(w, "server busy", http.StatusTooManyRequests)
			return
		}

		select {
		case result := <-resultChan:
			if result.Err != nil {
				http.Error(w, result.Err.Error(), http.StatusBadGateway)
				return
			}

			json.NewEncoder(w).Encode(map[string]any{
				"downstream_status": result.StatusCode,
			})

		case <-time.After(3 * time.Second):
			http.Error(w, "downstream timeout", http.StatusGatewayTimeout)
		case <-r.Context().Done():
			http.Error(w, "request cancelled", http.StatusRequestTimeout)
		}
	})

	log.Println("server listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", mux))
}
