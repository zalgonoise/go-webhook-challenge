package main

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"

	"github.com/zalgonoise/go-webhook-challenge/internal/log"
)

func main() {
	logger := log.New("debug")

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/accept", acceptHandler(logger))

	port := "8090"
	logger.Info("starting accepter server", slog.String("port", port))

	if err := http.ListenAndServe(":"+port, mux); err != nil {
		logger.Error("server failed to start", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func acceptHandler(logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		if r.Method != http.MethodPost {
			logger.ErrorContext(ctx, "non-POST method", slog.String("method", r.Method))

			http.Error(w, "only POST method is accepted", http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			logger.ErrorContext(ctx, "failed to read request body", slog.String("error", err.Error()))

			http.Error(w, "failed to read request body", http.StatusInternalServerError)
			return
		}

		logger.InfoContext(ctx, "received webhook", slog.String("payload", string(body)), slog.Any("headers", r.Header))

		w.WriteHeader(http.StatusOK)
		if _, err := fmt.Fprintln(w, "webhook accepted"); err != nil {
			logger.WarnContext(ctx, "failed to write response")
		}
	}
}
