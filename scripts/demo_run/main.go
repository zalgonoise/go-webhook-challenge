package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/zalgonoise/go-webhook-challenge/internal/log"
)

type Request struct {
	EventType      string `json:"event_type"`
	DestinationURL string `json:"destination_url"`
	Data           Data   `json:"data"`
}

type Data struct {
	Key string `json:"key"`
}

func main() {
	logger := log.New("info")
	ctx := context.Background()

	acceptPayload := Request{
		EventType:      "payment.created",
		DestinationURL: "http://accepter:8090/v1/accept",
		Data: Data{
			Key: "value",
		},
	}

	rejectPayload := Request{
		EventType:      "payment.created",
		DestinationURL: "http://rejecter:8091/v1/reject",
		Data: Data{
			Key: "value",
		},
	}

	client := &http.Client{
		Timeout: 20 * time.Second,
	}

	logger.InfoContext(ctx, "starting an example accept receiver request")
	newReq(ctx, client, logger, "http://localhost:8090/v1/accept", acceptPayload)

	time.Sleep(time.Second)

	logger.InfoContext(ctx, "starting an example reject receiver request")
	newReq(ctx, client, logger, "http://localhost:8091/v1/reject", rejectPayload)

	time.Sleep(time.Second)

	logger.InfoContext(ctx, "sending a single dispatch job to the server")
	newReq(ctx, client, logger, "http://localhost:8080/v1/dispatch", acceptPayload)

	time.Sleep(time.Second)

	logger.InfoContext(ctx, "sending a single *failing* dispatch job to the server")
	newReq(ctx, client, logger, "http://localhost:8080/v1/dispatch", rejectPayload)

	time.Sleep(time.Second)

	logger.InfoContext(ctx, "sending twenty (20) dispatch jobs to the server")
	for range 20 {
		newReq(ctx, client, logger, "http://localhost:8080/v1/dispatch", acceptPayload)
	}

	logger.InfoContext(ctx, "completed example requests from client")
}

func newReq[T any](ctx context.Context, client *http.Client, logger *slog.Logger, uri string, data T) {
	buf, err := json.Marshal(data)
	if err != nil {
		slog.ErrorContext(ctx, "invalid JSON data", slog.String("error", err.Error()))

		os.Exit(1)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, uri, bytes.NewReader(buf))
	if err != nil {
		logger.ErrorContext(ctx, "failed to create request", slog.String("error", err.Error()))

		os.Exit(1)
	}

	res, err := client.Do(req)
	if err != nil {
		logger.ErrorContext(ctx, "failed to issue HTTP request", slog.String("error", err.Error()))

		os.Exit(1)
	}

	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		logger.WarnContext(ctx, "failed to read HTTP response body", slog.String("error", err.Error()))
	}

	slog.InfoContext(ctx, "executed HTTP request",
		slog.Int("status_code", res.StatusCode),
		slog.Any("body", body),
	)
}
