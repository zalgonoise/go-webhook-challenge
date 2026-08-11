package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cloudresty/go-env"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/zalgonoise/go-webhook-challenge/internal/config"
	"github.com/zalgonoise/go-webhook-challenge/internal/database"
	"github.com/zalgonoise/go-webhook-challenge/internal/dispatcher"
	"github.com/zalgonoise/go-webhook-challenge/internal/grpcserver"
	"github.com/zalgonoise/go-webhook-challenge/internal/httpserver"
	"github.com/zalgonoise/go-webhook-challenge/internal/log"
	"github.com/zalgonoise/go-webhook-challenge/internal/poller"
	"github.com/zalgonoise/go-webhook-challenge/internal/repository"
	pb "github.com/zalgonoise/go-webhook-challenge/pb/dispatcher/v1"
	"github.com/zalgonoise/x/cli/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const shutdownTimeout = time.Minute

func main() {
	logger := log.New("debug")

	runner := cli.NewRunner("dispatcher",
		cli.WithExecutors(map[string]cli.Executor{
			"":      cli.Executable(ExecServe),
			"serve": cli.Executable(ExecServe),
		}),
	)

	cli.Run(runner, logger)
}

func ExecServe(ctx context.Context, logger *slog.Logger, args []string) (int, error) {
	fs := flag.NewFlagSet("dispatcher", flag.ExitOnError)

	envFile := fs.String("env", "", "path to .env file (overrides OS env)")

	if err := fs.Parse(args); err != nil {
		return 1, err
	}

	var (
		cfg *config.Config
		err error
	)

	switch {
	case *envFile != "":
		err = env.LoadFromFile(*envFile, cfg)
		if err != nil {
			logger.ErrorContext(ctx, "loading config from file", slog.String("error", err.Error()))

			return 1, err
		}
	default:
		cfg, err = config.New()
		if err != nil {
			logger.ErrorContext(ctx, "loading config from env", slog.String("error", err.Error()))

			return 1, err
		}
	}

	if cfg == nil {
		return 1, errors.New("nil configuration")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := database.Open(cfg.DB.DSN(), cfg.DB.MaxConns, cfg.DB.MaxIdleConns)
	if err != nil {
		logger.ErrorContext(ctx, "connecting to PostgreSQL database", slog.String("error", err.Error()))

		return 1, err
	}

	if err := database.Migrate(db); err != nil {
		logger.ErrorContext(ctx, "migrating database", slog.String("error", err.Error()))

		return 1, err
	}

	repo := repository.NewRepository(db)

	poll := poller.NewPoller(cfg.Poller, repo, logger)
	dispatch := dispatcher.NewDispatcher(cfg.Dispatcher.HMACSecret, repo, logger)

	go poll.Poll(ctx)

	httpServer, err := httpserver.NewServer(fmt.Sprintf(":%d", cfg.HTTP.Port))
	if err != nil {
		return 1, err
	}

	logger.DebugContext(ctx, "HTTP server is ready", slog.Int("port", cfg.HTTP.Port))
	logger.DebugContext(ctx, "preparing gRPC server")

	grpcServer, err := runGRPCServer(ctx, cfg.GRPC.Port, dispatch, httpServer, logger)
	if err != nil {
		return 1, err
	}

	logger.DebugContext(ctx, "gRPC server is ready", slog.Int("port", cfg.GRPC.Port))
	logger.DebugContext(ctx, "serving requests")

	go runHTTPServer(ctx, cfg.HTTP.Port, httpServer, logger)

	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)

	<-signalChannel

	cancel()
	logger.InfoContext(ctx, "shutting down", slog.Duration("timeout", shutdownTimeout))

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()

	if err = httpServer.Shutdown(shutdownCtx); err != nil {
		return 1, err
	}

	grpcServer.Shutdown(shutdownCtx)

	return 0, nil
}

func runGRPCServer(
	ctx context.Context,
	port int,
	dispatcher pb.DispatcherServiceServer,
	server *httpserver.Server,
	logger *slog.Logger,
) (*grpcserver.Server, error) {
	loggingOpts := []logging.Option{
		logging.WithLogOnEvents(logging.StartCall, logging.FinishCall),
	}

	grpcServer := grpcserver.NewServer(
		[]grpc.UnaryServerInterceptor{
			logging.UnaryServerInterceptor(log.InterceptorLogger(logger), loggingOpts...),
		}, []grpc.StreamServerInterceptor{
			logging.StreamServerInterceptor(log.InterceptorLogger(logger), loggingOpts...),
		},
	)

	if dispatcher != nil {
		grpcServer.RegisterDispatcherService(dispatcher)
	}

	logger.InfoContext(ctx, "listening on gRPC", slog.Int("port", port))

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, err
	}

	go func() {
		if serveErr := grpcServer.Serve(lis); serveErr != nil {
			logger.ErrorContext(ctx, "failed to start gRPC server",
				slog.String("error", serveErr.Error()),
				slog.Int("port", port),
			)
			os.Exit(1)
		}
	}()

	grpcClient, err := grpc.Dial(
		fmt.Sprintf("localhost:%d", port),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}

	if dispatcher != nil {
		dispatcherClient := pb.NewDispatcherServiceClient(grpcClient)

		if err = server.RegisterDispatcher(ctx, dispatcherClient); err != nil {
			return nil, err
		}
	}

	return grpcServer, nil
}

func runHTTPServer(ctx context.Context, port int, httpServer *httpserver.Server, logger *slog.Logger) {
	logger.InfoContext(ctx, "listening on http", slog.Int("port", port))

	err := httpServer.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.ErrorContext(ctx, "http server listen error", slog.String("error", err.Error()))
	}
}
