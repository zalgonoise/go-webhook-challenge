package config

import (
	"fmt"
	"time"

	"github.com/cloudresty/go-env"
)

type Config struct {
	DB         Database
	Poller     Poller
	Dispatcher Dispatcher
	HTTP       HTTP
	GRPC       GRPC
}

type Database struct {
	User         string `env:"PAYMENTS_DISPATCHER_DB_USER,required=true"`
	Password     string `env:"PAYMENTS_DISPATCHER_DB_PASSWORD,required=true"`
	Host         string `env:"PAYMENTS_DISPATCHER_DB_HOST,required=true"`
	Port         int    `env:"PAYMENTS_DISPATCHER_DB_PORT,default=5432"`
	Name         string `env:"PAYMENTS_DISPATCHER_DB_NAME,required=true"`
	SSLMode      string `env:"PAYMENTS_DISPATCHER_DB_SSL_MODE,default=disable"`
	MaxConns     int    `env:"PAYMENTS_DISPATCHER_DB_MAX_CONNS,default=10"`
	MaxIdleConns int    `env:"PAYMENTS_DISPATCHER_DB_MAX_IDLE_CONNS,default=10"`
}

// DSN returns the data source name string for connecting to the database.
func (d *Database) DSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
		d.User, d.Password, d.Host, d.Port, d.Name)
}

type Poller struct {
	Frequency     time.Duration `env:"PAYMENTS_DISPATCHER_POLLER_FREQUENCY,default=5s"`
	MaxRetries    int           `env:"PAYMENTS_DISPATCHER_POLLER_MAX_RETRIES,default=10"`
	MinRetryDelay time.Duration `env:"PAYMENTS_DISPATCHER_POLLER_MIN_RETRY_DELAY,default=1s"`
	MaxRetryDelay time.Duration `env:"PAYMENTS_DISPATCHER_POLLER_MAX_RETRY_DELAY,default=15s"`
	MaxJobs       int           `env:"PAYMENTS_DISPATCHER_POLLER_MAX_JOBS,default=10"`
}

type Dispatcher struct {
	HMACSecret string `env:"PAYMENTS_DISPATCHER_HMAC_SECRET,required=true"`
}

type HTTP struct {
	Port int `env:"PAYMENTS_DISPATCHER_HTTP_PORT,default=8080"`
}

type GRPC struct {
	Port int `env:"PAYMENTS_DISPATCHER_GRPC_PORT,default=8081"`
}

func New() (*Config, error) {
	var config Config

	if err := env.Bind(&config, env.DefaultBindingOptions()); err != nil {
		return nil, err
	}

	return &config, nil
}
