package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		t.Setenv("PAYMENTS_DISPATCHER_DB_USER", "testuser")
		t.Setenv("PAYMENTS_DISPATCHER_DB_PASSWORD", "testpass")
		t.Setenv("PAYMENTS_DISPATCHER_DB_HOST", "localhost")
		t.Setenv("PAYMENTS_DISPATCHER_DB_NAME", "testdb")
		t.Setenv("PAYMENTS_DISPATCHER_HMAC_SECRET", "test-secret")

		cfg, err := New()

		require.NoError(t, err)
		require.NotNil(t, cfg)

		assert.Equal(t, "testuser", cfg.DB.User)
		assert.Equal(t, "testpass", cfg.DB.Password)
		assert.Equal(t, "localhost", cfg.DB.Host)
		assert.Equal(t, "testdb", cfg.DB.Name)
		assert.Equal(t, "test-secret", cfg.Dispatcher.HMACSecret)

		assert.Equal(t, 5432, cfg.DB.Port)
		assert.Equal(t, "disable", cfg.DB.SSLMode)
		assert.Equal(t, 10, cfg.DB.MaxConns)
		assert.Equal(t, 10, cfg.DB.MaxIdleConns)
		assert.Equal(t, 8080, cfg.HTTP.Port)
		assert.Equal(t, 8081, cfg.GRPC.Port)
		assert.Equal(t, 5*time.Second, cfg.Poller.Frequency)
		assert.Equal(t, 10, cfg.Poller.MaxRetries)
		assert.Equal(t, time.Second, cfg.Poller.MinRetryDelay)
		assert.Equal(t, 15*time.Second, cfg.Poller.MaxRetryDelay)
		assert.Equal(t, 10, cfg.Poller.MaxJobs)
	})

	t.Run("FailureMissingRequired", func(t *testing.T) {
		t.Setenv("PAYMENTS_DISPATCHER_DB_USER", "testuser")
		t.Setenv("PAYMENTS_DISPATCHER_DB_PASSWORD", "testpass")
		t.Setenv("PAYMENTS_DISPATCHER_DB_HOST", "localhost")
		t.Setenv("PAYMENTS_DISPATCHER_DB_NAME", "testdb")

		cfg, err := New()

		require.Error(t, err)
		assert.Nil(t, cfg)
		assert.Contains(t, err.Error(), "PAYMENTS_DISPATCHER_HMAC_SECRET")
	})

	t.Run("SuccessWithDSN", func(t *testing.T) {
		dbConfig := &Database{
			User:     "testuser",
			Password: "testpassword",
			Host:     "testhost",
			Port:     1234,
			Name:     "testdb",
		}

		assert.Equal(t, "postgres://testuser:testpassword@testhost:1234/testdb", dbConfig.DSN())
	})
}
