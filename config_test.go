package workers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRedisPoolConfig(t *testing.T) {
	// Tests redis pool size which defaults to 1
	cfg, err := configFromOptions(Options{
		ServerAddr: "localhost:6379",
		ProcessID:  "2",
	})

	assert.NoError(t, err)
	assert.Equal(t, 1, cfg.Client.Options().PoolSize)

	cfg, err = configFromOptions(Options{
		ServerAddr: "localhost:6379",
		ProcessID:  "1",
		PoolSize:   20,
	})

	assert.NoError(t, err)
	assert.Equal(t, 20, cfg.Client.Options().PoolSize)
}

func TestCustomProcessConfig(t *testing.T) {
	cfg, err := configFromOptions(Options{
		ServerAddr: "localhost:6379",
		ProcessID:  "1",
	})

	assert.NoError(t, err)
	assert.Equal(t, "1", cfg.processId)

	cfg, err = configFromOptions(Options{
		ServerAddr: "localhost:6379",
		ProcessID:  "2",
	})

	assert.NoError(t, err)
	assert.Equal(t, "2", cfg.processId)
}

func TestRequiresRedisConfig(t *testing.T) {
	_, err := configFromOptions(Options{ProcessID: "2"})

	assert.Error(t, err, "Configure requires either the Server or Sentinels option")
}

func TestRequiresProcessConfig(t *testing.T) {
	_, err := configFromOptions(Options{ServerAddr: "localhost:6379"})

	assert.Error(t, err, "Configure requires a ProcessID, which uniquely identifies this instance")
}

func TestAddsColonToNamespace(t *testing.T) {
	cfg, err := configFromOptions(Options{
		ServerAddr: "localhost:6379",
		ProcessID:  "1",
	})

	assert.NoError(t, err)
	assert.Equal(t, "", cfg.Namespace)

	cfg, err = configFromOptions(Options{
		ServerAddr: "localhost:6379",
		ProcessID:  "1",
		Namespace:  "prod",
	})

	assert.NoError(t, err)
	assert.Equal(t, "prod:", cfg.Namespace)
}

func TestDefaultPollIntervalConfig(t *testing.T) {
	cfg, err := configFromOptions(Options{
		ServerAddr: "localhost:6379",
		ProcessID:  "1",
	})

	assert.NoError(t, err)
	assert.Equal(t, 15, cfg.PollInterval)

	cfg, err = configFromOptions(Options{
		ServerAddr:   "localhost:6379",
		ProcessID:    "1",
		PollInterval: 1,
	})

	assert.NoError(t, err)
	assert.Equal(t, 1, cfg.PollInterval)
}

func TestSentinelConfigGood(t *testing.T) {
	cfg, err := configFromOptions(Options{
		SentinelAddrs:   "localhost:26379,localhost:46379",
		RedisMasterName: "123",
		ProcessID:       "1",
		PollInterval:    1,
	})

	assert.NoError(t, err)
	assert.Equal(t, "FailoverClient", cfg.Client.Options().Addr)
}

func TestSentinelConfigNoMaster(t *testing.T) {
	_, err := configFromOptions(Options{
		SentinelAddrs: "localhost:26379,localhost:46379",
		ProcessID:     "1",
		PollInterval:  1,
	})

	assert.Error(t, err)
}
