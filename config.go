package workers

import (
	"errors"
	"strings"
	"time"

	"github.com/go-redis/redis"
)

type config struct {
	processId    string
	Namespace    string
	PollInterval int
	Client       *redis.Client
}

type Options struct {
	ProcessID    string
	Namespace    string
	PollInterval int
	Database     int
	Password     string
	PoolSize     int

	// Provide one of ServerAddr or (SentinelAddrs + RedisMasterName)
	ServerAddr      string
	SentinelAddrs   string
	RedisMasterName string
}

func configFromOptions(options Options) (*config, error) {
	if options.ProcessID == "" {
		return nil, errors.New("Options requires a ProcessID, which uniquely identifies this instance")
	}

	if options.Namespace != "" {
		options.Namespace += ":"
	}
	if options.PoolSize == 0 {
		options.PoolSize = 1
	}
	if options.PollInterval <= 0 {
		options.PollInterval = 15
	}

	redisIdleTimeout := 240 * time.Second

	var rc *redis.Client
	if options.ServerAddr != "" {
		rc = redis.NewClient(&redis.Options{
			IdleTimeout: redisIdleTimeout,
			Password:    options.Password,
			DB:          options.Database,
			PoolSize:    options.PoolSize,
			Addr:        options.ServerAddr,
		})
	} else if options.SentinelAddrs != "" {
		if options.RedisMasterName == "" {
			return nil, errors.New("Sentinel configuration requires a master name")
		}

		rc = redis.NewFailoverClient(&redis.FailoverOptions{
			IdleTimeout:   redisIdleTimeout,
			Password:      options.Password,
			DB:            options.Database,
			PoolSize:      options.PoolSize,
			SentinelAddrs: strings.Split(options.SentinelAddrs, ","),
			MasterName:    options.RedisMasterName,
		})
	} else {
		return nil, errors.New("Options requires either the Server or Sentinels option")
	}
	return &config{
		processId:    options.ProcessID,
		Namespace:    options.Namespace,
		PollInterval: options.PollInterval,
		Client:       rc,
	}, nil
}
