package workers

import (
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis"
)

const (
	retryKey         = "goretry"
	scheduledJobsKey = "schedule"
)

type scheduledWorker struct {
	config config
	keys   []string
	done   chan bool
}

func (s *scheduledWorker) run() {
	for {
		select {
		case <-s.done:
			return
		default:
		}

		s.poll()

		time.Sleep(time.Duration(s.config.PollInterval) * time.Second)
	}
}

func (s *scheduledWorker) quit() {
	close(s.done)
}

func (s *scheduledWorker) poll() {
	now := nowToSecondsWithNanoPrecision()

	for _, key := range s.keys {
		key = s.config.Namespace + key
		for {
			messages, _ := s.config.Client.ZRangeByScore(key, redis.ZRangeBy{
				Min:    "-inf",
				Max:    strconv.FormatFloat(now, 'f', -1, 64),
				Offset: 0,
				Count:  1,
			}).Result()

			if len(messages) == 0 {
				break
			}

			message, _ := NewMsg(messages[0])

			if removed, _ := s.config.Client.ZRem(key, messages[0]).Result(); removed != 0 {
				queue, _ := message.Get("queue").String()
				queue = strings.TrimPrefix(queue, s.config.Namespace)
				message.Set("enqueued_at", nowToSecondsWithNanoPrecision())
				s.config.Client.LPush(s.config.Namespace+"queue:"+queue, message.ToJson()).Result()
			}
		}
	}
}

func newScheduledWorker(cfg config) *scheduledWorker {
	return &scheduledWorker{
		config: cfg,
		keys:   []string{retryKey, scheduledJobsKey},
		done:   make(chan bool),
	}
}
