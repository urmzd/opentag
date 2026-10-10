package bus_test

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/urmzd/mandatum/pkg/bus"
	"github.com/urmzd/mandatum/pkg/bus/bustest"
)

// addrEnv names the Redis the conformance suite runs against. Without it the
// Redis subtests skip, so the suite stays runnable with no infrastructure —
// but the moment someone exports it, the Redis backend is held to exactly the
// same contract as Memory.
const addrEnv = "MANDATUM_REDIS_ADDR"

// keyspace numbers each bus the suite builds, so subtests sharing one Redis
// cannot see each other's events.
var keyspace atomic.Uint64

func TestRedisConformance(t *testing.T) {
	addr := os.Getenv(addrEnv)
	if addr == "" {
		t.Skipf("set %s to run the Redis bus conformance suite", addrEnv)
	}
	bustest.Run(t, func(t *testing.T, cfg bustest.Config) bus.Bus {
		client := redis.NewClient(&redis.Options{Addr: addr})
		prefix := fmt.Sprintf("mandatumtest:%d", keyspace.Add(1))
		t.Cleanup(func() {
			drop(t, client, prefix)
			_ = client.Close()
		})
		if err := client.Ping(t.Context()).Err(); err != nil {
			t.Fatalf("ping %s: %v", addr, err)
		}
		return bus.NewRedis(client,
			bus.WithRetention(cfg.Retention),
			bus.WithBacklog(cfg.Backlog),
			bus.WithKeyPrefix(prefix),
			// Short enough that the suite's idle probes and Close waits stay
			// brisk; the contract does not depend on the value.
			bus.WithPollInterval(25*time.Millisecond),
		)
	})
}

// drop removes every key the bus created under prefix. It runs from a test
// cleanup, where the test context is already cancelled, so it uses its own.
func drop(t *testing.T, client *redis.Client, prefix string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var cursor uint64
	for {
		keys, next, err := client.Scan(ctx, cursor, prefix+":*", 512).Result()
		if err != nil {
			t.Errorf("scan %s: %v", prefix, err)
			return
		}
		if len(keys) > 0 {
			if err := client.Del(ctx, keys...).Err(); err != nil {
				t.Errorf("delete %d keys under %s: %v", len(keys), prefix, err)
				return
			}
		}
		if next == 0 {
			return
		}
		cursor = next
	}
}
