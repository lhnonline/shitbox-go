package shitdis

import (
	"fmt"
	"github.com/go-redis/redis"
	"time"
)

func ScanKeyWithHandler(client *redis.Client, pattern string, count int64, handle func(key string), result *[]string) {
	var cursor uint64
	var matchedKeys int
	var keys []string
	var err error

	for {
		keys, cursor, err = client.Scan(cursor, pattern, count).Result()
		if err != nil {
			panic(err)
		}
		matchedKeys += len(keys)

		if handle != nil {
			for _, key := range keys {
				handle(key)
			}
		}

		if result != nil {
			*result = append(*result, keys...)
		}

		if cursor == 0 {
			break
		}
	}
}

func ScanKey(client *redis.Client, pattern string, count int64, result *[]string) {
	var cursor uint64
	var matchedKeys int
	var keys []string
	var err error

	for {
		keys, cursor, err = client.Scan(cursor, pattern, count).Result()
		if err != nil {
			panic(err)
		}
		matchedKeys += len(keys)

		if result != nil {
			*result = append(*result, keys...)
		}

		if cursor == 0 {
			break
		}
	}
}

func ScanKeyPip(client *redis.Client, pattern string, result *[]string, pip func(redis.Pipeliner, *[]string)) {
	ScanKeyPipLine(client, pattern, 1000, result, pip, true)
}

// https://dzone.com/articles/the-effects-of-redis-scan-on-performance-and-how-k
func ScanKeyPipLine(client *redis.Client, pattern string, count int64, result *[]string, pip func(redis.Pipeliner, *[]string), quiet bool) {

	startAt := time.Now()
	var cursor uint64
	var matchedKeys int
	var keys []string
	var err error

	for {
		keys, cursor, err = client.Scan(cursor, pattern, count).Result()
		if err != nil {
			panic(err)
		}
		matchedKeys += len(keys)

		if result != nil {
			*result = append(*result, keys...)
		}

		if cursor == 0 {
			if ! quiet {
				endAt := time.Now()
				fmt.Printf("\nfound [%d] keys,use [%v] ms\n", matchedKeys, endAt.Sub(startAt).Milliseconds())
			}

			pip(client.Pipeline(), result)
			break
		}
	}
}
