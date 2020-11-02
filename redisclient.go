//
// redisclient.go
// Copyright (C) 2020 Toran Sahu <toran.sahu@yahoo.com>
//
// Distributed under terms of the MIT license.
//

package main

import (
	"context"
	"flag"
	"fmt"
	"strings"

	redis "github.com/go-redis/redis/v8"
	"github.com/toransahu/go-utils"
)

var ctx = context.Background()

func NewConnection(host string) *redis.Client {
	opt, err := redis.ParseURL(host)
	if err != nil {
		panic(err)
	}
	return redis.NewClient(opt)
}

func publish(conn *redis.Client, channels []string) {
	pipe := conn.Pipeline()

	msg := `{"test": "by toran"}`
	for _, channel := range channels {
		fmt.Println(channel)
		val, err := pipe.Publish(ctx, channel, msg).Result()
		if err != nil {
			fmt.Printf("Error while: PUBLISH %s %s", channel, msg)
		}
		fmt.Printf("PUBLISH %s %s = %v", channel, msg, val)
	}
	pipe.Exec(ctx)
	pipe.Close()
}

func main() {
	host := flag.String("host", "redis://localhost:6379/0", "Redis Hostname like: redis://localhost:6379/0")
	channels_file := flag.String("channels-file", "", "Channels file")
	flag.Parse()

	path := *channels_file
	data, err := utils.ReadFile(path)
	if err != nil {
		fmt.Printf("Error while ReadFile(%s)", path)
	}
	channels := strings.Split(data, "\n")

	conn := NewConnection(*host)
	for {
		publish(conn, channels)
	}
}
