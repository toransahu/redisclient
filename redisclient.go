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
	"os"
	"strings"

	redis "github.com/go-redis/redis/v8"
	utils "github.com/toransahu/goutils"
)

var ctx = context.Background()

func check(e error) {
	if e != nil {
		panic(e)
	}
}

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

func publishTestDataCLI() {
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

func tryReadBytesFromRedisAndWriteFile() {
	host := "redis://localhost:6379/0"
	conn := NewConnection(host)

	val, err := conn.Get(ctx, "file_bytes").Result()
	check(err)
	fmt.Println("file_bytes", val)

	f, err := os.Create("/home/toransahu/Desktop/gocreated.jpg")
	check(err)
	noOfBytes, err := f.Write([]byte(val))
	check(err)
	fmt.Printf("wrote %d bytes\n", noOfBytes)
	f.Sync()
}

func main() {
	// publishTestDataCLI()
	tryReadBytesFromRedisAndWriteFile()
}
