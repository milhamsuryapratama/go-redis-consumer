package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"milhamsuryapratama/go-redis-consumer/config"
	"milhamsuryapratama/go-redis-consumer/models"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
)

var (
	client        *redis.Client
	streamName    = "stream-tes"
	consumerGroup = "stream-tes-group"
	waitGrp       sync.WaitGroup
	db            *sql.DB
)

func init() {
	var err error
	client, err = config.NewRedisClient()
	if err != nil {
		panic(err)
	}

	db, err = sql.Open("mysql", "root:@tcp(localhost:3306)/event-driven")
	if err != nil {
		panic(err)
	}

	createConsumerGroup()
}

func main() {
	go consumeEvent()

	chanOS := make(chan os.Signal)
	//Gracefully disconection
	signal.Notify(chanOS, syscall.SIGINT, syscall.SIGTERM)
	<-chanOS

	waitGrp.Wait()
	client.Close()
}

func createConsumerGroup() {
	if _, err := client.XGroupCreateMkStream(context.Background(), streamName, consumerGroup, "0").Result(); err != nil {
		if !strings.Contains(fmt.Sprint(err), "BUSYGROUP") {
			fmt.Printf("Error on create Consumer Group: %v ...\n", consumerGroup)
			panic(err)
		}
	}
}

func consumeEvent() {
	for {
		func() {
			streams, err := client.XReadGroup(context.Background(), &redis.XReadGroupArgs{
				Streams:  []string{streamName, ">"},
				Group:    consumerGroup,
				Consumer: "consumer",
				Count:    10,
				Block:    0,
			}).Result()

			if err != nil {
				log.Printf("err on consume events: %+v\n", err)
				return
			}

			for _, stream := range streams[0].Messages {
				waitGrp.Add(1)
				// process stream
				go processStream(stream)
			}
			waitGrp.Wait()
		}()
	}
}

func processStream(stream redis.XMessage) {

	defer waitGrp.Done()

	typeEvent := stream.Values["type"].(string)
	data := stream.Values["data"].(string)

	var category models.Category
	json.Unmarshal([]byte(data), &category)

	fmt.Println("type", typeEvent)
	fmt.Println("data", category)
	fmt.Println("id", stream.ID)

	db.Query("INSERT INTO category (category_name) VALUES (?)", category.CategoryName)

	client.XAck(context.Background(), streamName, consumerGroup, stream.ID)
}
