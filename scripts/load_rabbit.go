package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"flag"
	"os"
	"sync"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func main() {
	queuePtr := flag.String("queue", "test", "name of the queue to send to")
	flag.Parse()

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		*queuePtr,
		false,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to declare queue")

	msgs := make(chan string)
	var wg sync.WaitGroup
	wg.Add(1)
	go publishMessages(&wg, ch, q.Name, msgs)
	go readMessages(msgs)

	wg.Wait()
}

func readMessages(msgs chan string) {
	scanner := bufio.NewScanner(os.Stdin)
	i := 0
	for scanner.Scan() {
		if i > 3 {
			break
		}
		scanned := []byte(scanner.Text())
		var event map[string]interface{}
		err := json.Unmarshal(scanned, &event)
		failOnError(err, "Failed to unmarshall message")
		payload, err := json.Marshal(event["payload"])

		failOnError(err, "Failed to marshall body")
		// fmt.Println(string(payload))
		msgs <- string(payload)
	}
	close(msgs)
}

func publishMessages(wg *sync.WaitGroup, ch *amqp.Channel, qName string, msgs chan string) {
	for m := range msgs {
		err := ch.Publish(
			"",
			qName,
			false,
			false,
			amqp.Publishing{
				MessageId:   fmt.Sprintf(m),
				ContentType: "text/plain",
				Body:        []byte(m),
			})
		failOnError(err, "Failed to send msg")
	}
	wg.Done()
}
