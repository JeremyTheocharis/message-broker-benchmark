package main

import (
	"strconv"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// sendPayloadKafka sends a payload in defined intervals to the Kafka broker
func sendPayloadKafka(payload []byte, timestamps []string, p *kafka.Producer, OUTPUT_TOPIC string, wg *sync.WaitGroup, name string) {
	defer wg.Done()

	lastTimestamp := 0
	// get the actual current timestamp in ms
	initialActualTimestamp := int(time.Now().UnixNano() / 1e6)
	println(name, "Start", initialActualTimestamp)
	totalAmountOfSentMessages := 0

	// defer sending the amount of sent messages
	defer func() {
		endActualTimestamp := int(time.Now().UnixNano() / 1e6)
		println("---RESULT ---", name, "Sent", totalAmountOfSentMessages, "messages", endActualTimestamp)
	}()

	// shitty line
	if false {
		println("shitty line", lastTimestamp)
	}
	for index, timestamp := range timestamps {
		// // stop after 100 seconds
		// if lastTimestamp >= 200000 {
		// 	println(name, "Stopping after 200 seconds")
		// 	break
		// }

		// get the timestamp, sleep, and send the payload
		currentTimestampString := timestamp
		currentTimestamp, err := strconv.Atoi(currentTimestampString)
		if err != nil {
			println(name, "Error converting timestamp to int", currentTimestampString)
			println(err)
			continue
		}

		lastTimestamp = currentTimestamp
		// get the actual current timestamp in ms
		actualTimestamp := int(time.Now().UnixNano() / 1e6)
		timeSinceProgramStart := actualTimestamp - initialActualTimestamp

		// sleep until the timestamp is reached or do not sleep if the timestamp is in the past
		if currentTimestamp > timeSinceProgramStart {
			// println(name, "Sleeping for", currentTimestamp-timeSinceProgramStart, "ms")
			time.Sleep(time.Duration(currentTimestamp-timeSinceProgramStart) * time.Millisecond)
		} else {
			println(name, "Sleeping only 1ms because timestamp is in the past")
			time.Sleep(time.Duration(1) * time.Millisecond)

			// 1ms is introduced to prevent duplicate timestamps (which would result in less entries in the database)
		}

		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &OUTPUT_TOPIC, Partition: kafka.PartitionAny},
			Value:          []byte(payload),
		}, nil)

		if err != nil {
			if err.(kafka.Error).Code() == kafka.ErrQueueFull {
				// Producer queue is full, wait 1s for messages
				// to be delivered then try again.
				time.Sleep(time.Second)
				continue
			}
			println("Failed to produce message: %v", err)
		}

		totalAmountOfSentMessages++

		// print the progress
		if index%10 == 0 {
			println(name, "Sent", index, "payloads")
		}

	}
}

// Kafka function, which sends the payload to the Kafka broker
func mainKafka(OUTPUT_TOPIC string, HOST string, PORT string) {

	// connect to Kafka broker
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": HOST + ":" + PORT})
	if err != nil {
		println("Failed to create producer: %s", err)
		panic(err)
	}

	// taken from https://github.com/confluentinc/confluent-kafka-go/blob/master/examples/producer_example/producer_example.go
	// Listen to all the events on the default events channel
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				// The message delivery report, indicating success or
				// permanent failure after retries have been exhausted.
				// Application level retries won't help since the client
				// is already configured to do that.
				m := ev
				if m.TopicPartition.Error != nil {
					println("Delivery failed: %v", m.TopicPartition.Error)
				} else {
					// println("Delivered message to topic %s [%d] at offset %v",
					// 	*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
				}
			case kafka.Error:
				// Generic client instance-level errors, such as
				// broker connection failures, authentication issues, etc.
				//
				// These errors should generally be considered informational
				// as the underlying client will automatically try to
				// recover from any errors encountered, the application
				// does not need to take action on them.
				println("Error")
			default:
				println("Ignored event: %s", ev)
			}
		}
	}()

	// EXECUTE CASE 1: PERFECT

	print("Sending case_perfect to the broker...")
	// read the payload
	casePerfectPayload, casePerfectTimestamps := readInPerfectCase()
	var wg sync.WaitGroup
	wg.Add(1)

	go sendPayloadKafka(casePerfectPayload, casePerfectTimestamps, p, OUTPUT_TOPIC, &wg, "case_perfect")

	wg.Wait()

	println("case_perfect sent")
	// sleep for 60 seconds
	println("Sleeping for 60 seconds...")
	time.Sleep(60 * time.Second)

	// EXECUTE CASE 2: DELAYED WITH INCORRECT MESSAGES
	print("Sending case_delayed and case_incorrect to the broker...")

	// read the payload
	caseDelayedPayload, caseDelayedTimestamps := readInDelayedCase()
	caseIncorrectPayload, caseIncorrectTimestamps := readInIncorrectCase()

	var wgBad sync.WaitGroup
	wgBad.Add(2)

	go sendPayloadKafka(caseDelayedPayload, caseDelayedTimestamps, p, OUTPUT_TOPIC, &wgBad, "case_delayed")
	go sendPayloadKafka(caseIncorrectPayload, caseIncorrectTimestamps, p, OUTPUT_TOPIC, &wgBad, "case_incorrect")

	wgBad.Wait()

	println("case_delayed and case_incorrect sent")

	// wait endless
	for {
		time.Sleep(1 * time.Second)
	}

	// flush the producer
	p.Flush(15 * 1000)

	// close the producer
	p.Close()
}
