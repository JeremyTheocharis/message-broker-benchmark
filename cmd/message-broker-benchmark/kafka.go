package main

import (
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

// sendPayloadKafka sends a payload in defined intervals to the Kafka broker
func sendPayloadKafka(payload []byte, timestamps []string, p sarama.SyncProducer, OUTPUT_TOPIC string, wg *sync.WaitGroup, name string) {
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

		kafkamessage := &sarama.ProducerMessage{
			Topic: OUTPUT_TOPIC,
			Value: sarama.ByteEncoder(payload),
		}

		err = Produce(p, kafkamessage)

		if err != nil {
			println("Failed to send Kafka message: %s", err)
			continue
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

	// fetch environment variable SKIP_GOOD_CASE. defaults to false
	SKIP_GOOD_CASEString := os.Getenv("SKIP_GOOD_CASE")
	if SKIP_GOOD_CASEString == "" {
		SKIP_GOOD_CASEString = "false"
	}

	SKIP_GOOD_CASE, err := strconv.ParseBool(SKIP_GOOD_CASEString)
	if err != nil {
		SKIP_GOOD_CASE = false
	}

	println("SKIP_GOOD_CASE", SKIP_GOOD_CASE)

	// connect to Kafka broker
	config := sarama.NewConfig()
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	// create a new producer
	producer, err := sarama.NewSyncProducer([]string{HOST + ":" + PORT}, config)
	if err != nil {
		panic(err)
	}

	// EXECUTE CASE 1: PERFECT

	if !SKIP_GOOD_CASE {

		println("Sending case_perfect to the broker...")
		// read the payload
		casePerfectPayload, casePerfectTimestamps := readInPerfectCase()
		var wg sync.WaitGroup
		wg.Add(1)

		go sendPayloadKafka(casePerfectPayload, casePerfectTimestamps, producer, OUTPUT_TOPIC, &wg, "case_perfect")

		wg.Wait()

		println("case_perfect sent")
		// sleep for 60 seconds
		println("Sleeping for 60 seconds...")
		time.Sleep(60 * time.Second)

	}
	// EXECUTE CASE 2: DELAYED WITH INCORRECT MESSAGES
	println("Sending case_delayed and case_incorrect to the broker...")

	// read the payload
	caseDelayedPayload, caseDelayedTimestamps := readInDelayedCase()
	caseIncorrectPayload, caseIncorrectTimestamps := readInIncorrectCase()

	var wgBad sync.WaitGroup
	wgBad.Add(2)

	go sendPayloadKafka(caseDelayedPayload, caseDelayedTimestamps, producer, OUTPUT_TOPIC, &wgBad, "case_delayed")
	go sendPayloadKafka(caseIncorrectPayload, caseIncorrectTimestamps, producer, OUTPUT_TOPIC, &wgBad, "case_incorrect")
	go trackOtherTests()

	wgBad.Wait()

	println("case_delayed and case_incorrect sent")

	// wait endless
	for {
		time.Sleep(1 * time.Second)
	}

}
