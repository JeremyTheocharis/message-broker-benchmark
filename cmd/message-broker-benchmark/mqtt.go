package main

import (
	"strconv"
	"sync"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

// OnConnect subscribes once the connection is established. Required to re-subscribe when cleansession is True
func OnConnect(c MQTT.Client) {

	optionsReader := c.OptionsReader()
	println("Connected to MQTT broker", optionsReader.ClientID())
}

// OnConnectionLost outputs warn message
func OnConnectionLost(c MQTT.Client, err error) {

	optionsReader := c.OptionsReader()
	println("Connection lost", err, optionsReader.ClientID())
}

// sendPayload sends a payload in defined intervals to the MQTT broker
func sendPayload(payload []byte, timestamps []string, client MQTT.Client, OUTPUT_TOPIC string, wg *sync.WaitGroup, name string) {
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
			println(name, "Sleeping for", currentTimestamp-timeSinceProgramStart, "ms")
			time.Sleep(time.Duration(currentTimestamp-timeSinceProgramStart) * time.Millisecond)
		} else {
			println(name, "Not sleeping because timestamp is in the past")
		}

		// send the payload to the MQTT broker, if the connection is lost, try to reconnect and send the message again
		token := client.Publish(OUTPUT_TOPIC, 0, false, payload)
		token.Wait()
		if token.Error() != nil {
			println("Error publishing message", token.Error())
			println("Trying to reconnect")
			token = client.Connect()
			// wait for the connection to be established, at least 300 seconds
			token.WaitTimeout(300 * time.Second)
			if token.Error() != nil {
				println("Error reconnecting", token.Error())
				break
			}
		}

		totalAmountOfSentMessages++

		// print the progress
		if index%10 == 0 {
			println(name, "Sent", index, "payloads")
		}

	}
}

// MQTT function, which sends the payload to the MQTT broker
func mainMQTT(OUTPUT_TOPIC string, HOST string, PORT string) {
	// connect to the MQTT broker
	opts := MQTT.NewClientOptions()
	opts.AddBroker(HOST + ":" + PORT)
	opts.SetClientID("file-importer")
	opts.SetAutoReconnect(true)
	opts.SetOnConnectHandler(OnConnect)
	opts.SetConnectionLostHandler(OnConnectionLost)

	client := MQTT.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	// wait until the connection is established and only proceed then
	for !client.IsConnected() {
		time.Sleep(1 * time.Second)
		println("Waiting for connection to be established...")
	}

	// EXECUTE CASE 1: PERFECT

	print("Sending case_perfect to the broker...")
	// read the payload
	casePerfectPayload, casePerfectTimestamps := readInPerfectCase()
	var wg sync.WaitGroup
	wg.Add(1)

	go sendPayload(casePerfectPayload, casePerfectTimestamps, client, OUTPUT_TOPIC, &wg, "case_perfect")

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

	go sendPayload(caseDelayedPayload, caseDelayedTimestamps, client, OUTPUT_TOPIC, &wgBad, "case_delayed")
	go sendPayload(caseIncorrectPayload, caseIncorrectTimestamps, client, OUTPUT_TOPIC, &wgBad, "case_incorrect")

	wgBad.Wait()

	println("case_delayed and case_incorrect sent")

	// Now execute this multiple times in parallel
	// count := 20
	// print("Sending", count, "times good payloads")

	// var wgPerformance sync.WaitGroup
	// wgPerformance.Add(count)

	// for i := 0; i < count; i++ {
	// 	go sendPayload(goodPayloadDecoded, goodTimestampsGoodPayload, client, OUTPUT_TOPIC, &wgPerformance, strconv.Itoa(i))
	// }

	// wgPerformance.Wait()
}
