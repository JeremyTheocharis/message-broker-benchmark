// This program loads in the file "good-payload-audio.txt" and "timestamps.txt" and sends the payload according to the timestamps to a MQTT or Kafka broker

package main

// Importing the required packages
import (
	"encoding/base64"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

// main function, which reads in the env variables and calls either the MQTT or Kafka function
func main() {
	println("Starting file-importer")
	// Read in env variable MQTTEnabled and set it to false by default
	MQTTEnabled := os.Getenv("MQTTEnabled")
	if MQTTEnabled == "" {
		MQTTEnabled = "false"
	}

	// Read in the variable OUTPUT_TOPIC and set it to /development/predictive_maintenance/rawAudio by default
	OUTPUT_TOPIC := os.Getenv("OUTPUT_TOPIC")
	if OUTPUT_TOPIC == "" {
		OUTPUT_TOPIC = "/development/predictive_maintenance/rawAudio"
	}

	// read in host and port
	HOST := os.Getenv("HOST")
	PORT := os.Getenv("PORT")

	// Call the MQTT function if MQTTEnabled is true
	if MQTTEnabled == "true" {
		mainMQTT(OUTPUT_TOPIC, HOST, PORT)
	} else {
		// Call the Kafka function if MQTTEnabled is false
		mainKafka(OUTPUT_TOPIC, HOST, PORT)
	}

}

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

func readInPayloads() (goodPayloadDecoded []byte, badPayloadDecoded []byte, goodTimestampsGoodPayload []string, timestampsBadPayload []string, badTimestampsGoodPayload []string) {

	// read in the good payload
	goodPayload, err := os.ReadFile("good-payload-audio.txt")
	if err != nil {
		log.Fatal(err)
	}

	// convert to string
	goodPayloadString := string(goodPayload)

	// decode from base64
	goodPayloadDecoded, err = base64.StdEncoding.DecodeString(goodPayloadString)
	if err != nil {
		log.Fatal(err)
	}

	// read in the bad payload
	badPayload, err := os.ReadFile("./bad/bad-payload-audio.txt")
	if err != nil {
		log.Fatal(err)
	}

	// convert to string
	badPayloadString := string(badPayload)

	// decode from base64
	badPayloadDecoded, err = base64.StdEncoding.DecodeString(badPayloadString)
	if err != nil {
		log.Fatal(err)
	}

	// read in the file "timestamps.txt" and "timestamps-bad.txt" into a byte array
	goodTimestampsGoodPayloadByte, err := os.ReadFile("timestamps.txt")
	if err != nil {
		log.Fatal(err)
	}

	// convert the byte array to a string array separated by newlines
	goodTimestampsGoodPayloadString := string(goodTimestampsGoodPayloadByte)
	goodTimestampsGoodPayload = strings.Split(goodTimestampsGoodPayloadString, "\r\n")

	// read in the file "timestamps-bad.txt" into a byte array
	timestampsBadPayloadByte, err := os.ReadFile("./bad/timestamps-bad.txt")
	if err != nil {
		log.Fatal(err)
	}

	// convert the byte array to a string array separated by newlines
	timestampsBadPayloadString := string(timestampsBadPayloadByte)
	timestampsBadPayload = strings.Split(timestampsBadPayloadString, "\r\n")

	return
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
		println(name, "Sent", totalAmountOfSentMessages, "messages", endActualTimestamp)
	}()

	// shitty line
	if false {
		println("shitty line", lastTimestamp)
	}
	for index, timestamp := range timestamps {
		// // stop after 100 seconds
		// if lastTimestamp >= 300000 {
		// 	println(name, "Stopping after 300 seconds")
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

	// read in the payloads
	goodPayloadDecoded, badPayloadDecoded, goodTimestampsGoodPayload, timestampsBadPayload, badTimestampsGoodPayload := readInPayloads()

	// yes, this line is shit
	if false {
		println(badPayloadDecoded, goodTimestampsGoodPayload, timestampsBadPayload, badTimestampsGoodPayload)
	}

	// Starting with the good payload, send it to the broker
	print("Sending good payload to the broker...")
	var wg sync.WaitGroup
	wg.Add(1)

	go sendPayload(goodPayloadDecoded, goodTimestampsGoodPayload, client, OUTPUT_TOPIC, &wg, "Good")

	wg.Wait()

	// Now send good and bad payloads
	print("Sending good and bad payload to the broker...")

	var wgBad sync.WaitGroup
	wgBad.Add(2)

	go sendPayload(goodPayloadDecoded, badTimestampsGoodPayload, client, OUTPUT_TOPIC, &wgBad, "Good")
	go sendPayload(badPayloadDecoded, timestampsBadPayload, client, OUTPUT_TOPIC, &wgBad, "Bad")

	wgBad.Wait()

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

// Kafka function, which sends the payload to the Kafka broker
func mainKafka(OUTPUT_TOPIC string, HOST string, PORT string) {

}
