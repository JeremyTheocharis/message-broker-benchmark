// This program loads in the file "good-payload-audio.txt" and "timestamps.txt" and sends the payload according to the timestamps to a MQTT or Kafka broker

package main

// Importing the required packages
import (
	"encoding/base64"
	"log"
	"os"
	"strconv"
	"strings"
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

	// // read in the bad payload
	// badPayload, err := os.ReadFile("./bad/bad-payload-audio.txt")
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// // convert to string
	// badPayloadString := string(badPayload)

	// // decode from base64
	// badPayloadDecoded, err = base64.StdEncoding.DecodeString(badPayloadString)
	// if err != nil {
	// 	log.Fatal(err)
	// }

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

// // getOnMessageReceived gets the function onMessageReceived, that is called everytime a message is received by a specific topic
// // TODO: thread safe
// func getOnMessageReceived(timestampQueue chan string) func(MQTT.Client, MQTT.Message) {

// 	return func(client MQTT.Client, message MQTT.Message) {
// 		topic := message.Topic()
// 		payload := message.Payload()

// 		// parse payload as json
// 		var payloadJSON map[string]interface{}
// 		err := json.Unmarshal(payload, &payloadJSON)
// 		if err != nil {
// 			log.Fatal(err)
// 		}

// 		// get timestamp from payload
// 		timestamp := payloadJSON["timestamp"].(string)

// 		// add timestamp to queue
// 		timestampQueue <- timestamp

// 		// get total queue length
// 		queueLength := len(timestampQueue)

// 		// print queue length every 20 messages
// 		if queueLength%20 == 0 {
// 			println("Queue length:", queueLength)
// 		}

// 	}
// }

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

	// returnedTimestampQueue := make(chan string, 3600*5)

	// // subscribe to the topic returning the aggregated data
	// if token := client.Subscribe("/development/predictive_maintenance/aggrResOUT", 0, getOnMessageReceived(returnedTimestampQueue)); token.Wait() && token.Error() != nil {
	// 	println(token.Error())
	// 	os.Exit(1)
	// }

	// read in the payloads
	goodPayloadDecoded, badPayloadDecoded, goodTimestampsGoodPayload, timestampsBadPayload, badTimestampsGoodPayload := readInPayloads()

	if false {
		println(badPayloadDecoded, goodTimestampsGoodPayload, timestampsBadPayload, badTimestampsGoodPayload)
	}
	// Starting with the good payload, send it to the broker
	print("Sending good payload to the broker...")

	// go though the file timestamps.txt and send the payload according to the timestamps
	lastTimestamp := 0
	for index, timestamp := range goodTimestampsGoodPayload {
		// get the timestamp, sleep, and send the payload
		currentTimestampString := timestamp
		currentTimestamp, err := strconv.Atoi(currentTimestampString)
		if err != nil {
			log.Fatal(err)
		}
		timeToWait := currentTimestamp - lastTimestamp
		lastTimestamp = currentTimestamp

		// sleep now
		time.Sleep(time.Duration(timeToWait) * time.Millisecond)

		// send the payload to the MQTT broker
		go client.Publish(OUTPUT_TOPIC, 0, false, goodPayloadDecoded)

		// print the progress
		if index%10 == 0 {
			println("Sent", index, "payloads")
		}

		// stop after 100 messages
		if index == 100 {
			break
		}
	}

}

// Kafka function, which sends the payload to the Kafka broker
func mainKafka(OUTPUT_TOPIC string, HOST string, PORT string) {

}
