// This program loads in the file "good-payload-audio.txt" and "timestamps.txt" and sends the payload according to the timestamps to a MQTT or Kafka broker

package main

// Importing the required packages
import (
	"bufio"
	"encoding/base64"
	"log"
	"os"
	"strconv"
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

	// read all files in the current directory
	files, err := os.ReadDir(".")
	if err != nil {
		log.Fatal(err)
	}

	// loop through all files in the current directory and print them
	for _, file := range files {
		println(file.Name())
	}

	// read in the file "good-payload-audio.txt"
	file, err := os.Open("good-payload-audio.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// read in the file "timestamps.txt"
	file2, err := os.Open("timestamps.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file2.Close()

	// get the first line of the file good-payload-audio.txt
	goodPayload, err := os.ReadFile("good-payload-audio.txt")
	if err != nil {
		log.Fatal(err)
	}

	// convert to string
	goodPayloadString := string(goodPayload)

	// decode from base64
	goodPayloadDecoded, err := base64.StdEncoding.DecodeString(goodPayloadString)
	if err != nil {
		log.Fatal(err)
	}

	// print goodPayload
	println("This is the good payload: ", goodPayloadDecoded)

	// go though the file timestamps.txt and send the payload according to the timestamps
	scanner2 := bufio.NewScanner(file2)
	lastTimestamp := 0
	for scanner2.Scan() {
		// get the timestamp, sleep, and send the payload
		currentTimestampString := scanner2.Text()
		currentTimestamp, err := strconv.Atoi(currentTimestampString)
		if err != nil {
			log.Fatal(err)
		}
		timeToWait := currentTimestamp - lastTimestamp
		lastTimestamp = currentTimestamp
		// sleep now and print the time
		time.Sleep(time.Duration(timeToWait) * time.Millisecond)
		log.Println("Sending payload at", time.Now().Format("2006-01-02 15:04:05.000"), OUTPUT_TOPIC)

		// send the payload to the MQTT broker
		client.Publish(OUTPUT_TOPIC, 0, false, goodPayloadDecoded)
	}
}

// Kafka function, which sends the payload to the Kafka broker
func mainKafka(OUTPUT_TOPIC string, HOST string, PORT string) {

}
