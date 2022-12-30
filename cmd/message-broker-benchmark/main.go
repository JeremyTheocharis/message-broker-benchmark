// this program benchmarks MQTT and Kafka message broker systems
// it sends a "rawAudio" payload to the message broker in various cases
// this rawAudio should hten be transformed by a separate docker container called raw-sound-to-spectogram, which takes the rawAudio data and converts it into a spectogram
// the spectogram is then sent to the message broker again
// from there on there is a separate docker container which classifies the spectogram and sends the classification back to the message broker
// from there on the classification is written to a database
// (this is the whole predictive maintenance pipeline)
// see also drawio diagram in the root folder of this project

// List of cases
// 1. perfect: the payload is sent with the correct timestamps
// 2. delayed: the payload is sent with the delayed timestamps
// 3. incorrect: the payload is sent with incorrect payload (malformed)
// by default, at first case 1 is executed and then case 2 and 3 at the same time

// the program can be run with the following env variables:
// - MQTTEnabled: true or false, if true, the MQTT broker is used, if false, the Kafka broker is used
// - OUTPUT_TOPIC: the topic to which the payload is sent
// - HOST: the host of the message broker
// - PORT: the port of the message broker
//
package main

// Importing the required packages
import (
	"encoding/base64"
	"log"
	"os"
	"strings"
	"time"
)

// main function, which reads in the env variables and calls either the MQTT or Kafka function
func main() {
	println("Starting message broker benchmark")
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

	// Sleep 500s to give the message broker time to start
	println("Sleeping 500s to give the message broker time to start")
	time.Sleep(500 * time.Second)
	println("Finished sleeping")

	// Call the MQTT function if MQTTEnabled is true
	if MQTTEnabled == "true" {
		mainMQTT(OUTPUT_TOPIC, HOST, PORT)
	} else {
		// Call the Kafka function if MQTTEnabled is false
		mainKafka(OUTPUT_TOPIC, HOST, PORT)
	}

}

// readInCase reads in and parses a payload file and a timestamp file
func readInCase(payloadPath string, timestampPath string) (payload []byte, timestamps []string) {

	// PAYLOAD

	originalPayload, err := os.ReadFile(payloadPath)
	if err != nil {
		log.Fatal(err)
	}

	// convert to string
	originalPayloadString := string(originalPayload)

	// decode from base64
	payload, err = base64.StdEncoding.DecodeString(originalPayloadString)
	if err != nil {
		log.Fatal(err)
	}

	// TIMESTAMPS

	timestampsByte, err := os.ReadFile(timestampPath)
	if err != nil {
		log.Fatal(err)
	}

	// convert the byte array to a string array separated by newlines
	timestampString := string(timestampsByte)
	timestamps = strings.Split(timestampString, "\r\n")

	return
}

func readInPerfectCase() (casePerfectPayloadDecoded []byte, casePerfectTimestamps []string) {

	// CASE 1: PERFECT
	casePerfectPayloadDecoded, casePerfectTimestamps = readInCase("/app/tests/case_perfect/payload-audio.txt", "/app/tests/case_perfect/timestamps.txt")

	return
}

func readInDelayedCase() (caseDelayedPayloadDecoded []byte, caseDelayedTimestamps []string) {

	// CASE 2: DELAYED
	caseDelayedPayloadDecoded, caseDelayedTimestamps = readInCase("/app/tests/case_delayed/payload-audio.txt", "/app/tests/case_delayed/timestamps.txt")
	return
}

func readInIncorrectCase() (caseIncorrectPayloadDecoded []byte, caseIncorrectTimestamps []string) {

	// CASE 3: INCORRECT
	caseIncorrectPayloadDecoded, caseIncorrectTimestamps = readInCase("/app/tests/case_incorrect/payload-audio.txt", "/app/tests/case_incorrect/timestamps.txt")
	return
}
