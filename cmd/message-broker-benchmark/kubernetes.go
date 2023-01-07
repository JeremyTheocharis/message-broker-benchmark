package main

import (
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

func trackOtherTests() {

	// read in timestamp files
	timestampsCPURaw, err := os.ReadFile("/app/tests/timestamps-bad-cpu.txt")
	if err != nil {
		log.Fatal(err)
	}

	timestampCPUString := string(timestampsCPURaw)
	timestampsCPU := strings.Split(timestampCPUString, "\r\n")

	timestampsBadContainerCrashKafkaRaw, err := os.ReadFile("/app/tests/timestamps-bad-container-crash-kafka.txt")
	if err != nil {
		log.Fatal(err)
	}

	timestampBadContainerCrashKafkaString := string(timestampsBadContainerCrashKafkaRaw)
	timestampsBadContainerCrashKafka := strings.Split(timestampBadContainerCrashKafkaString, "\r\n")

	timestampsBadCOntainerCrashMachineLearningRaw, err := os.ReadFile("/app/tests/timestamps-bad-container-crash-machine-learning.txt")
	if err != nil {
		log.Fatal(err)
	}

	timestampBadContainerCrashMachineLearningString := string(timestampsBadCOntainerCrashMachineLearningRaw)
	timestampsBadContainerCrashMachineLearning := strings.Split(timestampBadContainerCrashMachineLearningString, "\r\n")

	go executeOtherTest([]string{string(timestampsCPU[0]), string(timestampsCPU[1])}, "bad-cpu")
	go executeOtherTest([]string{string(timestampsBadContainerCrashKafka[0]), string(timestampsBadContainerCrashKafka[1])}, "bad-container-crash-kafka")
	go executeOtherTest([]string{string(timestampsBadContainerCrashMachineLearning[0]), string(timestampsBadContainerCrashMachineLearning[1])}, "bad-container-crash-machine-learning")

}

// executeOtherTest announces when a test "name" should be executed and prints regular updates on the duration until the test is started (or ended)
// timestamps are two strings, the first one start time of the test, the second the end time
// both a are in the unit "milliseconds since start of the function"
func executeOtherTest(timestamps []string, name string) {

	startTimestamp := timestamps[0]
	endTimestamp := timestamps[1]

	log.Printf("Waiting for test %s to start, start time is %s, end time is %s", name, startTimestamp, endTimestamp)

	// convert timestamps to int
	startTimestampInt, err := strconv.Atoi(startTimestamp)
	if err != nil {
		log.Fatal(err)
	}

	endTimestampInt, err := strconv.Atoi(endTimestamp)
	if err != nil {
		log.Fatal(err)
	}

	// get current time
	originalTime := time.Now().UnixNano() / int64(time.Second)

	testIsRunning := true

	for testIsRunning {

		// get current time
		currentTime := time.Now().UnixNano() / int64(time.Second)

		// calculate time difference
		timeDifference := currentTime - originalTime

		// calculate time until test starts
		timeUntilStart := int(startTimestampInt/1000 - int(timeDifference))

		// calculate time until test ends. Convert to seconds
		timeUntilEnd := int(endTimestampInt/1000 - int(timeDifference))

		// if test is not yet started
		if timeUntilStart > 0 {
			if timeUntilStart > 200 {
				log.Printf("Test %s will start in %d seconds", name, timeUntilStart)
				time.Sleep(100 * time.Second)
			} else if timeUntilStart > 20 {
				log.Printf("Test %s will start in %d seconds", name, timeUntilStart)
				time.Sleep(10 * time.Second)
			} else {
				log.Printf("Test %s will start in %d seconds", name, timeUntilStart)
				time.Sleep(1 * time.Second)
			}
		}

		// if test is already started
		if timeUntilStart <= 0 && timeUntilEnd > 0 {
			log.Printf("Test %s is running for %d seconds", name, timeUntilEnd)
			time.Sleep(time.Duration(timeUntilEnd) * time.Second)
		}

		// if test is already ended
		if timeUntilEnd <= 0 {
			log.Printf("Test %s is finished, %d, %d, %d", name, timeUntilEnd, currentTime, endTimestampInt)
			testIsRunning = false
			continue
		}

	}
}
