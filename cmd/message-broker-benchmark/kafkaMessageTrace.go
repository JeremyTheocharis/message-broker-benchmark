package main

import (
	"errors"
	"os"

	"github.com/Shopify/sarama"
)

var SerialNumber = os.Getenv("SERIAL_NUMBER")
var MicroserviceName = os.Getenv("MICROSERVICE_NAME")

type TraceValue struct {
	Traces map[int64]string `json:"trace"`
}

func Produce(producer sarama.SyncProducer, msg *sarama.ProducerMessage) error {
	if MicroserviceName == "" {
		println("MicroserviceName is empty")
		return errors.New("microservice name is empty")
	}
	if SerialNumber == "" {
		println("SerialNumber is empty")
		return errors.New("microservice name is empty")
	}
	// identifier := MicroserviceName + "-" + SerialNumber
	// err := AddXTrace(msg, identifier)
	// if err != nil {
	// 	return err
	// }
	_, _, err := producer.SendMessage(msg)
	if err != nil {
		println("Error sending message", err.Error())
	}

	return err
}

// func addXOrigin(message *sarama.ProducerMessage, origin string) error {
// 	return addHeaderTrace(message, "x-origin", origin)
// }

// func AddXOriginIfMissing(message *sarama.ProducerMessage) error {
// 	trace := GetTrace(message, "x-origin")
// 	if trace == nil {
// 		err := addXOrigin(message, SerialNumber)
// 		return err
// 	}
// 	return nil
// }

// func AddXTrace(message *sarama.ProducerMessage, value string) error {
// 	err := addHeaderTrace(message, "x-trace", value)
// 	if err != nil {
// 		return err
// 	}
// 	err = AddXOriginIfMissing(message)
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }

// func addHeaderTrace(message *sarama.ProducerMessage, key, value string) error {
// 	if message.Headers == nil {
// 		message.Headers = make([]sarama.RecordHeader, 0)
// 	}

// 	for i := 0; i < len(message.Headers); i++ {
// 		header := message.Headers[i]
// 		if header.Key == []byte(key) {
// 			// Json decode
// 			var traceValue TraceValue
// 			err := jsoniter.Unmarshal(header.Value, traceValue)
// 			if err != nil {
// 				return err
// 			}
// 			// Current time
// 			t := time.Now().UnixNano()
// 			// Check if trace already exists
// 			if _, ok := traceValue.Traces[t]; ok {
// 				return errors.New("trace already exists")
// 			}
// 			// Add new trace
// 			traceValue.Traces[t] = value
// 			// Json encode
// 			var json []byte
// 			json, err = jsoniter.Marshal(traceValue)
// 			if err != nil {
// 				return err
// 			}
// 			// Update header
// 			header.Value = json
// 			message.Headers[i] = header
// 			return nil
// 		}
// 	}

// 	// Create new header
// 	var traceValue TraceValue
// 	traceValue.Traces = make(map[int64]string)
// 	traceValue.Traces[time.Now().UnixNano()] = value
// 	// Json encode
// 	var json []byte
// 	json, err := jsoniter.Marshal(traceValue)
// 	if err != nil {
// 		return err
// 	}

// 	// Add new header
// 	message.Headers = append(message.Headers, sarama.RecordHeader{
// 		Key:   []byte(key),
// 		Value: json,
// 	})
// 	return nil
// }

// func GetTrace(message *sarama.ProducerMessage, key string) *TraceValue {
// 	if message.Headers == nil {
// 		return nil
// 	}

// 	for i := 0; i < len(message.Headers); i++ {
// 		header := message.Headers[i]
// 		if header.Key == []byte(key) {
// 			// Json decode
// 			var traceValue TraceValue
// 			err := jsoniter.Unmarshal(header.Value, &traceValue)
// 			if err != nil {
// 				println("Failed to unmarshal trace header: %s (%s)", err, key)
// 				return nil
// 			}
// 			return &traceValue
// 		}
// 	}
// 	return nil
// }
