package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/go-playground/validator"
	"github.com/gorilla/mux"
	"github.com/mitchellh/mapstructure"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

var writer *kafka.Writer

type Message struct {
	Type    string      `json:"type"`
	Content interface{} `json:"content"`
}

type PaymentMessage struct {
	Id     string `json:"id" validate:"required"`
	Amount int    `json:"amount" validate:"required"`
	Status string `json:"status" validate:"required"`
}

type UserMessage struct {
	Name    string `json:"name" validate:"required"`
	Email   string `json:"email" validate:"required"`
	Phone   string `json:"phone" validate:"required"`
	Address string `json:"address"`
}

func createWriter(topic string) {
	writer = kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	})
}

func createMessageHandler(w http.ResponseWriter, r *http.Request) {

	params := mux.Vars(r)

	topic := params["topic"]

	if strings.TrimSpace(topic) == "" {
		log.Error("Missing topic")
		handleBadRequest(w)
		return
	}

	createWriter(topic)

	defer writer.Close()

	var msg Message

	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		log.WithField("error", err).Error("Failed to decode request body")
		handleBadRequest(w)
		return
	}

	var messageJson []byte
	var err error

	switch msg.Type {
	case "new_user":
		var userMessage UserMessage
		contentMap, ok := msg.Content.(map[string]interface{})
		if !ok {
			log.WithField("error", "content is not a map").Error("Invalid content")
			handleBadRequest(w)
			return
		}

		err = mapstructure.Decode(contentMap, &userMessage)
		if err != nil {
			log.WithField("error", err).Error("Failed to decode user message")
			handleBadRequest(w)
			return
		}

		validate := validator.New()
		err = validate.Struct(userMessage)
		if err != nil {
			log.WithField("error", err).Error("Validation failed")
			handleBadRequest(w)
			return
		}
	case "new_payment":
		var paymentMessage PaymentMessage
		contentMap, ok := msg.Content.(map[string]interface{})
		if !ok {
			log.WithField("error", "content is not a map").Error("Invalid content")
			handleBadRequest(w)
			return
		}

		err = mapstructure.Decode(contentMap, &paymentMessage)
		if err != nil {
			log.WithField("error", err).Error("Failed to decode payment message")
			handleBadRequest(w)
			return
		}

		validate := validator.New()
		err = validate.Struct(paymentMessage)
		if err != nil {
			log.WithField("error", err).Error("Validation failed")
			handleBadRequest(w)
			return
		}
	default:
		log.WithField("type", msg.Type).Error("Invalid message type")
		handleBadRequest(w)
		return
	}

	err = writer.WriteMessages(context.Background(),
		kafka.Message{
			Value: messageJson,
		},
	)

	if err != nil {
		log.WithField("error", err).Error("Failed to write message")
		handleInternalError(w)
		return
	}

	w.WriteHeader(http.StatusCreated)
	fmt.Fprintf(w, "Message created successfully")
}

func createTopicHandler(w http.ResponseWriter, r *http.Request) {

	params := mux.Vars(r)

	topicName := params["topic"]

	if strings.TrimSpace(topicName) == "" {
		log.Error("Missing topic name")
		handleBadRequest(w)
		return
	}

	conn, err := kafka.Dial("tcp", "localhost:9092")

	if err != nil {
		log.WithField("error", err).Error("Failed to dial leader")
		handleInternalError(w)
		return
	}

	defer conn.Close()

	topicConfig := kafka.TopicConfig{
		Topic:             topicName,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}

	err = conn.CreateTopics(topicConfig)
	if err != nil {
		log.WithField("error", err).Error("Failed to create topic")
		handleInternalError(w)
		return
	}

	w.WriteHeader(http.StatusCreated)
	fmt.Fprintf(w, "Topic created successfully")
}

func listTopicsHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := kafka.Dial("tcp", "localhost:9092")
	if err != nil {
		log.WithField("error", err).Error("Failed to dial leader")
		handleInternalError(w)
		return
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		log.WithField("error", err).Error("Failed to read partitions")
		handleInternalError(w)
		return
	}

	topics := make(map[string]bool)
	for _, p := range partitions {
		topics[p.Topic] = true
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "Topics: ", strings.Join(getKeysFromMap(topics), ", "))
}

func listMessagesHandler(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	topic := params["topic"]

	if strings.TrimSpace(topic) == "" {
		log.Error("Missing topic")
		handleBadRequest(w)
		return
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9092"},
		Topic:     topic,
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
	defer reader.Close()

	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			break
		}
		fmt.Fprintf(w, "message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
	}
}

func main() {

	r := mux.NewRouter()

	r.HandleFunc("/create/{topic}", createTopicHandler).Methods("POST")

	r.HandleFunc("/publish/{topic}", createMessageHandler).Methods("POST")

	r.HandleFunc("/topics", listTopicsHandler).Methods("GET")
	r.HandleFunc("/messages/{topic}", listMessagesHandler).Methods("GET")

	if err := http.ListenAndServe(":8080", r); err != nil {
		log.WithField("error", err).Error("Failed to start server")
		return
	}

	log.Info("Server started successfully")
}

func handleError(w http.ResponseWriter, status int) {
	log.WithField("status", status).Info("Handling error")
	w.WriteHeader(status)
}

func handleBadRequest(w http.ResponseWriter) {
	log.Info("Bad request")
	handleError(w, http.StatusBadRequest)
}

func handleInternalError(w http.ResponseWriter) {
	log.Info("Internal server error")
	handleError(w, http.StatusInternalServerError)
}

func getKeysFromMap(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
