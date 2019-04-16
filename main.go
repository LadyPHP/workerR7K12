package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	uuid "github.com/nu7hatch/gouuid"
	"github.com/streadway/amqp"
)

// Подключение к серверу с Rabbit
func rabbitconnect() *amqp.Connection {
	connUrl := os.Getenv("CONN_URL")
	conn, err := amqp.Dial(connUrl)

	if err != nil {
		fmt.Println(err)
	}
	return conn
}

func syslogconnect() *net.TCPConn {
	sysLog := os.Getenv("SYSLOG")
	tcpAddr, err := net.ResolveTCPAddr("tcp", sysLog)
	if err != nil {
		fmt.Println("ResolveTCPAddr failed:", err.Error())
	}
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		fmt.Println("Dial failed:", err.Error())
	}
	return conn
}

func getMessages(ch *amqp.Channel, syslogconn *net.TCPConn) {
	msgs, err := ch.Consume(
		"r7k12",
		"",
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		logger("Consume", "getMessages", err.Error(), uuID(), syslogconn)
	}

	for d := range msgs {
		uuid := uuID()
		logger("SendMessage", "getMessages", string(d.Body), uuid, syslogconn)
		apiUrl := os.Getenv("API_URL")
		fmt.Println("URL:>", apiUrl)

		req, err := http.NewRequest("POST", apiUrl, bytes.NewBuffer(d.Body))
		req.Header.Set("Content-Type", "application/json")

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		defer resp.Body.Close()
		body, _ := ioutil.ReadAll(resp.Body)
		logger("SendMessageResponse", "getMessages", "response Status:"+string(resp.Status)+"response Body:"+string(body), uuid, syslogconn)
		time.Sleep(4000 * time.Millisecond)
	}
}

// Логирование сообщений с сервиса
func logger(action string, function string, message string, loguuid string, conn net.Conn) {
	graylog := map[string]string{}
	graylog["version"] = "1.1"
	graylog["host"] = os.Getenv("SYN_HOST")
	graylog["short_message"] = action + "::" + function
	graylog["_on"] = "R7K12Worker"
	graylog["full_message"] = message
	graylog["timestamp"] = strconv.FormatInt(time.Now().UTC().Unix(), 10)
	graylog["level"] = "1"
	graylog["_loguuid"] = loguuid
	j, _ := json.Marshal(graylog)
	conn.Write(append([]byte(string(j)), 0))
}

func uuID() string {
	out, err := uuid.NewV4()
	if err != nil {
		log.Println(err)
	}
	return out.String()
}

func main() {
	syslogconn := syslogconnect()
	conn := rabbitconnect()
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		fmt.Println(err)
	}

	ch.Qos(1, 0, false)
	logger("START", "Main", "ServiceStart", uuID(), syslogconn)
	go getMessages(ch, syslogconn)

	chn := make(chan bool)
	<-chn
}
