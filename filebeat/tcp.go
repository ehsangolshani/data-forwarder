package filebeat

import (
	"github.com/pkg/errors"
	"net"
	"sync"
	"time"
)

const (
	newLineByteValue byte = 10
)

type TCPForwarder struct {
	ch            chan int
	mux           *sync.Mutex
	TCPAddress    *net.TCPAddr
	Conn          *net.TCPConn
	ReconnectWait time.Duration
	MaxReconnect  int
}

func NewTCPForwarder(address string, reconnectWait time.Duration, maxReconnect int) (*TCPForwarder, error) {
	tcpAddress, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTCP("tcp", nil, tcpAddress)
	if err != nil {
		return nil, err
	}
	return &TCPForwarder{ch: make(chan int, 1), mux: &sync.Mutex{}, TCPAddress: tcpAddress, Conn: conn, ReconnectWait: reconnectWait, MaxReconnect: maxReconnect}, nil
}

// output: number of written bytes, error, reconnectOk
func (t *TCPForwarder) Send(data []byte, addNewLine bool) (int, error, bool) {
	if addNewLine {
		data = append(data, newLineByteValue)
	}
	n, err := t.Conn.Write(data)
	if err != nil {
		if len(t.ch) > 0 {
			return 0, err, true
		}
		t.ch <- 0
		err = t.reconnect()
		<-t.ch
		if err != nil {
			return 0, err, false
		}
		n, err = t.Conn.Write(data)
		if err != nil {
			return 0, err, true
		}
	}
	return n, nil, true
}

func (t *TCPForwarder) reconnect() error {
	_ = t.Conn.Close()
	for i := 0; i < t.MaxReconnect; i++ {
		conn, err := net.DialTCP("tcp", nil, t.TCPAddress)
		if err != nil {
			time.Sleep(t.ReconnectWait)
			continue
		}
		t.Conn = conn
		return nil
	}
	return errors.New("failed to reconnect to Filebeat tcp listener")
}
