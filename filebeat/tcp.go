package filebeat

import (
	"github.com/pkg/errors"
	"net"
	"sync"
	"time"
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
func (t *TCPForwarder) Send(data []byte) (int, error, bool) {
	//there is a small problem here, if remote host close the connection unexpectedly an unexpected situation happens
	//client is not aware of connection closure because it's state is old yet, so first write with be successful and client won't return any err
	// after that state of tcp client updates and know it know that connection is closed, then next call to Write will return err
	// our Approach is to ignore this situation
	// additional links:
	// https://stackoverflow.com/questions/15067286/golang-tcpconn-setwritedeadline-doesnt-seem-to-work-as-expected
	// https://grokbase.com/t/gg/golang-nuts/14car3mfh9/go-nuts-best-way-to-retry-failed-writes-write-to-a-disconnected-tcpconn
	// https://stackoverflow.com/questions/51317968/write-on-a-closed-net-conn-but-returned-nil-error
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
