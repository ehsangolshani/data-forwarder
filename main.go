package main

import (
	"bufio"
	"data-forwarder/filebeat"
	"fmt"
	"github.com/spf13/cobra"
	"log"
	"os"
	"time"
)

func main() {
	var filebeatCmd = &cobra.Command{
		Run: func(cmd *cobra.Command, args []string) {
			address, err := cmd.Flags().GetString("address")
			if err != nil {
				log.Fatal(err)
			}
			reconnectWait, err := cmd.Flags().GetDuration("reconnectWait")
			if err != nil {
				log.Fatal(err)
			}
			maxReconnect, err := cmd.Flags().GetInt("maxReconnect")
			if err != nil {
				log.Fatal(err)
			}

			filebeatTCPFoerwarder, err := filebeat.NewTCPForwarder(address, reconnectWait, maxReconnect)
			if err != nil {
				log.Fatal(err)
			}

			scanner := bufio.NewScanner(os.Stdin) //default scanner is ScanLines
			for scanner.Scan() {
				data := scanner.Bytes()
				_, err, reconnectOk := filebeatTCPFoerwarder.Send(data)
				if err != nil {
					log.Println(err)
				}
				if !reconnectOk {
					log.Fatal("failed to reconnect, stopping application")
				}
			}
			if err := scanner.Err(); err != nil {
				log.Println(err)
			}
		},
	}
	filebeatCmd.Flags().StringP("address", "a", "127.0.0.1:8080", "url and port of filebeat tcp listener")
	filebeatCmd.Flags().IntP("maxReconnect", "m", 100, "maximum number to try reconnecting in case of connection close")
	filebeatCmd.Flags().DurationP("reconnectWait", "w", 1000*time.Millisecond, "number of milliseconds to wait between reconnect attempts")

	err := filebeatCmd.Execute()
	if err != nil {
		fmt.Println(err)
	}
}
