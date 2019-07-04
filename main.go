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

const newLineDelimiter byte = '\n'

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

			verbose, err := cmd.Flags().GetBool("verbose")
			if err != nil {
				log.Fatal(err)
			}

			addNewLine, err := cmd.Flags().GetBool("addNewLine")
			if err != nil {
				log.Fatal(err)
			}

			filebeatTCPForwarder, err := filebeat.NewTCPForwarder(address, reconnectWait, maxReconnect)
			if err != nil {
				log.Fatal(err)
			}

			scanner := bufio.NewScanner(os.Stdin) //default scanner is ScanLines
			for scanner.Scan() {
				data := scanner.Bytes()
				if addNewLine {
					data = append(data, newLineDelimiter)
				}
				_, err, reconnectOk := filebeatTCPForwarder.Send(data)
				if err != nil {
					log.Println(err)
				}
				if !reconnectOk {
					log.Fatalln("failed to reconnect, stopping application")
				}
				if verbose {
					_, err = os.Stdout.Write(data)
					if err != nil {
						log.Println(err)
					}
				}
			}
			if err := scanner.Err(); err != nil {
				log.Fatalln(err)
			}
		},
	}
	filebeatCmd.Flags().StringP("address", "a", "127.0.0.1:8080", "url and port of filebeat tcp listener")
	filebeatCmd.Flags().IntP("maxReconnect", "m", 100, "maximum number to try reconnecting in case of connection close")
	filebeatCmd.Flags().DurationP("reconnectWait", "w", 1000*time.Millisecond, "number of milliseconds to wait between reconnect attempts")
	filebeatCmd.Flags().BoolP("verbose", "v", false, "print out each transmitted part, default is false")
	filebeatCmd.Flags().BoolP("addNewLine", "n", true, "add new line delimiter ('\n'), default is true")

	err := filebeatCmd.Execute()
	if err != nil {
		fmt.Println(err)
	}
}
