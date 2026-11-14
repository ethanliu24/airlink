package main

import (
	"airlink/internal/event"
	"airlink/internal/node"
	"airlink/internal/transfer"
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

const LOCAL_IP string = "127.0.0.1"
const PORT_1 int = 1111
const PORT_2 int = 2222

func printToTerminal(format string, a ...any) {
	// Determine the system-specific standard console device
	ttyDevice := "/dev/tty"
	if runtime.GOOS == "windows" {
		ttyDevice = "CON"
	}

	// Open the terminal device for writing
	f, err := os.OpenFile(ttyDevice, os.O_WRONLY, 0)
	if err != nil {
		// Fallback to stderr if opening the specific TTY device fails
		fmt.Fprintf(os.Stderr, format, a...)
		return
	}
	defer f.Close()

	fmt.Fprintf(f, format, a...)
}

func eventHandler(node *node.P2PNode) event.EventHandlerFunc {
	return func(e event.Event) error {
		switch e.Name() {
		case transfer.RequestTransferAcceptanceEvtName:
			payload := e.Payload().(*transfer.RequestTransferAcceptancePayload)
			scanner := bufio.NewScanner(os.Stdin)

			printToTerminal("Accept Transfer? [y/n] ")
			if scanner.Scan() {
				input := strings.TrimSpace(scanner.Text())
				printToTerminal("Enter destination: ")

				accepted := false
				switch strings.ToLower(input) {
				case "y", "yes", "1":
					accepted = true
				}

				err := node.SubmitTransferAcceptDecision(payload.TransferId, accepted)
				if err != nil {
					return err
				}
			}
		case transfer.RequestDownloadDestinationEvtName:
			payload := e.Payload().(*transfer.RequestDownloadDestinationPayload)
			scanner := bufio.NewScanner(os.Stdin)

			printToTerminal("Enter destination: ")
			if scanner.Scan() {
				input := strings.TrimSpace(scanner.Text())
				printToTerminal("Enter destination: ")

				destination, err := filepath.Abs(input)
				if err != nil {
					return err
				}

				err = node.SubmitDownloadDestination(payload.TransferId, destination)
				if err != nil {
					return err
				}
			}
		case transfer.TransferTimedOutEvtName:
			payload := e.Payload().(*transfer.TransferTimedOutPayload)
			printToTerminal("Transfer timed out - message: %s, id: %s\n", payload.Message, payload.TransferId)
		default:
			return fmt.Errorf("unrecognized event name for node: %s", e.Name())
		}

		return nil
	}
}

func startSender(node *node.P2PNode, receiverPort int, receiverAddr *net.UDPAddr) {
	for {
		printToTerminal("Enter path to send to port %d: ", receiverPort)
		reader := bufio.NewReader(os.Stdin)
		input, _ := reader.ReadString('\n')
		userInput := strings.TrimSpace(input)

		switch userInput {
		case "q", "quit", "exit":
			os.Exit(0)
		default:
			err := node.SendFile(receiverAddr, userInput)
			if err != nil {
				printToTerminal("%v", err)
			}
		}
	}
}

func startReceiver() {
	select {}
}

// Run the commands in two terminals:
// go run cmd/test/main.go --sender=true
// go run cmd/test/main.go --sender=false > tmp/listener.txt
// source: cmd/test/file.txt
// destination: tmp/destination.txt
func main() {
	isSender := flag.Bool("sender", false, "whether the node focuses on listening or sending")

	flag.Parse()

	addr1, err := node.CreateUDPAddress(LOCAL_IP, PORT_1)
	if err != nil {
		log.Fatalf("port 1 udp addr creation failed: %v\n", err)
	}

	addr2, err := node.CreateUDPAddress(LOCAL_IP, PORT_2)
	if err != nil {
		log.Fatalf("port 2 udp addr creation failed: %v\n", err)
	}

	listeningAddr := addr1
	if !*isSender {
		listeningAddr = addr2
	}

	node, err := node.NewP2PNode(listeningAddr)
	if err != nil {
		log.Fatalf("node creation failed: %v\n", err)
	}

	node.RegisterEventHandler(eventHandler(node))
	defer node.Cleanup()

	err = node.Start()
	if err != nil {
		log.Fatalf("node listen failed: %v\n", err)
	}

	if *isSender {
		startSender(node, PORT_2, addr2)
	} else {
		startReceiver()
	}
}
