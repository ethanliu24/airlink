package main

import (
	"airlink/internal/event"
	"airlink/internal/node"
	"airlink/internal/transfer"
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
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

			fmt.Print("Accept Transfer? [y/n]")
			if scanner.Scan() {
				input := strings.TrimSpace(scanner.Text())

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
		default:
			return fmt.Errorf("unrecognized event name for node: %s", e.Name())
		}

		return nil
	}
}

// Run the commands in two terminals:
// go run cmd/test/main.go --sender=true
// go run cmd/test/main.go --sender=false > tmp/listener.txt
func main() {
	isSender := flag.Bool("sender", false, "whether the node focuses on listening or sending")

	flag.Parse()

	senderPort, receiverPort := PORT_1, PORT_2
	if !*isSender {
		senderPort, receiverPort = PORT_2, PORT_1
	}

	if !*isSender {
		senderPort, receiverPort = PORT_2, PORT_1
	}

	senderAddr, err := node.CreateUDPAddress(LOCAL_IP, senderPort)
	if err != nil {
		log.Fatalf("sender udp addr creation failed: %v\n", err)
	}

	receiverAddr, nil := node.CreateUDPAddress(LOCAL_IP, receiverPort)
	if err != nil {
		log.Fatalf("receiver udp addr creation failed: %v\n", err)
	}

	node, err := node.NewP2PNode(senderAddr)
	node.RegisterEventHandler(eventHandler(node))
	defer node.Cleanup()

	err = node.Start()
	if err != nil {
		log.Fatalf("node listen failed: %v\n", err)
	}

	for {
		printToTerminal("Enter data to send to port %d: ", receiverPort)
		reader := bufio.NewReader(os.Stdin)
		input, _ := reader.ReadString('\n')
		userInput := strings.TrimSpace(input)

		switch userInput {
		case "q", "quit", "exit":
			os.Exit(0)
		default:
			node.SendFile(receiverAddr, userInput)
		}
	}
}
