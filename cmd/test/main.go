package main

import (
	"airlink/internal/node"
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
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

// Run the commands in two terminals:
// go run cmd/test/main.go > tmp/test_node_1.txt
// go run cmd/test/main.go --rev=true > tmp/test_node_2.txt
func main() {
	reverse := flag.Bool("rev", false, "reverse the send and receive port assignments")

	flag.Parse()

	senderPort, receiverPort := PORT_1, PORT_2
	if *reverse {
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

	node, err := node.NewP2PNode(senderAddr, net.ListenUDP)
	defer node.Cleanup()

	err = node.Listen()
	if err != nil {
		log.Fatalf("node listen failed: %v\n", err)
	}

	for {
		printToTerminal("Enter data to send to port %d: ", receiverPort)
		reader := bufio.NewReader(os.Stdin)
		input, _ := reader.ReadString('\n')
		data := strings.TrimSpace(input)

		switch data {
		case "q", "quit", "exit":
			os.Exit(0)
		default:
			node.Send(receiverAddr, []byte(data))
		}
	}
}