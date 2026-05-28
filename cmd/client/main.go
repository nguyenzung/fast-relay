package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/coder/websocket"
)

// Interactive CLI client to connect to the relayer and send messages.
// Commands available during interactive session:
//   /chat a,b,c   -> start a chat session with recipients a,b,c (messages sent while session active go to these recipients)
//   /exit         -> leave current chat session (subsequent messages broadcast)
//   /quit         -> exit program
//   /help         -> show help

func main() {
	addr := flag.String("addr", "localhost:8080", "server address (host:port)")
	pub := flag.String("pub", "", "this client's pub key as 64-hex chars")
	flag.Parse()

	if *pub == "" {
		log.Fatalf("pub required and must be 64-hex")
	}
	pubBytes, err := hex.DecodeString(*pub)
	if err != nil || len(pubBytes) != 32 {
		log.Fatalf("invalid pub, must be 64 hex chars representing 32 bytes")
	}
	var pubKey [32]byte
	copy(pubKey[:], pubBytes)

	q := url.Values{}
	q.Set("pub", *pub)
	u := url.URL{Scheme: "ws", Host: *addr, Path: "/", RawQuery: q.Encode()}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn, _, err := websocket.Dial(ctx, u.String(), nil)
	if err != nil {
		log.Fatalf("dial error: %v", err)
	}
	defer func() {
		_ = conn.Close(websocket.StatusNormalClosure, "client closing")
	}()

	// Handle OS signals for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("signal received, shutting down client")
		cancel()
	}()

	// Reader: prints any inbound messages from server. Exits when ctx canceled or read fails.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				mt, data, err := conn.Read(ctx)
				if err != nil {
					cancel()
					return
				}
				if mt != websocket.MessageBinary {
					continue
				}
				// Minimum frame length: FromID(32) + ToIDsLen(1) + DataLen(4) = 37
				if len(data) < 37 {
					continue
				}
				var from [32]byte
				copy(from[:], data[0:32])
				nTo := int(data[32])
				dataLenOff := 33 + nTo*32
				// Need at least 4 bytes for DataLen
				if dataLenOff+4 > len(data) {
					continue
				}
				payloadLen := int(binary.BigEndian.Uint32(data[dataLenOff : dataLenOff+4]))
				start := dataLenOff + 4
				if start+payloadLen > len(data) {
					continue
				}
			}
		}
	}()

	reader := bufio.NewReader(os.Stdin)

	// currentRecipients == nil => broadcast; otherwise messages sent to these recipients
	var currentRecipients [][32]byte

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		default:
			fmt.Print("> ")
			line, err := reader.ReadString('\n')
			if err != nil {
				log.Printf("stdin read error: %v", err)
				break loop
			}
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}

			if strings.HasPrefix(line, "/") {
				parts := strings.Fields(strings.TrimPrefix(line, "/"))
				switch parts[0] {
				case "chat":
					if len(parts) < 2 {
						fmt.Println("usage: /chat a,b,c (each a 64-hex pub)")
						continue
					}
					arg := strings.Join(parts[1:], " ")
					currentRecipients = parseRecipients(arg)
					fmt.Printf("chat session started -> recipients: %v\n", currentRecipients)
				case "exit":
					currentRecipients = nil
					fmt.Println("left chat session; messages will be broadcast")
				case "quit":
					cancel()
					break loop
				case "help":
					fmt.Println("Commands:\n  /chat a,b,c   - start a chat session with recipients a,b,c (64-hex each)\n  /exit - leave chat\n  /quit - exit")
				default:
					fmt.Println("unknown command")
				}
				continue
			}

			// Build binary message
			// FromID
			var buf []byte
			buf = append(buf, pubKey[:]...)
			// ToIDsLen
			n := len(currentRecipients)
			if n > 255 {
				n = 255
			}
			buf = append(buf, byte(n))
			// ToIDs
			for i := 0; i < n; i++ {
				buf = append(buf, currentRecipients[i][:]...)
			}
			// DataLen (uint32 BE)
			payload := []byte(line)

			// Chunk large payloads to avoid library hard read limit (~32KB single frame)
			const maxPayloadPerFrame = 32 * 1024 // 32 KiB
			sentAny := false
			for off := 0; off < len(payload); off += maxPayloadPerFrame {
				end := off + maxPayloadPerFrame
				if end > len(payload) {
					end = len(payload)
				}
				chunk := payload[off:end]

				var lenb [4]byte
				binary.BigEndian.PutUint32(lenb[:], uint32(len(chunk)))

				// build frame for this chunk
				frame := make([]byte, 0, 32+1+len(currentRecipients)*32+4+len(chunk))
				frame = append(frame, pubKey[:]...)
				n := len(currentRecipients)
				if n > 255 {
					n = 255
				}
				frame = append(frame, byte(n))
				for i := 0; i < n; i++ {
					frame = append(frame, currentRecipients[i][:]...)
				}
				frame = append(frame, lenb[:]...)
				frame = append(frame, chunk...)

				if err := conn.Write(ctx, websocket.MessageBinary, frame); err != nil {
					log.Printf("write error: %v", err)
					cancel()
					break loop
				}
				sentAny = true
				// small pause to avoid hogging the connection
				time.Sleep(2 * time.Millisecond)
			}
			if sentAny {
				fmt.Println("sent")
			}
		}
	}

	// Wait briefly for any last inbound messages, then exit
	select {
	case <-time.After(300 * time.Millisecond):
	case <-ctx.Done():
	}
	log.Println("client exit")
}

func parseRecipients(s string) [][32]byte {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([][32]byte, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			if b, err := hex.DecodeString(t); err == nil {
				var k [32]byte
				copy(k[:], b)
				out = append(out, k)
				continue
			}
			// Fallback: accept short human-friendly id by copying/padding into 32 bytes
			var k [32]byte
			bs := []byte(t)
			if len(bs) > 32 {
				copy(k[:], bs[:32])
			} else {
				copy(k[:], bs)
			}
			out = append(out, k)
		}
	}
	return out
}
