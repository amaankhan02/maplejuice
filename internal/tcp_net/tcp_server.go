package tcp_net

import (
	"log"
	"net"
	"sync"
)

/*
SDFSNode implements this interface
*/
type TCPServerConnectionHandler interface {

	// Called when TCP server connected to a new client
	HandleTCPServerConnection(conn net.Conn)
}

type TCPServer struct {
	listener          net.Listener
	wg                sync.WaitGroup
	port              string
	connectionHandler TCPServerConnectionHandler
}

func NewTCPServer(port string, serverConnectionHandler TCPServerConnectionHandler) *TCPServer {
	tcpServer := TCPServer{
		port:              port,
		connectionHandler: serverConnectionHandler,
	}

	return &tcpServer
}

/*
Establishes a listening port and creates a new goroutine to begin
a loop where it infinitely Accept()s new connections
*/
func (this *TCPServer) StartServer() {
	l, err := net.Listen("tcp", ":"+this.port)
	if err != nil {
		log.Fatalf("net.Listen(): %v", err)
	}
	this.listener = l
	this.wg.Add(1)
	go this.serve()
}

// Helper function
// Create socket endpoint on the port passed in, and listen to new connections to this server
// For every new accepted TCP connection, call a new goroutine to handle that connection
func (this *TCPServer) serve() {
	defer this.wg.Done()

	// Listen for connections, accept, and spawn goroutine to handle that connection
	for {
		conn, err := this.listener.Accept()
		if err != nil {
			// TODO: should i check if it just quit?
			log.Println("Accept() error: ", err)
		} else {
			// TODO: log that it connected to 'conn'
			this.wg.Add(1)
			go func() {
				defer this.wg.Done()
				this.connectionHandler.HandleTCPServerConnection(conn)
			}()
		}
	}
}

func (this *TCPServer) StopServer() {
	err := this.listener.Close()
	if err != nil {
		log.Fatalln("Failed to close server's listener object")
	}
	this.wg.Wait()
}
