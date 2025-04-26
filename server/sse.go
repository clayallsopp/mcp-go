package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/mark3labs/mcp-go/mcp"
)

// sseConnection represents an active SSE connection linked to a clientSession.
type sseConnection struct {
	writer     http.ResponseWriter
	flusher    http.Flusher
	done       chan struct{}
	eventQueue chan string // Channel for queuing events specific to this connection
	requestID  atomic.Int64
	session    *clientSession // Reference back to the logical session
}

// close closes the connection's done channel.
func (c *sseConnection) close() {
	// Use a non-blocking close to avoid panic on double-close
	select {
	case <-c.done:
		// Already closed
	default:
		close(c.done)
	}
}

// queueEvent tries to queue an event for this connection.
// Returns false if the queue is full or the connection is closed.
func (c *sseConnection) queueEvent(event string) bool {
	select {
	case c.eventQueue <- event:
		return true
	case <-c.done:
		return false // Connection closed
	default:
		// Consider adding a timeout or logging if queue is full for extended periods
		return false // Queue full
	}
}

// clientSession represents a logical client session.
type clientSession struct {
	sessionID           string
	notificationChannel chan mcp.JSONRPCNotification
	initialized         atomic.Bool
	// Store the active connection associated with this session.
	// A session might potentially have multiple connections in the future,
	// but for now, we assume one active connection.
	mu               sync.RWMutex // Protects activeConnection
	activeConnection *sseConnection
}

func (cs *clientSession) SessionID() string {
	return cs.sessionID
}

func (cs *clientSession) NotificationChannel() chan<- mcp.JSONRPCNotification {
	return cs.notificationChannel
}

func (cs *clientSession) Initialize() {
	cs.initialized.Store(true)
}

func (cs *clientSession) Initialized() bool {
	return cs.initialized.Load()
}

// setActiveConnection safely sets the active connection.
func (cs *clientSession) setActiveConnection(conn *sseConnection) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.activeConnection = conn
}

// getActiveConnection safely gets the active connection.
func (cs *clientSession) getActiveConnection() *sseConnection {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.activeConnection
}

// clearActiveConnection safely clears the active connection, returning the old one.
func (cs *clientSession) clearActiveConnection() *sseConnection {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	oldConn := cs.activeConnection
	cs.activeConnection = nil
	return oldConn
}

var _ ClientSession = (*clientSession)(nil)

// SSEContextFunc is a function that takes an existing context and the current
// request and returns a potentially modified context based on the request
// content. This can be used to inject context values from headers, for example.
type SSEContextFunc func(ctx context.Context, r *http.Request) context.Context

// SSEServer implements a Server-Sent Events (SSE) based MCP server.
// It provides real-time communication capabilities over HTTP using the SSE protocol.
type SSEServer struct {
	server                       *MCPServer
	baseURL                      string
	basePath                     string
	useFullURLForMessageEndpoint bool
	messageEndpoint              string
	sseEndpoint                  string
	sessions                     sync.Map // Stores *clientSession, keyed by sessionID
	srv                          *http.Server
	contextFunc                  SSEContextFunc

	keepAlive         bool
	keepAliveInterval time.Duration

	mu sync.RWMutex
}

// SSEOption defines a function type for configuring SSEServer
type SSEOption func(*SSEServer)

// WithBaseURL sets the base URL for the SSE server
func WithBaseURL(baseURL string) SSEOption {
	return func(s *SSEServer) {
		if baseURL != "" {
			u, err := url.Parse(baseURL)
			if err != nil {
				return
			}
			if u.Scheme != "http" && u.Scheme != "https" {
				return
			}
			// Check if the host is empty or only contains a port
			if u.Host == "" || strings.HasPrefix(u.Host, ":") {
				return
			}
			if len(u.Query()) > 0 {
				return
			}
		}
		s.baseURL = strings.TrimSuffix(baseURL, "/")
	}
}

// Add a new option for setting base path
func WithBasePath(basePath string) SSEOption {
	return func(s *SSEServer) {
		// Ensure the path starts with / and doesn't end with /
		if !strings.HasPrefix(basePath, "/") {
			basePath = "/" + basePath
		}
		s.basePath = strings.TrimSuffix(basePath, "/")
	}
}

// WithMessageEndpoint sets the message endpoint path
func WithMessageEndpoint(endpoint string) SSEOption {
	return func(s *SSEServer) {
		s.messageEndpoint = endpoint
	}
}

// WithUseFullURLForMessageEndpoint controls whether the SSE server returns a complete URL (including baseURL)
// or just the path portion for the message endpoint. Set to false when clients will concatenate
// the baseURL themselves to avoid malformed URLs like "http://localhost/mcphttp://localhost/mcp/message".
func WithUseFullURLForMessageEndpoint(useFullURLForMessageEndpoint bool) SSEOption {
	return func(s *SSEServer) {
		s.useFullURLForMessageEndpoint = useFullURLForMessageEndpoint
	}
}

// WithSSEEndpoint sets the SSE endpoint path
func WithSSEEndpoint(endpoint string) SSEOption {
	return func(s *SSEServer) {
		s.sseEndpoint = endpoint
	}
}

// WithHTTPServer sets the HTTP server instance
func WithHTTPServer(srv *http.Server) SSEOption {
	return func(s *SSEServer) {
		s.srv = srv
	}
}

func WithKeepAliveInterval(keepAliveInterval time.Duration) SSEOption {
	return func(s *SSEServer) {
		s.keepAlive = true
		s.keepAliveInterval = keepAliveInterval
	}
}

func WithKeepAlive(keepAlive bool) SSEOption {
	return func(s *SSEServer) {
		s.keepAlive = keepAlive
	}
}

// WithContextFunc sets a function that will be called to customise the context
// to the server using the incoming request.
func WithSSEContextFunc(fn SSEContextFunc) SSEOption {
	return func(s *SSEServer) {
		s.contextFunc = fn
	}
}

// NewSSEServer creates a new SSE server instance with the given MCP server and options.
func NewSSEServer(server *MCPServer, opts ...SSEOption) *SSEServer {
	s := &SSEServer{
		server:                       server,
		sseEndpoint:                  "/sse",
		messageEndpoint:              "/message",
		useFullURLForMessageEndpoint: true,
		keepAlive:                    false,
		keepAliveInterval:            10 * time.Second,
	}

	// Apply all options
	for _, opt := range opts {
		opt(s)
	}

	return s
}

// NewTestServer creates a test server for testing purposes
func NewTestServer(server *MCPServer, opts ...SSEOption) *httptest.Server {
	sseServer := NewSSEServer(server, opts...)

	testServer := httptest.NewServer(sseServer)
	sseServer.baseURL = testServer.URL
	return testServer
}

// Start begins serving SSE connections on the specified address.
// It sets up HTTP handlers for SSE and message endpoints.
func (s *SSEServer) Start(addr string) error {
	s.mu.Lock()
	s.srv = &http.Server{
		Addr:    addr,
		Handler: s,
	}
	s.mu.Unlock()

	return s.srv.ListenAndServe()
}

// Shutdown gracefully stops the SSE server, closing all active sessions
// and shutting down the HTTP server.
func (s *SSEServer) Shutdown(ctx context.Context) error {
	s.mu.RLock()
	srv := s.srv
	s.mu.RUnlock()

	if srv != nil {
		// Iterate over sessions and signal associated connections to close
		s.sessions.Range(func(key, value interface{}) bool {
			sessionID := key.(string)
			if session, ok := value.(*clientSession); ok {
				// Safely get and clear the active connection
				if connection := session.clearActiveConnection(); connection != nil {
					// Signal the connection handler to stop
					connection.close()
				}
			}
			// Delete the session from the map during shutdown iteration
			s.sessions.Delete(sessionID)
			// Note: UnregisterSession is implicitly handled as connections close
			// and trigger the defer in handleSSE, or the MCPServer might handle
			// cleanup based on context cancellation during its own shutdown.
			return true
		})

		// Now shutdown the underlying HTTP server
		return srv.Shutdown(ctx)
	}
	return nil
}

// handleSSE handles incoming SSE connection requests.
// It sets up appropriate headers and creates a new session for the client.
func (s *SSEServer) handleSSE(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	sessionID := uuid.New().String()

	// Create the logical session
	session := &clientSession{
		sessionID:           sessionID,
		notificationChannel: make(chan mcp.JSONRPCNotification, 100),
		// initialized is false initially
	}

	// Create the connection associated with this request
	connection := &sseConnection{
		writer:     w,
		flusher:    flusher,
		done:       make(chan struct{}),
		eventQueue: make(chan string, 100), // Buffer for events for this connection
		session:    session,
		// requestID starts at 0
	}

	// Link session and connection
	session.setActiveConnection(connection)

	// Store the logical session *before* registering it
	s.sessions.Store(sessionID, session)

	// Defer cleanup for this connection handler
	defer func() {
		// When the connection handler exits, signal connection closure,
		// clear the active connection reference in the session,
		// remove the session from the map, and unregister from MCPServer.
		connection.close()
		session.clearActiveConnection()
		s.sessions.Delete(sessionID)
		// Use context.Background() for unregistration if request context might be done.
		s.server.UnregisterSession(context.Background(), sessionID)
	}()

	// Register the logical session with the MCPServer
	// The context here is the request context, tied to the connection lifetime.
	if err := s.server.RegisterSession(r.Context(), session); err != nil {
		http.Error(w, fmt.Sprintf("Session registration failed: %v", err), http.StatusInternalServerError)
		// If registration fails after storing, remove from map before returning.
		s.sessions.Delete(sessionID)
		// We don't run the deferred cleanup in this error path.
		return
	}
	// Note: The defer above handles UnregisterSession and other cleanup on normal exit.

	// Start notification handler for this session, sending to the active connection
	go func() {
		connDone := connection.done      // Cache connection's done channel
		reqCtxDone := r.Context().Done() // Cache request context's done channel

		for {
			select {
			case notification, ok := <-session.notificationChannel:
				if !ok {
					// Should not happen with current setup, but good practice
					return
				}
				// Get the currently active connection for this session
				activeConn := session.getActiveConnection()
				if activeConn == nil || activeConn != connection {
					// Connection associated with this goroutine is no longer active
					return
				}
				eventData, err := json.Marshal(notification)
				if err == nil {
					// Queue event for the specific connection
					if !activeConn.queueEvent(fmt.Sprintf("event: message\ndata: %s\n\n", eventData)) {
						// Log or handle queue failure (e.g., queue full, connection closing)
					}
				} else {
					// Log marshalling error
				}
			case <-connDone: // Listen to connection close signal
				return
			case <-reqCtxDone: // Also listen to request context cancellation
				return
			}
		}
	}()

	// Start keep alive : ping for this connection
	if s.keepAlive {
		go func() {
			ticker := time.NewTicker(s.keepAliveInterval)
			defer ticker.Stop()
			connDone := connection.done      // Cache connection's done channel
			reqCtxDone := r.Context().Done() // Cache request context's done channel

			for {
				select {
				case <-ticker.C:
					// Get the currently active connection for this session
					activeConn := session.getActiveConnection()
					if activeConn == nil || activeConn != connection {
						// Connection associated with this goroutine is no longer active
						return // Connection is gone or replaced
					}
					message := mcp.JSONRPCRequest{
						JSONRPC: "2.0",
						ID:      activeConn.requestID.Add(1), // Use connection's requestID
						Request: mcp.Request{
							Method: "ping",
						},
					}
					messageBytes, _ := json.Marshal(message) // Error handling omitted for ping
					pingMsg := fmt.Sprintf("event: message\ndata:%s\n\n", messageBytes)
					if !activeConn.queueEvent(pingMsg) {
						// Log or handle failure (e.g., connection closing)
						return // Stop trying if queue fails
					}
				case <-connDone: // Listen to connection close signal
					return
				case <-reqCtxDone: // Also listen to request context cancellation
					return
				}
			}
		}()
	}

	// Send the initial endpoint event via the connection
	fmt.Fprintf(connection.writer, "event: endpoint\ndata: %s\r\n\r\n", s.GetMessageEndpointForClient(sessionID))
	connection.flusher.Flush()

	// Main event loop for this connection
	connDone := connection.done      // Cache connection's done channel
	reqCtxDone := r.Context().Done() // Cache request context's done channel
	for {
		select {
		case event, ok := <-connection.eventQueue:
			if !ok {
				// eventQueue closed, likely means connection is closing
				return
			}
			// Write the event to the response
			if _, err := fmt.Fprint(connection.writer, event); err != nil {
				// Error writing to client, likely disconnected
				return // Exit loop, defer will handle cleanup
			}
			connection.flusher.Flush()
		case <-reqCtxDone:
			// Client disconnected (request context cancelled)
			return // Exit loop, defer will handle cleanup
		case <-connDone:
			// Connection explicitly closed (e.g., by shutdown)
			return // Exit loop, defer will handle cleanup
		}
	}
}

// GetMessageEndpointForClient returns the appropriate message endpoint URL with session ID
// based on the useFullURLForMessageEndpoint configuration.
func (s *SSEServer) GetMessageEndpointForClient(sessionID string) string {
	messageEndpoint := s.messageEndpoint
	if s.useFullURLForMessageEndpoint {
		messageEndpoint = s.CompleteMessageEndpoint()
	}
	return fmt.Sprintf("%s?sessionId=%s", messageEndpoint, sessionID)
}

// handleMessage processes incoming JSON-RPC messages from clients and sends responses
// back through both the SSE connection and HTTP response.
func (s *SSEServer) handleMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeJSONRPCError(w, nil, mcp.INVALID_REQUEST, "Method not allowed")
		return
	}

	sessionID := r.URL.Query().Get("sessionId")
	if sessionID == "" {
		s.writeJSONRPCError(w, nil, mcp.INVALID_PARAMS, "Missing sessionId")
		return
	}
	sessionI, ok := s.sessions.Load(sessionID)
	if !ok {
		// It's possible the session timed out or disconnected between getting the
		// endpoint and sending the message.
		s.writeJSONRPCError(w, nil, mcp.INVALID_PARAMS, "Invalid or expired session ID")
		return
	}
	session := sessionI.(*clientSession) // Now retrieving *clientSession

	// Set the client context using the logical session before handling the message
	// Use the request's context as the base.
	ctx := s.server.WithContext(r.Context(), session)
	if s.contextFunc != nil {
		ctx = s.contextFunc(ctx, r)
	}

	// Parse message as raw JSON
	var rawMessage json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&rawMessage); err != nil {
		s.writeJSONRPCError(w, nil, mcp.PARSE_ERROR, "Parse error")
		return
	}

	// Process message through MCPServer using the logical session context
	response := s.server.HandleMessage(ctx, rawMessage)

	// Only send response if there is one (not for notifications)
	if response != nil {
		// Get the active connection associated with the session *at this moment*.
		connection := session.getActiveConnection()

		// Try to send the response via the active SSE connection's queue
		if connection != nil {
			eventData, err := json.Marshal(response)
			// Handle potential marshalling error
			if err != nil {
				// Log marshalling error - This response won't be sent via SSE
			} else {
				queued := connection.queueEvent(fmt.Sprintf("event: message\ndata: %s\n\n", eventData))
				if !queued {
					// Log error: Failed to queue SSE response (queue full or connection closed)
				}
			}
		} else {
			// Log info/warn: No active SSE connection found for session to send response
		}

		// Send HTTP response (this is the POST response, separate from SSE)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)  // Stick with 202 Accepted as per original code
		json.NewEncoder(w).Encode(response) // Error handling for Encode?
	} else {
		// For notifications processed via POST, just send 202 Accepted with no body
		w.WriteHeader(http.StatusAccepted)
	}
}

// writeJSONRPCError writes a JSON-RPC error response with the given error details.
func (s *SSEServer) writeJSONRPCError(
	w http.ResponseWriter,
	id interface{},
	code int,
	message string,
) {
	response := createErrorResponse(id, code, message)
	w.Header().Set("Content-Type", "application/json")
	// Determine appropriate status code based on JSON-RPC error
	// For simplicity, keeping Bad Request for now, but could be more specific.
	statusCode := http.StatusBadRequest
	if code == mcp.INVALID_PARAMS || code == mcp.INVALID_REQUEST || code == mcp.PARSE_ERROR {
		statusCode = http.StatusBadRequest
	} else if code == mcp.METHOD_NOT_FOUND || code == mcp.RESOURCE_NOT_FOUND { // Assuming RESOURCE_NOT_FOUND maps here too
		statusCode = http.StatusNotFound // Or maybe Bad Request depending on interpretation
	} else {
		statusCode = http.StatusInternalServerError // Default for internal errors
	}

	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(response) // Error handling for Encode?
}

// SendEventToSession sends an event to a specific SSE session identified by sessionID.
// Returns an error if the session is not found or has no active connection.
func (s *SSEServer) SendEventToSession(
	sessionID string,
	event interface{},
) error {
	sessionI, ok := s.sessions.Load(sessionID)
	if !ok {
		return fmt.Errorf("session not found: %s", sessionID)
	}
	session := sessionI.(*clientSession) // Now retrieving *clientSession

	connection := session.getActiveConnection()
	if connection == nil {
		return fmt.Errorf("no active connection found for session: %s", sessionID)
	}

	eventData, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// Queue the event for sending via the specific SSE connection
	if !connection.queueEvent(fmt.Sprintf("event: message\ndata: %s\n\n", eventData)) {
		// Check if the connection was closed or queue was full
		select {
		case <-connection.done:
			return fmt.Errorf("session connection closed for session %s", sessionID)
		default:
			return fmt.Errorf("event queue full for connection associated with session %s", sessionID)
		}
	}
	return nil
}

func (s *SSEServer) GetUrlPath(input string) (string, error) {
	parse, err := url.Parse(input)
	if err != nil {
		return "", fmt.Errorf("failed to parse URL %s: %w", input, err)
	}
	return parse.Path, nil
}

func (s *SSEServer) CompleteSseEndpoint() string {
	return s.baseURL + s.basePath + s.sseEndpoint
}

func (s *SSEServer) CompleteSsePath() string {
	path, err := s.GetUrlPath(s.CompleteSseEndpoint())
	if err != nil {
		return s.basePath + s.sseEndpoint
	}
	return path
}

func (s *SSEServer) CompleteMessageEndpoint() string {
	return s.baseURL + s.basePath + s.messageEndpoint
}

func (s *SSEServer) CompleteMessagePath() string {
	path, err := s.GetUrlPath(s.CompleteMessageEndpoint())
	if err != nil {
		return s.basePath + s.messageEndpoint
	}
	return path
}

// ServeHTTP implements the http.Handler interface.
func (s *SSEServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	// Use exact path matching rather than Contains
	ssePath := s.CompleteSsePath()
	if ssePath != "" && path == ssePath {
		s.handleSSE(w, r)
		return
	}
	messagePath := s.CompleteMessagePath()
	if messagePath != "" && path == messagePath {
		s.handleMessage(w, r)
		return
	}

	http.NotFound(w, r)
}
