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

	"github.com/clayallsopp/mcp-go/mcp"
	"github.com/google/uuid"
)

// sseConnection represents an active SSE connection linked to a clientSession.
type sseConnection struct {
	writer    http.ResponseWriter
	flusher   http.Flusher
	done      chan struct{}
	requestID atomic.Int64
	session   *SSESession // Reference back to the logical session
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

type SSESession interface {
	ClientSession

	// PublishEvent sends an event string (intended for the client) to the session's backing store.
	// This might involve writing to a channel, publishing to Redis, etc.
	// It should be non-blocking or handle backpressure appropriately.
	PublishEvent(ctx context.Context, event string) error

	// SubscribeEvents returns a read-only channel that delivers events for this session.
	// The provided context should be used to signal when the subscription is no longer needed.
	// The implementer is responsible for closing the channel when the context is done.
	SubscribeEvents(ctx context.Context) <-chan string
}

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
	connections                  sync.Map // Stores *sseConnection, keyed by sessionID
	srv                          *http.Server
	contextFunc                  SSEContextFunc

	keepAlive         bool
	keepAliveInterval time.Duration

	sessionStore SSESessionStore

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

type SSESessionStore interface {
	SessionStore
	CreateSession(ctx context.Context, sessionID string) SSESession
	Load(ctx context.Context, sessionID string) (SSESession, bool)
}

// defaultSession represents a logical client session.
type defaultSession struct {
	sessionID string
	// Internal channels for the default in-memory implementation
	notificationChan chan mcp.JSONRPCNotification
	eventChan        chan string
	initialized      atomic.Bool
	closeOnce        sync.Once
	closed           chan struct{} // To signal subscribers to stop
}

func (cs *defaultSession) SessionID() string {
	return cs.sessionID
}

func (cs *defaultSession) NotificationChannel() chan mcp.JSONRPCNotification {
	return cs.notificationChan
}

func (cs *defaultSession) EventQueue() chan string {
	return cs.eventChan
}

func (cs *defaultSession) Initialize(ctx context.Context) {
	cs.initialized.Store(true)
}

func (cs *defaultSession) Initialized(ctx context.Context) bool {
	return cs.initialized.Load()
}

func (cs *defaultSession) PublishNotification(ctx context.Context, notification mcp.JSONRPCNotification) error {
	select {
	case <-cs.closed:
		return fmt.Errorf("session %s closed", cs.sessionID)
	case cs.notificationChan <- notification:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		// Non-blocking attempt, return error if full
		return fmt.Errorf("notification channel full for session %s", cs.sessionID)
	}
}

func (cs *defaultSession) PublishEvent(ctx context.Context, event string) error {
	select {
	case <-cs.closed:
		return fmt.Errorf("session %s closed", cs.sessionID)
	case cs.eventChan <- event:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		// Non-blocking attempt, return error if full
		return fmt.Errorf("event channel full for session %s", cs.sessionID)
	}
}

// SubscribeNotifications returns the internal channel.
// NOTE: This simple implementation doesn't handle multiple subscribers well if the
// underlying channel is closed by one. A more robust version might use a fan-out pattern.
func (cs *defaultSession) SubscribeNotifications(ctx context.Context) <-chan mcp.JSONRPCNotification {
	// Return the direct channel for the in-memory version.
	// The goroutine watching ctx.Done() is implicitly the one calling this method (in handleSSE).
	return cs.notificationChan
}

// SubscribeEvents returns the internal channel.
func (cs *defaultSession) SubscribeEvents(ctx context.Context) <-chan string {
	// Return the direct channel for the in-memory version.
	return cs.eventChan
}

// Close cleans up the default session resources (closes channels).
func (cs *defaultSession) Close() {
	cs.closeOnce.Do(func() {
		close(cs.closed) // Signal subscribers
		// Drain and close channels to prevent blocking publishes after close
		go func() {
			for range cs.notificationChan {
			}
		}()
		close(cs.notificationChan)
		go func() {
			for range cs.eventChan {
			}
		}()
		close(cs.eventChan)
	})
}

type defaultSseSessionStore struct {
	sessions sync.Map
}

func NewDefaultSSESessionStore() *defaultSseSessionStore {
	return &defaultSseSessionStore{
		sessions: sync.Map{},
	}
}

func (d *defaultSseSessionStore) CreateSession(ctx context.Context, sessionID string) SSESession {
	ds := &defaultSession{
		sessionID:        sessionID,
		notificationChan: make(chan mcp.JSONRPCNotification, 100),
		eventChan:        make(chan string, 100),
		initialized:      atomic.Bool{},
		closed:           make(chan struct{}),
	}
	return ds
}

func (d *defaultSseSessionStore) Load(ctx context.Context, sessionID string) (SSESession, bool) {
	sessionI, ok := d.sessions.Load(sessionID)
	if !ok {
		return nil, false
	}
	session := sessionI.(SSESession)
	return session, true
}

func (d *defaultSseSessionStore) LoadAndDelete(ctx context.Context, sessionID string) (ClientSession, bool) {
	sessionI, loaded := d.sessions.LoadAndDelete(sessionID)
	if !loaded {
		return nil, false
	}
	session := sessionI.(SSESession)
	return session, true
}

func (d *defaultSseSessionStore) LoadOrStore(ctx context.Context, sessionID string, session ClientSession) (ClientSession, bool) {
	sessionI, loaded := d.sessions.LoadOrStore(sessionID, session)
	if !loaded {
		return nil, false
	}
	return sessionI.(ClientSession), true
}

func WithSSESessionStore(store SSESessionStore) SSEOption {
	return func(s *SSEServer) {
		s.sessionStore = store
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
		s.connections.Range(func(key, value interface{}) bool {
			sessionID := key.(string)
			if connection, ok := value.(*sseConnection); ok {
				connection.close()
			}
			// Delete the session from the map during shutdown iteration
			s.connections.Delete(sessionID)
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
	ctx := r.Context()
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
	session := s.sessionStore.CreateSession(ctx, sessionID)

	// Create the connection associated with this request
	connection := &sseConnection{
		writer:  w,
		flusher: flusher,
		done:    make(chan struct{}),
		session: &session,
		// requestID starts at 0
	}

	// Store the logical session *before* registering it
	s.connections.Store(sessionID, connection)

	// Set the client context using the logical session before prcoeeding
	// Use the request's context as the base.
	// This happens before the RegisterSession is called so that the context
	// is available for the hooks.
	if s.contextFunc != nil {
		ctx = s.contextFunc(ctx, r)
	}

	// Defer cleanup for this connection handler
	defer func() {
		// Signal connection closure (if still using connection.done)
		connection.close()              // Consider removing sseConnection entirely if not needed
		s.connections.Delete(sessionID) // Remove the specific connection tracking

		// Use context.Background() for unregistration if request context might be done.
		// Unregister the *logical* session
		s.server.UnregisterSession(context.Background(), sessionID)
	}()

	// Register the logical session with the MCPServer
	// Pass the request context, tied to the connection lifetime.
	// MCPServer uses this context for its hooks if needed.
	if err := s.server.RegisterSession(ctx, session); err != nil {
		http.Error(w, fmt.Sprintf("Session registration failed: %v", err), http.StatusInternalServerError)
		// If registration fails after storing, remove from map before returning.
		s.connections.Delete(sessionID)
		// We don't run the deferred cleanup in this error path.
		return
	}
	// Note: The defer above handles UnregisterSession and other cleanup on normal exit.

	// Start notification handler for this session, sending to the active connection
	// This goroutine now publishes notifications *to* the session store.
	go func() {
		// Subscribe directly to the session's notification source
		notificationSubChan := session.SubscribeNotifications(ctx)
		reqCtxDone := ctx.Done()

		for {
			select {
			case notification, ok := <-notificationSubChan:
				if !ok {
					// Session closed or context cancelled
					return
				}
				// Publish the notification via the session's mechanism
				if err := session.PublishNotification(ctx, notification); err != nil {
					// Log or handle publish failure (e.g., session closed, Redis down)
				}
			case <-reqCtxDone:
				// Client disconnected (request context cancelled)
				return
			}
		}
	}()

	// Start keep alive : ping for this connection
	if s.keepAlive {
		go func() {
			// Ticker for interval
			ticker := time.NewTicker(s.keepAliveInterval)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					// Create ping message
					message := mcp.JSONRPCRequest{
						JSONRPC: "2.0",
						ID:      connection.requestID.Add(1), // Use connection's requestID
						Request: mcp.Request{
							Method: "ping",
						},
					}
					// Marshal and format as SSE event
					messageBytes, err := json.Marshal(message)
					if err != nil {
						// Log marshalling error for ping
						continue
					}
					pingMsg := fmt.Sprintf("event: message\ndata: %s\n\n", messageBytes)
					// Publish the ping event via the session
					if err := session.PublishEvent(ctx, pingMsg); err != nil {
						// Log or handle publish failure (e.g., session closed, Redis down)
						return // Stop trying if publish fails
					}
				case <-connection.done: // Listen to connection close signal
					return
				case <-ctx.Done(): // Also listen to request context cancellation
					return
				}
			}
		}()
	}

	// Send the initial endpoint event via the connection
	fmt.Fprintf(connection.writer, "event: endpoint\ndata: %s\r\n\r\n", s.GetMessageEndpointForClient(sessionID))
	connection.flusher.Flush()

	// Main event loop for this connection
	// Subscribe to events *from* the session store
	eventSubChan := session.SubscribeEvents(ctx)
	reqCtxDone := ctx.Done() // Cache request context's done channel

	for {
		select {
		case event, ok := <-eventSubChan:
			if !ok {
				// Subscription channel closed (e.g., context cancelled, session deleted)
				return
			}
			// Write the event received from the subscription to the HTTP response
			// Assume `event` is already correctly formatted SSE string
			if _, err := fmt.Fprint(connection.writer, event); err != nil {
				// Error writing to client, likely disconnected
				return // Exit loop, defer will handle cleanup
			}
			connection.flusher.Flush()
		case <-reqCtxDone:
			// Client disconnected (request context cancelled)
			return // Exit loop, defer will handle cleanup
		case <-connection.done:
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
	ctx := r.Context()
	if r.Method != http.MethodPost {
		s.writeJSONRPCError(w, nil, mcp.INVALID_REQUEST, "Method not allowed")
		return
	}

	sessionID := r.URL.Query().Get("sessionId")
	if sessionID == "" {
		s.writeJSONRPCError(w, nil, mcp.INVALID_PARAMS, "Missing sessionId")
		return
	}

	session, ok := s.sessionStore.Load(ctx, sessionID)
	if !ok {
		// It's possible the session timed out or disconnected between getting the
		// endpoint and sending the message.
		s.writeJSONRPCError(w, nil, mcp.INVALID_PARAMS, "Invalid or expired session ID")
		return
	}

	// Set the client context using the logical session before handling the message
	// Use the request's context as the base.
	ctx = s.server.WithContext(ctx, session)
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
		responseData, err := json.Marshal(response)
		// Handle potential marshalling error
		if err != nil {
			s.writeJSONRPCError(w, nil, mcp.INTERNAL_ERROR, "Failed to marshal response")
			// Log marshalling error - This response won't be sent via SSE either
		} else {
			// Format as SSE event and publish via the session
			sseEvent := fmt.Sprintf("event: message\ndata: %s\n\n", responseData)
			if pubErr := session.PublishEvent(ctx, sseEvent); pubErr != nil {
				// Log error: Failed to publish SSE response (e.g., session closed, Redis down)
			}
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
	ctx context.Context,
	sessionID string,
	event interface{},
) error {
	session, ok := s.sessionStore.Load(ctx, sessionID)
	if !ok {
		return fmt.Errorf("session not found: %s", sessionID)
	}

	eventData, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// Format as SSE event and publish via the session
	// Use context.Background() or a short timeout context for this server-initiated push
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second) // Example timeout
	defer cancel()
	sseEvent := fmt.Sprintf("event: message\ndata: %s\n\n", eventData)
	return session.PublishEvent(ctx, sseEvent)
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
