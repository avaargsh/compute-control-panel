// Package consoleauth provides a narrowly scoped, HTTPS-only browser session
// boundary for the read-only Console API. It is not automatically mounted in
// the default CCP service and does not implement enterprise identity federation.
package consoleauth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/httpapi"
	"golang.org/x/crypto/bcrypt"
)

const (
	cookieName     = "__Host-ccp_session"
	maxLoginBytes  = 4096
	maxSessions    = 32
	maxBadAttempts = 5
	failureWindow  = time.Minute
)

var ErrUnauthenticated = errors.New("browser session missing, invalid or expired")
var ErrAccessDenied = errors.New("console observation project access denied")

// Config must be supplied by trusted server configuration, never from HTTP
// request fields. PasswordBcryptHash must be generated and stored separately.
type Config struct {
	Username           string
	PasswordBcryptHash []byte
	TenantID           string
	ProjectID          string
	Origin             string
	SessionTTL         time.Duration
	Now                func() time.Time
}

type storedSession struct {
	expires time.Time
	csrf    string
}

type Manager struct {
	cfg        Config
	originHost string

	mu       sync.Mutex
	sessions map[[32]byte]storedSession
	attempts int
	retryAt  time.Time
}

// NewHandler composes a session-scoped read-only API. The returned handler
// requires direct TLS. It is intentionally NOT registered by the default
// cmd/compute-control-panel/main.go process.
func NewHandler(cfg Config, reader httpapi.ObservationReader) (http.Handler, error) {
	// Interfaces may contain a typed-nil Reader. Such an implementation can
	// pass a plain nil check and panic only when an authenticated read occurs.
	readerMissing := reader == nil
	if !readerMissing {
		value := reflect.ValueOf(reader)
		switch value.Kind() {
		case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
			readerMissing = value.IsNil()
		}
	}
	if readerMissing || cfg.Now == nil ||
		strings.TrimSpace(cfg.Username) == "" ||
		strings.TrimSpace(cfg.TenantID) == "" ||
		strings.TrimSpace(cfg.ProjectID) == "" ||
		cfg.SessionTTL < 5*time.Minute || cfg.SessionTTL > 8*time.Hour {
		return nil, errors.New("console auth requires explicit identity, scope, clock, session expiry and reader")
	}
	cost, err := bcrypt.Cost(cfg.PasswordBcryptHash)
	if err != nil || cost < bcrypt.DefaultCost {
		return nil, errors.New("console admin bcrypt verifier is missing or has insufficient cost")
	}
	u, err := url.Parse(cfg.Origin)
	if err != nil || u == nil || u.Scheme != "https" || u.Host == "" ||
		u.User != nil || u.RawQuery != "" || u.Fragment != "" ||
		u.Opaque != "" || (u.Path != "" && u.Path != "/") ||
		!strings.HasPrefix(cfg.Origin, "https://") {
		return nil, errors.New("console origin must be an exact HTTPS origin")
	}
	if u.Path == "/" || cfg.Origin != "https://"+u.Host {
		return nil, errors.New("console origin cannot include a path or extra components")
	}
	cfg.PasswordBcryptHash = append([]byte(nil), cfg.PasswordBcryptHash...)
	m := &Manager{
		cfg: cfg, originHost: u.Host,
		sessions: make(map[[32]byte]storedSession),
	}
	observations, err := httpapi.NewObservationReadHandler(httpapi.ObservationReadDependencies{
		Authenticator: m, Authorizer: m, Reader: reader, Now: cfg.Now,
	})
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.Handle("/api/v1/observations", observations)
	mux.HandleFunc("POST /api/v1/console/login", m.login)
	mux.HandleFunc("POST /api/v1/console/logout", m.logout)
	mux.HandleFunc("GET /api/v1/console/session", m.sessionStatus)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'; base-uri 'none'")
		if r.TLS == nil || r.Host != m.originHost {
			http.Error(w, "HTTPS origin required", http.StatusForbidden)
			return
		}
		mux.ServeHTTP(w, r)
	}), nil
}

// Authenticate implements httpapi.WorkloadAuthenticator for read-only
// observation reads. It never trusts project or tenant fields from a browser.
func (m *Manager) Authenticate(_ context.Context, r *http.Request) (httpapi.WorkloadPrincipal, error) {
	if m == nil || r == nil || r.TLS == nil || r.Host != m.originHost {
		return httpapi.WorkloadPrincipal{}, ErrUnauthenticated
	}
	_, _, err := m.authSession(r)
	if err != nil {
		return httpapi.WorkloadPrincipal{}, ErrUnauthenticated
	}
	return httpapi.WorkloadPrincipal{
		TenantID:   m.cfg.TenantID,
		Subject:    "ccp-console-admin:" + m.cfg.Username,
		Service:    "console",
		ProjectIDs: []string{m.cfg.ProjectID},
	}, nil
}

// AuthorizeObservationRead is an independent, fixed-project grant. A runtime
// directory-backed authorizer is needed before multi-admin production use.
func (m *Manager) AuthorizeObservationRead(_ context.Context, principal httpapi.WorkloadPrincipal) error {
	if m == nil || principal.TenantID != m.cfg.TenantID ||
		principal.Subject != "ccp-console-admin:"+m.cfg.Username ||
		principal.Service != "console" || len(principal.ProjectIDs) != 1 ||
		principal.ProjectIDs[0] != m.cfg.ProjectID {
		return ErrAccessDenied
	}
	return nil
}

func (m *Manager) authSession(r *http.Request) ([32]byte, storedSession, error) {
	var empty [32]byte
	cookie, err := r.Cookie(cookieName)
	if err != nil {
		return empty, storedSession{}, ErrUnauthenticated
	}
	token, err := base64.RawURLEncoding.DecodeString(cookie.Value)
	if err != nil || len(token) != 32 {
		return empty, storedSession{}, ErrUnauthenticated
	}
	id := sha256.Sum256(token)
	now := m.cfg.Now().UTC()
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[id]
	if !ok || now.IsZero() || !now.Before(s.expires) {
		delete(m.sessions, id)
		return empty, storedSession{}, ErrUnauthenticated
	}
	return id, s, nil
}

func (m *Manager) allowedMutation(r *http.Request) bool {
	// Strict same-origin checks cover login CSRF even before a session exists.
	// Never trust forwarded-host/forwarded-proto headers to satisfy TLS.
	site := r.Header.Get("Sec-Fetch-Site")
	return r.TLS != nil && r.Host == m.originHost &&
		r.Header.Get("Origin") == m.cfg.Origin &&
		(site == "" || site == "same-origin")
}

func (m *Manager) login(w http.ResponseWriter, r *http.Request) {
	if !m.allowedMutation(r) {
		http.Error(w, "same-origin HTTPS request required", http.StatusForbidden)
		return
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" || r.URL.RawQuery != "" {
		http.Error(w, "invalid login request", http.StatusBadRequest)
		return
	}
	var body struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxLoginBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil || body.Username == "" || body.Password == "" ||
		len(body.Password) > 72 || len(body.Username) > 256 {
		http.Error(w, "invalid login request", http.StatusBadRequest)
		return
	}
	var tail any
	if err := decoder.Decode(&tail); err != io.EOF {
		http.Error(w, "invalid login request", http.StatusBadRequest)
		return
	}

	now := m.cfg.Now().UTC()
	m.mu.Lock()
	defer m.mu.Unlock()
	if now.IsZero() {
		http.Error(w, "login unavailable", http.StatusServiceUnavailable)
		return
	}
	if !m.retryAt.IsZero() && now.Before(m.retryAt) {
		http.Error(w, "login temporarily rate limited", http.StatusTooManyRequests)
		return
	}
	if !m.retryAt.IsZero() && !now.Before(m.retryAt) {
		m.attempts = 0
		m.retryAt = time.Time{}
	}
	// Compare the password even for the wrong username. Neither branch reveals
	// whether a username exists in this single-administrator preview.
	passwordErr := bcrypt.CompareHashAndPassword(m.cfg.PasswordBcryptHash, []byte(body.Password))
	if body.Username != m.cfg.Username || passwordErr != nil {
		m.attempts++
		if m.attempts >= maxBadAttempts {
			m.retryAt = now.Add(failureWindow)
		}
		http.Error(w, "invalid credentials", http.StatusUnauthorized)
		return
	}
	m.attempts = 0
	m.retryAt = time.Time{}
	for id, s := range m.sessions {
		if !now.Before(s.expires) {
			delete(m.sessions, id)
		}
	}
	if len(m.sessions) >= maxSessions {
		http.Error(w, "session limit reached", http.StatusServiceUnavailable)
		return
	}
	var token, csrfBytes [32]byte
	if _, err := rand.Read(token[:]); err != nil {
		http.Error(w, "session initialization unavailable", http.StatusServiceUnavailable)
		return
	}
	if _, err := rand.Read(csrfBytes[:]); err != nil {
		http.Error(w, "session initialization unavailable", http.StatusServiceUnavailable)
		return
	}
	rawToken := base64.RawURLEncoding.EncodeToString(token[:])
	id := sha256.Sum256(token[:])
	m.sessions[id] = storedSession{
		expires: now.Add(m.cfg.SessionTTL),
		csrf:    base64.RawURLEncoding.EncodeToString(csrfBytes[:]),
	}
	http.SetCookie(w, &http.Cookie{
		Name: cookieName, Value: rawToken, Path: "/",
		HttpOnly: true, Secure: true, SameSite: http.SameSiteStrictMode,
		MaxAge: int(m.cfg.SessionTTL.Seconds()),
	})
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Authenticated bool `json:"authenticated"`
	}{Authenticated: true})
}

func (m *Manager) sessionStatus(w http.ResponseWriter, r *http.Request) {
	_, session, err := m.authSession(r)
	if err != nil {
		http.Error(w, "authentication required", http.StatusUnauthorized)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Authenticated bool   `json:"authenticated"`
		ProjectID     string `json:"project_id"`
		CSRFToken     string `json:"csrf_token"`
	}{
		Authenticated: true, ProjectID: m.cfg.ProjectID, CSRFToken: session.csrf,
	})
}

func (m *Manager) logout(w http.ResponseWriter, r *http.Request) {
	if !m.allowedMutation(r) {
		http.Error(w, "same-origin HTTPS request required", http.StatusForbidden)
		return
	}
	id, session, err := m.authSession(r)
	if err != nil {
		http.Error(w, "authentication required", http.StatusUnauthorized)
		return
	}
	header := r.Header.Get("X-CCP-CSRF")
	if len(header) == 0 || len(header) != len(session.csrf) ||
		subtle.ConstantTimeCompare([]byte(header), []byte(session.csrf)) != 1 {
		http.Error(w, "CSRF token required", http.StatusForbidden)
		return
	}
	m.mu.Lock()
	delete(m.sessions, id)
	m.mu.Unlock()
	http.SetCookie(w, &http.Cookie{
		Name: cookieName, Value: "", Path: "/", HttpOnly: true,
		Secure: true, SameSite: http.SameSiteStrictMode, MaxAge: -1,
	})
	w.WriteHeader(http.StatusNoContent)
}
