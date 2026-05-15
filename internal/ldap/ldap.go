// Package ldap provides LDAP directory authentication for WeKnora.
//
// When LDAPAuthConfig.Enable is true, the Login endpoint authenticates users
// against the corporate LDAP/AD server instead of the local password hash.
// On first successful LDAP login the user is auto-provisioned in WeKnora
// (random provisional password — never used for auth).
//
// Protocol:
//  1. Service-account bind (BindDN + BindPassword) to search for the user DN.
//  2. Re-bind with user DN + supplied password to verify credentials.
//  3. Return extracted email and display name attributes.
package ldap

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"

	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/logger"
	goldap "github.com/go-ldap/ldap/v3"
)

// UserAttrs contains the LDAP attributes extracted for a successfully
// authenticated user.
type UserAttrs struct {
	Email    string
	Username string // display name / CN
}

// Authenticate binds to the LDAP directory and verifies email + password.
// Returns UserAttrs on success, or an error describing the failure.
//
// The function is safe to call concurrently; each call opens and closes its
// own connection.
func Authenticate(ctx context.Context, cfg *config.LDAPAuthConfig, email, password string) (*UserAttrs, error) {
	if cfg == nil || !cfg.Enable {
		return nil, fmt.Errorf("ldap: authentication is not enabled")
	}

	email = strings.TrimSpace(email)
	password = strings.TrimSpace(password)
	if email == "" || password == "" {
		return nil, fmt.Errorf("ldap: email and password are required")
	}

	conn, err := dial(cfg)
	if err != nil {
		logger.Errorf(ctx, "[LDAP] dial failed host=%s: %v", cfg.Host, err)
		return nil, fmt.Errorf("ldap: cannot connect to directory server: %w", err)
	}
	defer conn.Close()

	// Step 1 — service-account bind so we can search for the user DN.
	if cfg.BindDN != "" {
		if err := conn.Bind(cfg.BindDN, cfg.BindPassword); err != nil {
			logger.Errorf(ctx, "[LDAP] service-account bind failed dn=%s: %v", cfg.BindDN, err)
			return nil, fmt.Errorf("ldap: service-account bind failed: %w", err)
		}
	}

	// Step 2 — search for the user entry.
	filter := buildFilter(cfg, email)
	emailAttr := attrOrDefault(cfg.EmailAttr, "mail")
	nameAttr := attrOrDefault(cfg.UsernameAttr, "displayName")

	searchReq := goldap.NewSearchRequest(
		cfg.UserSearchBase,
		goldap.ScopeWholeSubtree,
		goldap.NeverDerefAliases,
		1,    // size limit — we only expect one result
		0,    // time limit
		false,
		filter,
		[]string{"dn", emailAttr, nameAttr},
		nil,
	)

	result, err := conn.Search(searchReq)
	if err != nil {
		logger.Errorf(ctx, "[LDAP] search failed base=%s filter=%s: %v", cfg.UserSearchBase, filter, err)
		return nil, fmt.Errorf("ldap: user search failed: %w", err)
	}
	if len(result.Entries) == 0 {
		logger.Warnf(ctx, "[LDAP] user not found email=%s", email)
		return nil, fmt.Errorf("ldap: user not found")
	}

	entry := result.Entries[0]
	userDN := entry.DN

	// Step 3 — re-bind as the user to verify password.
	if err := conn.Bind(userDN, password); err != nil {
		logger.Warnf(ctx, "[LDAP] user bind failed dn=%s: %v", userDN, err)
		return nil, fmt.Errorf("ldap: invalid credentials")
	}

	attrs := &UserAttrs{
		Email:    getAttr(entry, emailAttr),
		Username: getAttr(entry, nameAttr),
	}
	// Fallback: if the directory doesn't return an email attribute, use the
	// login email the caller supplied.
	if attrs.Email == "" {
		attrs.Email = email
	}
	if attrs.Username == "" {
		// Derive display name from the email local part.
		attrs.Username = strings.SplitN(email, "@", 2)[0]
	}

	logger.Infof(ctx, "[LDAP] authenticated email=%s dn=%s", attrs.Email, userDN)
	return attrs, nil
}

// ─── internals ───────────────────────────────────────────────────────────────

func dial(cfg *config.LDAPAuthConfig) (*goldap.Conn, error) {
	addr := fmt.Sprintf("%s:%d", strings.TrimPrefix(strings.TrimPrefix(cfg.Host, "ldaps://"), "ldap://"), cfg.Port)

	if cfg.UseTLS || strings.HasPrefix(cfg.Host, "ldaps://") {
		return goldap.DialTLS("tcp", addr, &tls.Config{InsecureSkipVerify: false}) //nolint:gosec
	}

	conn, err := goldap.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func buildFilter(cfg *config.LDAPAuthConfig, email string) string {
	tmpl := cfg.UserFilter
	if tmpl == "" {
		tmpl = "(mail={email})"
	}
	// Escape the email value to prevent LDAP injection.
	safe := goldap.EscapeFilter(email)
	return strings.ReplaceAll(tmpl, "{email}", safe)
}

func getAttr(entry *goldap.Entry, name string) string {
	v := entry.GetAttributeValue(name)
	return strings.TrimSpace(v)
}

func attrOrDefault(v, def string) string {
	if strings.TrimSpace(v) == "" {
		return def
	}
	return v
}
