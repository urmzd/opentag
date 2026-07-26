package registry

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
)

// tokenVersion prefixes every continuation token. A token outlives the request
// that issued it, so the encoding has to be able to change without silently
// reinterpreting tokens issued by the previous one.
const tokenVersion = 1

// encodeToken produces the opaque continuation token for a listing that stopped
// after name.
//
// It carries the tenant as well as the position. A token is a client-held value
// that comes back on a later request, and a listing resumed under a different
// tenant than it was issued for is a bug worth reporting rather than silently
// resuming at whatever that name happens to mean over there. The fields are
// length-prefixed for the same reason pkg/agentspec's hash encoding is: without
// it, tenant "ab" with agent "c" and tenant "a" with agent "bc" would produce
// the same token, and the check would pass for the wrong pair.
func encodeToken(tenant, name string) string {
	buf := binary.AppendUvarint(nil, tokenVersion)
	buf = appendField(buf, tenant)
	buf = appendField(buf, name)
	return base64.RawURLEncoding.EncodeToString(buf)
}

// decodeToken returns the name a listing resumes after. An empty token starts
// at the beginning.
func decodeToken(token, tenant string) (string, error) {
	if token == "" {
		return "", nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return "", fmt.Errorf("%w: page token is not a token this registry issued: %w", ErrInvalid, err)
	}
	version, n := binary.Uvarint(raw)
	if n <= 0 {
		return "", fmt.Errorf("%w: page token has no version", ErrInvalid)
	}
	if version != tokenVersion {
		return "", fmt.Errorf("%w: page token is version %d, this registry issues version %d", ErrInvalid, version, tokenVersion)
	}
	raw = raw[n:]
	issuedFor, raw, err := readField(raw)
	if err != nil {
		return "", fmt.Errorf("%w: page token tenant: %w", ErrInvalid, err)
	}
	name, raw, err := readField(raw)
	if err != nil {
		return "", fmt.Errorf("%w: page token position: %w", ErrInvalid, err)
	}
	if len(raw) != 0 {
		return "", fmt.Errorf("%w: page token has %d trailing bytes", ErrInvalid, len(raw))
	}
	if issuedFor != tenant {
		return "", fmt.Errorf("%w: page token was issued for another tenant", ErrInvalid)
	}
	if name == "" {
		return "", fmt.Errorf("%w: page token names no position", ErrInvalid)
	}
	return name, nil
}

func appendField(buf []byte, s string) []byte {
	buf = binary.AppendUvarint(buf, uint64(len(s)))
	return append(buf, s...)
}

func readField(buf []byte) (string, []byte, error) {
	size, n := binary.Uvarint(buf)
	if n <= 0 {
		return "", nil, fmt.Errorf("length prefix is malformed")
	}
	buf = buf[n:]
	if uint64(len(buf)) < size {
		return "", nil, fmt.Errorf("field claims %d bytes, %d remain", size, len(buf))
	}
	return string(buf[:size]), buf[size:], nil
}
