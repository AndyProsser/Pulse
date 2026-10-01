package websocket

import (
	"bytes"
	"encoding/json"
	"errors"
	"slices"
	"unicode/utf8"
)

// The state snapshot and the per-entry merge patch both walk large, already
// encoded JSON documents. Decoding those into interface{} trees or repeated
// RawMessage copies dominated the broadcast worker (#2199), so this file holds a
// strict single-pass scanner that hands back sub-slices of its input instead.
//
// The scanner validates the full JSON grammar as it goes. Anything it does not
// handle, or any malformed input, is reported as errScanUnsupported and the
// caller falls back to the encoding/json reference implementation, which keeps
// that implementation's exact semantics (and error values) for odd inputs.

var errScanUnsupported = errors.New("json shape not handled by the fast scanner")

// maxScanDepth matches encoding/json's nesting limit.
const maxScanDepth = 10000

type jsonScanner struct {
	b []byte
	i int
}

func (s *jsonScanner) skipSpace() {
	for s.i < len(s.b) {
		switch s.b[s.i] {
		case ' ', '\t', '\n', '\r':
			s.i++
		default:
			return
		}
	}
}

// atEnd reports whether only whitespace remains.
func (s *jsonScanner) atEnd() bool {
	s.skipSpace()
	return s.i == len(s.b)
}

// stringSpecial marks the bytes a string scan must look at: the closing quote,
// the escape introducer and control characters (invalid unescaped in JSON).
var stringSpecial = func() (t [256]bool) {
	for c := 0; c < 0x20; c++ {
		t[c] = true
	}
	t['"'], t['\\'] = true, true
	return t
}()

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func isHex(c byte) bool {
	return isDigit(c) || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

// scanString consumes a string starting at the opening quote and reports its
// raw content (between the quotes) and whether it contains escapes.
func (s *jsonScanner) scanString() (content []byte, escaped bool, err error) {
	if s.i >= len(s.b) || s.b[s.i] != '"' {
		return nil, false, errScanUnsupported
	}
	s.i++
	start := s.i
	for s.i < len(s.b) {
		// Skip the plain run in a tight loop; most string bytes are ordinary.
		for s.i < len(s.b) && !stringSpecial[s.b[s.i]] {
			s.i++
		}
		if s.i >= len(s.b) {
			break
		}
		c := s.b[s.i]
		switch {
		case c == '"':
			content = s.b[start:s.i]
			s.i++
			return content, escaped, nil
		case c < 0x20:
			return nil, false, errScanUnsupported
		case c == '\\':
			escaped = true
			s.i++
			if s.i >= len(s.b) {
				return nil, false, errScanUnsupported
			}
			switch s.b[s.i] {
			case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
				s.i++
			case 'u':
				if s.i+4 >= len(s.b) ||
					!isHex(s.b[s.i+1]) || !isHex(s.b[s.i+2]) || !isHex(s.b[s.i+3]) || !isHex(s.b[s.i+4]) {
					return nil, false, errScanUnsupported
				}
				s.i += 5
			default:
				return nil, false, errScanUnsupported
			}
		default:
			s.i++
		}
	}
	return nil, false, errScanUnsupported
}

func (s *jsonScanner) scanLiteral(word string) error {
	if len(s.b)-s.i < len(word) || string(s.b[s.i:s.i+len(word)]) != word {
		return errScanUnsupported
	}
	s.i += len(word)
	return nil
}

func (s *jsonScanner) scanNumber() error {
	if s.i < len(s.b) && s.b[s.i] == '-' {
		s.i++
	}
	if s.i >= len(s.b) {
		return errScanUnsupported
	}
	switch {
	case s.b[s.i] == '0':
		s.i++
	case s.b[s.i] >= '1' && s.b[s.i] <= '9':
		for s.i < len(s.b) && isDigit(s.b[s.i]) {
			s.i++
		}
	default:
		return errScanUnsupported
	}
	if s.i < len(s.b) && s.b[s.i] == '.' {
		s.i++
		if s.i >= len(s.b) || !isDigit(s.b[s.i]) {
			return errScanUnsupported
		}
		for s.i < len(s.b) && isDigit(s.b[s.i]) {
			s.i++
		}
	}
	if s.i < len(s.b) && (s.b[s.i] == 'e' || s.b[s.i] == 'E') {
		s.i++
		if s.i < len(s.b) && (s.b[s.i] == '+' || s.b[s.i] == '-') {
			s.i++
		}
		if s.i >= len(s.b) || !isDigit(s.b[s.i]) {
			return errScanUnsupported
		}
		for s.i < len(s.b) && isDigit(s.b[s.i]) {
			s.i++
		}
	}
	return nil
}

// skipValue consumes one value of any kind, validating it.
func (s *jsonScanner) skipValue(depth int) error {
	if depth > maxScanDepth {
		return errScanUnsupported
	}
	s.skipSpace()
	if s.i >= len(s.b) {
		return errScanUnsupported
	}
	switch c := s.b[s.i]; {
	case c == '"':
		_, _, err := s.scanString()
		return err
	case c == '{':
		return s.scanObject(depth+1, nil)
	case c == '[':
		return s.scanArray(depth+1, nil)
	case c == 't':
		return s.scanLiteral("true")
	case c == 'f':
		return s.scanLiteral("false")
	case c == 'n':
		return s.scanLiteral("null")
	case c == '-' || isDigit(c):
		return s.scanNumber()
	}
	return errScanUnsupported
}

// scanObject consumes an object, calling fn (when non-nil) for every member.
// key is the raw key content (still escaped when escaped is true) and value
// the raw value bytes; both alias the scanner's input.
func (s *jsonScanner) scanObject(depth int, fn func(key []byte, escaped bool, value []byte) error) error {
	if depth > maxScanDepth {
		return errScanUnsupported
	}
	s.i++ // '{'
	s.skipSpace()
	if s.i < len(s.b) && s.b[s.i] == '}' {
		s.i++
		return nil
	}
	for {
		s.skipSpace()
		key, escaped, err := s.scanString()
		if err != nil {
			return err
		}
		s.skipSpace()
		if s.i >= len(s.b) || s.b[s.i] != ':' {
			return errScanUnsupported
		}
		s.i++
		s.skipSpace()
		start := s.i
		if err := s.skipValue(depth); err != nil {
			return err
		}
		if fn != nil {
			if err := fn(key, escaped, s.b[start:s.i]); err != nil {
				return err
			}
		}
		s.skipSpace()
		if s.i >= len(s.b) {
			return errScanUnsupported
		}
		switch s.b[s.i] {
		case ',':
			s.i++
		case '}':
			s.i++
			return nil
		default:
			return errScanUnsupported
		}
	}
}

// scanArray consumes an array, calling fn (when non-nil) for every element.
func (s *jsonScanner) scanArray(depth int, fn func(elem []byte) error) error {
	if depth > maxScanDepth {
		return errScanUnsupported
	}
	s.i++ // '['
	s.skipSpace()
	if s.i < len(s.b) && s.b[s.i] == ']' {
		s.i++
		return nil
	}
	for {
		s.skipSpace()
		start := s.i
		if err := s.skipValue(depth); err != nil {
			return err
		}
		if fn != nil {
			if err := fn(s.b[start:s.i]); err != nil {
				return err
			}
		}
		s.skipSpace()
		if s.i >= len(s.b) {
			return errScanUnsupported
		}
		switch s.b[s.i] {
		case ',':
			s.i++
		case ']':
			s.i++
			return nil
		default:
			return errScanUnsupported
		}
	}
}

// forEachObjectMember scans b as exactly one JSON object.
func forEachObjectMember(b []byte, fn func(key []byte, escaped bool, value []byte) error) error {
	s := jsonScanner{b: b}
	s.skipSpace()
	if s.i >= len(b) || b[s.i] != '{' {
		return errScanUnsupported
	}
	if err := s.scanObject(1, fn); err != nil {
		return err
	}
	if !s.atEnd() {
		return errScanUnsupported
	}
	return nil
}

// forEachArrayElement scans b as exactly one JSON array.
func forEachArrayElement(b []byte, fn func(elem []byte) error) error {
	s := jsonScanner{b: b}
	s.skipSpace()
	if s.i >= len(b) || b[s.i] != '[' {
		return errScanUnsupported
	}
	if err := s.scanArray(1, fn); err != nil {
		return err
	}
	if !s.atEnd() {
		return errScanUnsupported
	}
	return nil
}

// validateJSONValue reports whether b is exactly one valid JSON value.
func validateJSONValue(b []byte) error {
	s := jsonScanner{b: b}
	if err := s.skipValue(1); err != nil {
		return err
	}
	if !s.atEnd() {
		return errScanUnsupported
	}
	return nil
}

func unquoteKey(key []byte, escaped bool) (string, error) {
	if !escaped {
		return string(key), nil
	}
	var out string
	quoted := make([]byte, 0, len(key)+2)
	quoted = append(quoted, '"')
	quoted = append(quoted, key...)
	quoted = append(quoted, '"')
	if err := json.Unmarshal(quoted, &out); err != nil {
		return "", errScanUnsupported
	}
	return out, nil
}

func isObjectValue(b []byte) bool {
	return len(b) > 0 && b[0] == '{'
}

// entryID extracts the top-level "id" of one encoded entry the way
// encoding/json decodes it into struct{ ID string `json:"id"` }: the key
// matches case-insensitively, the last occurrence wins, null is ignored and any
// other non-string value is an error. Escaped keys, non-UTF-8 ids and anything
// unusual are reported as unsupported so the reference decoder decides.
func entryID(entry []byte) (string, error) {
	var id string
	err := forEachObjectMember(entry, func(key []byte, escaped bool, value []byte) error {
		if escaped {
			return errScanUnsupported
		}
		if len(key) != 2 || (key[0]|0x20) != 'i' || (key[1]|0x20) != 'd' {
			return nil
		}
		switch {
		case bytes.Equal(value, []byte("null")):
			return nil
		case len(value) >= 2 && value[0] == '"':
			content := value[1 : len(value)-1]
			if bytes.IndexByte(content, '\\') >= 0 {
				return errScanUnsupported
			}
			if !utf8.Valid(content) {
				return errScanUnsupported
			}
			id = string(content)
			return nil
		}
		return errScanUnsupported
	})
	return id, err
}

// extractKeyedEntriesFast is extractKeyedEntries for an encoded array the
// caller owns: entries are capped sub-slices of encoded rather than copies.
// Every failure is errScanUnsupported so the reference decoder can report it.
func extractKeyedEntriesFast(encoded []byte) (map[string]json.RawMessage, []string, error) {
	byID := make(map[string]json.RawMessage)
	order := make([]string, 0)
	err := forEachArrayElement(encoded, func(entry []byte) error {
		id, err := entryID(entry)
		if err != nil {
			return err
		}
		if id == "" {
			return errScanUnsupported
		}
		if _, exists := byID[id]; exists {
			return errScanUnsupported
		}
		byID[id] = entry[:len(entry):len(entry)]
		order = append(order, id)
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return byID, order, nil
}

// rawMember is one member of an object; key is the unescaped key bytes and both
// slices alias the scanned input.
type rawMember struct {
	key   []byte
	value []byte
}

// sortedObjectMembers returns an object's members ordered by key. Escaped keys
// and duplicate keys are reported as unsupported: both need encoding/json's
// exact handling, which the reference implementation provides.
func sortedObjectMembers(b []byte) ([]rawMember, error) {
	members := make([]rawMember, 0, 16)
	err := forEachObjectMember(b, func(key []byte, escaped bool, value []byte) error {
		if escaped || !utf8.Valid(key) {
			return errScanUnsupported
		}
		members = append(members, rawMember{key: key, value: value})
		return nil
	})
	if err != nil {
		return nil, err
	}
	slices.SortFunc(members, func(a, b rawMember) int { return bytes.Compare(a.key, b.key) })
	for i := 1; i < len(members); i++ {
		if bytes.Equal(members[i-1].key, members[i].key) {
			return nil, errScanUnsupported
		}
	}
	return members, nil
}

func appendMemberPrefix(dst, key []byte, first bool) []byte {
	if !first {
		dst = append(dst, ',')
	}
	dst = append(dst, '"')
	dst = append(dst, key...)
	return append(dst, '"', ':')
}

// appendMergePatch appends the RFC 7386 patch turning previous into current to
// dst and reports whether anything changed (dst is returned untouched when it
// did not). It diffs the raw encoded bytes by merge-joining the two objects'
// sorted members, so unchanged entries and values are never decoded, and values
// the patch carries keep their original encoding (large integers are not
// squeezed through float64). With top set, a patch that exists always carries
// the current "id" so receivers can address it.
func appendMergePatch(dst, previous, current []byte, top bool) ([]byte, bool, error) {
	previousMembers, err := sortedObjectMembers(previous)
	if err != nil {
		return dst, false, err
	}
	currentMembers, err := sortedObjectMembers(current)
	if err != nil {
		return dst, false, err
	}
	start := len(dst)
	dst = append(dst, '{')
	first := true
	changedAny := false
	i, j := 0, 0
	for i < len(previousMembers) || j < len(currentMembers) {
		switch {
		case j >= len(currentMembers) || (i < len(previousMembers) && bytes.Compare(previousMembers[i].key, currentMembers[j].key) < 0):
			dst = appendMemberPrefix(dst, previousMembers[i].key, first)
			dst = append(dst, "null"...)
			first, changedAny = false, true
			i++
		case i >= len(previousMembers) || bytes.Compare(currentMembers[j].key, previousMembers[i].key) < 0:
			dst = appendMemberPrefix(dst, currentMembers[j].key, first)
			dst = append(dst, currentMembers[j].value...)
			first, changedAny = false, true
			j++
		default:
			key, previousValue, currentValue := currentMembers[j].key, previousMembers[i].value, currentMembers[j].value
			i, j = i+1, j+1
			if bytes.Equal(previousValue, currentValue) {
				if top && string(key) == "id" {
					// Addressing member: kept only if something else changed.
					dst = appendMemberPrefix(dst, key, first)
					dst = append(dst, currentValue...)
					first = false
				}
				continue
			}
			changedAny = true
			if isObjectValue(previousValue) && isObjectValue(currentValue) {
				mark, changedAnyBefore := len(dst), changedAny
				dst = appendMemberPrefix(dst, key, first)
				var nestedChanged bool
				dst, nestedChanged, err = appendMergePatch(dst, previousValue, currentValue, false)
				if err != nil {
					return dst, false, err
				}
				if !nestedChanged {
					dst = dst[:mark]
					changedAny = changedAnyBefore
					continue
				}
				first = false
				continue
			}
			dst = appendMemberPrefix(dst, key, first)
			dst = append(dst, currentValue...)
			first = false
		}
	}
	if !changedAny {
		return dst[:start], false, nil
	}
	return append(dst, '}'), true, nil
}

// trimJSONSpace trims only the four whitespace bytes JSON allows.
func trimJSONSpace(b []byte) []byte {
	for len(b) > 0 && (b[0] == ' ' || b[0] == '\t' || b[0] == '\n' || b[0] == '\r') {
		b = b[1:]
	}
	for len(b) > 0 {
		c := b[len(b)-1]
		if c != ' ' && c != '\t' && c != '\n' && c != '\r' {
			break
		}
		b = b[:len(b)-1]
	}
	return b
}
