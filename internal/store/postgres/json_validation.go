package postgres

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"
)

const maxNormalizedRequestJSONDepth = 128

// strictJSONObject validates a complete UTF-8 JSON object, rejects duplicate
// keys recursively, and bounds attacker-controlled parser recursion. It then
// returns the root fields for domain-specific identity checks.
func strictJSONObject(raw []byte) (map[string]json.RawMessage, error) {
	if len(raw) == 0 || !utf8.Valid(raw) {
		return nil, errors.New("empty or non-UTF-8 JSON")
	}

	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var walk func(int) error
	walk = func(depth int) error {
		if depth > maxNormalizedRequestJSONDepth {
			return errors.New("normalized request JSON nesting exceeds limit")
		}
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delimiter, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delimiter {
		case '{':
			seen := make(map[string]struct{})
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return err
				}
				key, ok := keyToken.(string)
				if !ok {
					return errors.New("JSON object key must be a string")
				}
				if _, duplicate := seen[key]; duplicate {
					return fmt.Errorf("duplicate normalized request JSON key %q", key)
				}
				seen[key] = struct{}{}
				if err := walk(depth + 1); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim('}') {
				return errors.New("unterminated JSON object")
			}
			return nil
		case '[':
			for decoder.More() {
				if err := walk(depth + 1); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim(']') {
				return errors.New("unterminated JSON array")
			}
			return nil
		default:
			return fmt.Errorf("unexpected JSON delimiter %q", delimiter)
		}
	}

	if err := walk(0); err != nil {
		return nil, fmt.Errorf("invalid normalized request JSON: %w", err)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("normalized request contains trailing JSON data")
	}

	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil || object == nil {
		return nil, errors.New("normalized request must be a JSON object")
	}
	return object, nil
}
