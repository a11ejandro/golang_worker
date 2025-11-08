package main

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// parseInt64 extracts an int64 from a Sidekiq payload argument that may be encoded
// either as a JSON number or as a quoted string.
func parseInt64(raw json.RawMessage) (int64, error) {
	var asNumber int64
	if err := json.Unmarshal(raw, &asNumber); err == nil {
		return asNumber, nil
	}

	var asString string
	if err := json.Unmarshal(raw, &asString); err == nil {
		if asString == "" {
			return 0, fmt.Errorf("empty string")
		}
		v, err := strconv.ParseInt(asString, 10, 64)
		if err != nil {
			return 0, err
		}
		return v, nil
	}

	return 0, fmt.Errorf("unsupported arg: %s", string(raw))
}
