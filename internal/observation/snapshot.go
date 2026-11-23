package observation

// Snapshot is an authenticated, scoped batch of infrastructure facts. A
// Reader chooses its data mode from trusted server configuration, never from
// a browser request.
type Snapshot struct {
	DataMode Mode     `json:"data_mode"`
	Items    []Record `json:"items"`
}
