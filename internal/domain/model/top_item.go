package model

type TopItem struct {
	Query string `json:"query"`
	Count uint64 `json:"count"`
}
