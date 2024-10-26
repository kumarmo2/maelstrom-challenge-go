package lib

type SendRequestBody struct {
	Variant string `json:"type"`
	Key     string `json:"key"`
	Msg     any    `json:"msg"`
}

type SendResponseBody struct {
	Variant string `json:"type"`
	Offset  int    `json:"offset"`
}

type LogSyncAck struct {
	Key      string `json:"key"`
	Offset   int    `json:"offset"`
	Follower string `json:"follower"`
}

type CustomMessage[T any] struct {
	Type string `json:"type"`
	Body T      `json:"body"`
}
