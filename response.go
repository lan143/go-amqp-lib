package go_amqp_lib

type Response[T any] struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	Payload T      `json:"payload"`
}
