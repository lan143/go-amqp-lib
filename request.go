package go_amqp_lib

import (
	"github.com/google/uuid"
	"time"
)

type Request[T any] struct {
	Id        uuid.UUID `json:"id"`
	CreatedAt time.Time `json:"createdAt"`
	ExpiresAt time.Time `json:"expiresAt"`
	Payload   T         `json:"payload"`
}
