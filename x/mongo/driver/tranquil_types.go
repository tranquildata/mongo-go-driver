package driver

import (
	"context"

	"go.mongodb.org/mongo-driver/x/mongo/driver/wiremessage"
)

type TranquilMongoConnection interface {
	WriteWireMessage(context.Context, []byte) error
	ReadWireMessage(context.Context, []byte) (*wiremessage.MsgHeader, []byte, error)
	Close()
}
