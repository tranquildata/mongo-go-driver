package driver

import (
	"context"

	"go.mongodb.org/mongo-driver/x/mongo/driver/wiremessage"
)

type TranquilMongoConnection interface {
	WriteWireMessage(context.Context, []byte) error
	//ReadWireMessage returns the header, the body, the whole message,  and/or any errors
	ReadWireMessage(context.Context, []byte) (*wiremessage.MsgHeader, []byte, []byte, error)
	Close()
}
