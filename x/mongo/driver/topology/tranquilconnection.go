package topology

import (
	"context"
	"errors"
	"net"
	"sync/atomic"

	"go.mongodb.org/mongo-driver/x/mongo/driver"
	"go.mongodb.org/mongo-driver/x/mongo/driver/address"
	"go.mongodb.org/mongo-driver/x/mongo/driver/wiremessage"
)

type Tranquilconn struct {
	mongoconn driver.Connection
}

func NewFrontendConnection(ctx context.Context, addr string, conn net.Conn) (driver.TranquilMongoConnection, error) {
	var driverAddr address.Address
	mongoconn, err := newConnection(ctx, driverAddr)
	if err != nil {
		return nil, err
	}
	mongoconn.nc = conn
	atomic.StoreInt32(&mongoconn.connected, connected)
	return &Tranquilconn{
		mongoconn: &Connection{
			connection: mongoconn,
		},
	}, nil
}

//
// Implement TranquilMongoConnection
//
func (tqConn *Tranquilconn) WriteWireMessage(ctx context.Context, msg []byte) error {
	return tqConn.mongoconn.WriteWireMessage(ctx, msg)
}

func (tqConn *Tranquilconn) ReadWireMessage(ctx context.Context, dst []byte) (hdr *wiremessage.MsgHeader, wm []byte, err error) {
	wm, err = tqConn.mongoconn.ReadWireMessage(ctx, dst)
	if err != nil {
		return nil, nil, err
	}
	var ok bool
	hdr = &wiremessage.MsgHeader{}
	hdr.Length, hdr.RequestID, hdr.ResponseTo, hdr.Opcode, _, ok = wiremessage.ReadHeader(wm)
	if !ok {
		return nil, nil, errors.New("Incomplete header")
	}
	return
}

func (tqConn *Tranquilconn) Close() {
	//not needed as this closes the pool, which we shouldn't be using anyway
	//tqConn.mongoconn.Close()
}
