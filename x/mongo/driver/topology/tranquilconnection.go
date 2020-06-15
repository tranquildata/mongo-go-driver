package topology

import (
	"context"
	"net"
	"sync/atomic"

	"go.mongodb.org/mongo-driver/x/mongo/driver"
	"go.mongodb.org/mongo-driver/x/mongo/driver/address"
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

func (tqConn *Tranquilconn) ReadWireMessage(ctx context.Context, dst []byte) ([]byte, error) {
	return tqConn.mongoconn.ReadWireMessage(ctx, dst)
}

func (tqConn *Tranquilconn) Close() {
	//not needed as this closes the pool, which we shouldn't be using anyway
	//tqConn.mongoconn.Close()
}
