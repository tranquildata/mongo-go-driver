package topology

import (
	"context"
	"fmt"
	"net"

	"go.mongodb.org/mongo-driver/x/mongo/driver"
	"go.mongodb.org/mongo-driver/x/mongo/driver/address"
)

type tranquilconn struct {
	mongoconn *connection
}

func NewFrontendConnection(ctx context.Context, addr string, conn net.Conn) (driver.TranquilMongoConnection, error) {
	var driverAddr address.Address
	mongoconn, err := newConnection(ctx, driverAddr)
	if err != nil {
		return nil, err
	}
	mongoconn.nc = conn
	return &tranquilconn{
		mongoconn: mongoconn,
	}, nil
}

//
// Implement TranquilMongoConnection
//
func (tqConn *tranquilconn) WriteWireMessage(context.Context, []byte) error {
	return fmt.Errorf("Not Yet Implemented")
}

func (tqConn *tranquilconn) ReadWireMessage(ctx context.Context, dst []byte) ([]byte, error) {
	return tqConn.mongoconn.readWireMessage(ctx, dst)
}

func (tqConn *tranquilconn) Close() {
	tqConn.mongoconn.close()
}
