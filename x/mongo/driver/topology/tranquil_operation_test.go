/*
 * Copyright (c) 2020, Tranquil Data, Inc. All rights reserved.
 */

package topology

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"go.mongodb.org/mongo-driver/x/mongo/driver"
	"go.mongodb.org/mongo-driver/x/mongo/driver/address"
	"go.mongodb.org/mongo-driver/x/mongo/driver/auth"
	"go.mongodb.org/mongo-driver/x/mongo/driver/description"
	"go.mongodb.org/mongo-driver/x/mongo/driver/operation"
)

type testConnection struct {
	writeBuffer *bytes.Buffer
	readBuffer  *bytes.Buffer
}

func (tc *testConnection) WriteWireMessage(ctx context.Context, bytez []byte) error {
	tc.writeBuffer.Write(bytez)
	return nil
}

func (tc *testConnection) ReadWireMessage(ctx context.Context, dst []byte) ([]byte, error) {
	if tc.readBuffer.Len() > len(dst) {
		dst = make([]byte, tc.readBuffer.Len())
	}
	n, err := tc.readBuffer.Read(dst)
	if n <= 0 {
		return nil, fmt.Errorf("No bytes read")
	}
	return dst, err
}

func (tc *testConnection) Description() description.Server {
	return description.Server{}
}

func (tc *testConnection) Close() error {
	return nil
}

func (tc *testConnection) ID() string {
	return ""
}

func (tc *testConnection) Address() address.Address {
	return ""
}

func (tc *testConnection) flip() {
	tmp := tc.writeBuffer
	tc.writeBuffer = tc.readBuffer
	tc.readBuffer = tmp
}

func newTestConnection() *testConnection {
	return &testConnection{
		writeBuffer: &bytes.Buffer{},
		readBuffer:  &bytes.Buffer{},
	}
}

func Test_thatOpsAreWritenAndRead(t *testing.T) {
	testConn := newTestConnection()
	tqConn := &Tranquilconn{testConn}
	ctx := context.Background()
	authThing, err := auth.CreateAuthenticator("PLAIN",
		&auth.Cred{
			Username: "test",
			Password: "test",
		})
	if err != nil {
		t.Errorf("Auth error: %s", err.Error())
	}
	opts := &auth.HandshakeOptions{
		AppName:       "test",
		Authenticator: authThing,
		DBUser:        "test",
	}
	_, err = auth.Handshaker(nil, opts).GetDescription(ctx, "", testConn)
	if err == nil {
		t.Errorf("Expected insufficient bytes error")
	}
	testConn.flip()

	op := &driver.Operation{}
	messageBytes := []byte{}
	messageBytes, err = op.ReadWireMessageDirect(ctx, tqConn, messageBytes)
	if err != nil {
		t.Errorf("ReadWireMessage Error: %s", err.Error())
	}
	isMasterRequest, err := operation.FromHandshake(messageBytes)
	if err != nil {
		t.Error(err)
	}
	if isMasterRequest == nil {
		t.Errorf("Nil request")
	}
	if isMasterRequest.DriverName != "mongo-go-driver" {
		t.Errorf("wrong driver name")
	}
	if isMasterRequest.DriverVersion != "v1.4.0+prerelease" {
		t.Errorf("wrong driver version")
	}
	if isMasterRequest.Appname != "test" {
		t.Errorf("Wrong app name: %s", isMasterRequest.Appname)
	}
}
