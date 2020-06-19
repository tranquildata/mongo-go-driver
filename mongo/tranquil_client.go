/**
 * Copyright (c) 2020, Tranquil Data, Inc. All rights reserved.
 */

package mongo

import (
	"go.mongodb.org/mongo-driver/x/mongo/driver"
	"go.mongodb.org/mongo-driver/x/mongo/driver/session"
	"go.mongodb.org/mongo-driver/x/mongo/driver/uuid"
)

func (c *Client) GetSessionPool() *session.Pool {
	return c.sessionPool
}

func (c *Client) GetID() uuid.UUID {
	return c.id
}

func (c *Client) GetDeployment() driver.Deployment {
	return c.deployment
}

func (c *Client) PrepareOperation(op *driver.Operation) *driver.Operation {
	op.Clock = c.clock
	op.Deployment = c.deployment
	op.CommandMonitor = c.monitor
	op.ReadConcern = c.readConcern
	op.ReadPreference = c.readPreference
	op.WriteConcern = c.writeConcern
	return op
}
