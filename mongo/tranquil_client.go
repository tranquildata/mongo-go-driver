/**
 * Copyright (c) 2020, Tranquil Data, Inc. All rights reserved.
 */

package mongo

import (
	"go.mongodb.org/mongo-driver/x/mongo/driver/session"
	"go.mongodb.org/mongo-driver/x/mongo/driver/uuid"
)

func (c *Client) GetSessionPool() *session.Pool {
	return c.sessionPool
}

func (c *Client) GetID() uuid.UUID {
	return c.id
}
