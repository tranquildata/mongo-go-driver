package operation

import (
	"go.mongodb.org/mongo-driver/x/mongo/driver"
)

func (i *Insert) ToOperation() *driver.Operation {
	batches := &driver.Batches{
		Identifier: "documents",
		Documents:  i.documents,
		Ordered:    i.ordered,
	}

	return &driver.Operation{
		CommandFn:         i.command,
		ProcessResponseFn: i.processResponse,
		Batches:           batches,
		RetryMode:         i.retry,
		Type:              driver.Write,
		Client:            i.session,
		Clock:             i.clock,
		CommandMonitor:    i.monitor,
		Crypt:             i.crypt,
		Database:          i.database,
		Deployment:        i.deployment,
		Selector:          i.selector,
		WriteConcern:      i.writeConcern,
	}
}
