package driver

import (
	"context"
)

func (op Operation) readWireMessageDirect(ctx context.Context, conn TranquilMongoConnection, wm []byte) ([]byte, error) {
	var err error

	wm, err = conn.ReadWireMessage(ctx, wm[:0])
	if err != nil {
		return nil, err
	}

	// decompress wiremessage
	wm, err = op.decompressWireMessage(wm)
	if err != nil {
		return nil, err
	}

	// decode
	res, err := op.decodeResult(wm)
	if err != nil {
		return res, err
	}

	// If there is no error, automatically attempt to decrypt all results if client side encryption is enabled.
	if op.Crypt != nil {
		return op.Crypt.Decrypt(ctx, res)
	}
	return res, nil
}
