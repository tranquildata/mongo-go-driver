package driver

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/x/mongo/driver/description"
	"go.mongodb.org/mongo-driver/x/mongo/driver/wiremessage"
)

func (op Operation) CreateWireMessageDirect(ctx context.Context, dst []byte, desc description.SelectedServer, conn Connection) ([]byte, startedInformation, error) {
	return op.createWireMessage(ctx, dst, desc, conn)
}

func (op Operation) ReadWireMessageDirect(ctx context.Context, wm []byte) ([]byte, error) {
	var err error

	// decode
	res, err := op.decodeWireMessage(wm)
	if err != nil {
		return res, err
	}

	// If there is no error, automatically attempt to decrypt all results if client side encryption is enabled.
	if op.Crypt != nil {
		return op.Crypt.Decrypt(ctx, res)
	}
	return res, nil
}

func (op Operation) decodeReply(wm []byte) (bsoncore.Document, error) {
	reply := op.decodeOpReply(wm, false)
	if reply.err != nil {
		return nil, reply.err
	}
	if reply.numReturned == 0 {
		return nil, ErrNoDocCommandResponse
	}
	if reply.numReturned > 1 {
		return nil, ErrMultiDocCommandResponse
	}
	rdr := reply.documents[0]
	if err := rdr.Validate(); err != nil {
		return nil, NewCommandResponseError("malformed OP_REPLY: invalid document", err)
	}

	return rdr, extractError(rdr)
}

func (op Operation) decodeOpMsg(wm []byte) (bsoncore.Document, error) {
	var ok bool
	_, wm, ok = wiremessage.ReadMsgFlags(wm)
	if !ok {
		return nil, errors.New("malformed wire message: missing OP_MSG flags")
	}

	var res bsoncore.Document
	for len(wm) > 0 {
		var stype wiremessage.SectionType
		stype, wm, ok = wiremessage.ReadMsgSectionType(wm)
		if !ok {
			return nil, errors.New("malformed wire message: insuffienct bytes to read section type")
		}

		switch stype {
		case wiremessage.SingleDocument:
			res, wm, ok = wiremessage.ReadMsgSectionSingleDocument(wm)
			if !ok {
				return nil, errors.New("malformed wire message: insufficient bytes to read single document")
			}
		case wiremessage.DocumentSequence:
			// TODO(GODRIVER-617): Implement document sequence returns.
			_, _, wm, ok = wiremessage.ReadMsgSectionDocumentSequence(wm)
			if !ok {
				return nil, errors.New("malformed wire message: insufficient bytes to read document sequence")
			}
		default:
			return nil, fmt.Errorf("malformed wire message: uknown section type %v", stype)
		}
	}

	err := res.Validate()
	if err != nil {
		return nil, NewCommandResponseError("malformed OP_MSG: invalid document", err)
	}

	return res, nil
}

func (op Operation) decodeWireMessage(wm []byte) (bsoncore.Document, error) {
	wmLength := len(wm)
	length, _, _, opcode, wm, ok := wiremessage.ReadHeader(wm)
	if !ok || int(length) > wmLength {
		return nil, errors.New("malformed wire message: insufficient bytes")
	}

	wm = wm[:wmLength-16] // constrain to just this wiremessage, incase there are multiple in the slice

	switch opcode {
	case wiremessage.OpReply:
		return op.decodeReply(wm)
	case wiremessage.OpMsg:
		return op.decodeOpMsg(wm)
	case wiremessage.OpQuery:
		return op.decodeOpQuery(wm)
	default:
		return nil, fmt.Errorf("cannot decode result from %s", opcode)
	}
}

func (op Operation) decodeOpQuery(wm []byte) (bsoncore.Document, error) {
	//TODO: figure out what to do with all this query metadata
	_, wm, ok := wiremessage.ReadQueryFlags(wm)
	if !ok {
		return nil, errors.New("could not read flags")
	}
	_, wm, ok = wiremessage.ReadQueryFullCollectionName(wm)
	if !ok {
		return nil, errors.New("could not read fullCollectionName")
	}
	_, wm, ok = wiremessage.ReadQueryNumberToSkip(wm)
	if !ok {
		return nil, errors.New("could not read numberToSkip")
	}
	_, wm, ok = wiremessage.ReadQueryNumberToReturn(wm)
	if !ok {
		return nil, errors.New("could not read numberToReturn")
	}

	var query bsoncore.Document
	query, wm, ok = wiremessage.ReadQueryQuery(wm)
	if !ok {
		return nil, errors.New("could not read query")
	}
	return query, nil
}
