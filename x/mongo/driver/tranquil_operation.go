package driver

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/x/mongo/driver/description"
	"go.mongodb.org/mongo-driver/x/mongo/driver/wiremessage"
)

type execSetup struct {
	srvr         Server
	conn         Connection
	desc         *description.SelectedServer
	batching     bool
	retryable    bool
	retryEnabled bool
	retries      int
}

func (es *execSetup) Close() {
	if es.conn != nil {
		es.conn.Close()
	}
}

func (op Operation) CreateWireMessageDirect(ctx context.Context, dst []byte, desc description.SelectedServer, conn Connection) ([]byte, startedInformation, error) {
	return op.createWireMessage(ctx, dst, desc, conn)
}

func (Operation) decompressBody(wm []byte) ([]byte, error) {
	// read the header and ensure this is a compressed wire message
	length, reqid, respto, opcode, rem, ok := wiremessage.ReadHeader(wm)
	if !ok || len(wm) < int(length) {
		return nil, errors.New("malformed wire message: insufficient bytes")
	}
	if opcode != wiremessage.OpCompressed {
		return rem, nil
	}
	// get the original opcode and uncompressed size
	opcode, rem, ok = wiremessage.ReadCompressedOriginalOpCode(rem)
	if !ok {
		return nil, errors.New("malformed OP_COMPRESSED: missing original opcode")
	}
	uncompressedSize, rem, ok := wiremessage.ReadCompressedUncompressedSize(rem)
	if !ok {
		return nil, errors.New("malformed OP_COMPRESSED: missing uncompressed size")
	}
	// get the compressor ID and decompress the message
	compressorID, rem, ok := wiremessage.ReadCompressedCompressorID(rem)
	if !ok {
		return nil, errors.New("malformed OP_COMPRESSED: missing compressor ID")
	}
	compressedSize := length - 25 // header (16) + original opcode (4) + uncompressed size (4) + compressor ID (1)
	// return the original wiremessage
	msg, rem, ok := wiremessage.ReadCompressedCompressedMessage(rem, compressedSize)
	if !ok {
		return nil, errors.New("malformed OP_COMPRESSED: insufficient bytes for compressed wiremessage")
	}

	header := make([]byte, 0, uncompressedSize+16)
	header = wiremessage.AppendHeader(header, uncompressedSize+16, reqid, respto, opcode)
	opts := CompressionOpts{
		Compressor:       compressorID,
		UncompressedSize: uncompressedSize,
	}
	uncompressed, err := DecompressPayload(msg, opts)
	if err != nil {
		return nil, err
	}

	return uncompressed, nil
}

//Reads the whole message and returns the body bytes uncompressed, and the interpreted body bytes
func (op Operation) ReadAndUncompressBodyBytes(ctx context.Context, wholeMsg []byte) ([]byte, bsoncore.Document, error) {
	bodyBytes, err := op.decompressBody(wholeMsg)
	if err != nil {
		return nil, nil, err
	}

	//TODO: figure out how to avoid interpreting every message to find the 1 or 2 terminating messages
	// decode strips off body bytes, and does a lot of work we probably don't need to do
	res, err := op.decodeWireMessage(wholeMsg)
	if err != nil {
		return bodyBytes, res, err
	}

	// If there is no error, automatically attempt to decrypt all results if client side encryption is enabled.
	if op.Crypt != nil {
		res, err = op.Crypt.Decrypt(ctx, res)
	}
	return bodyBytes, res, err

}

func (op Operation) ReadWireMessageDirect(ctx context.Context, wm []byte) ([]byte, error) {
	var err error
	wm, err = op.decompressWireMessage(wm)
	if err != nil {
		return nil, err
	}

	// decode strips off body bytes, and does a lot of work we probably don't need to do
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

//This will strip off flags from the incoming message
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

func (op Operation) DecodeMessageBody(header *wiremessage.MsgHeader, body []byte) (bsoncore.Document, error) {
	return op.decodeWireMessageBody(body, header.Opcode)
}

func (op Operation) decodeWireMessage(wm []byte) (bsoncore.Document, error) {
	wmLength := len(wm)
	length, _, _, opcode, wm, ok := wiremessage.ReadHeader(wm)
	if !ok || int(length) > wmLength {
		return nil, errors.New("malformed wire message: insufficient bytes")
	}

	// constrain to just this wiremessage, incase there are multiple in the slice
	return op.decodeWireMessageBody(wm[:wmLength-16], opcode)
}

func (op Operation) decodeWireMessageBody(body []byte, opcode wiremessage.OpCode) (bsoncore.Document, error) {
	switch opcode {
	case wiremessage.OpReply:
		return op.decodeReply(body)
	case wiremessage.OpMsg:
		return op.decodeOpMsg(body)
	case wiremessage.OpQuery:
		return op.decodeOpQuery(body)
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

func (op Operation) executeValidateAndConfig(ctx context.Context, scratch []byte) (*execSetup, error) {
	err := op.Validate()
	if err != nil {
		return nil, err
	}

	srvr, err := op.selectServer(ctx)
	if err != nil {
		return nil, err
	}

	conn, err := srvr.Connection(ctx)
	if err != nil {
		return nil, err
	}

	desc := description.SelectedServer{Server: conn.Description(), Kind: op.Deployment.Kind()}
	scratch = scratch[:0]

	/* LEGACY HANDLING
	if desc.WireVersion == nil || desc.WireVersion.Max < 4 {
		switch op.Legacy {
		case LegacyFind:
			return op.legacyFind(ctx, scratch, srvr, conn, desc)
		case LegacyGetMore:
			return op.legacyGetMore(ctx, scratch, srvr, conn, desc)
		case LegacyKillCursors:
			return op.legacyKillCursors(ctx, scratch, srvr, conn, desc)
		}
	}
	if desc.WireVersion == nil || desc.WireVersion.Max < 3 {
		switch op.Legacy {
		case LegacyListCollections:
			return op.legacyListCollections(ctx, scratch, srvr, conn, desc)
		case LegacyListIndexes:
			return op.legacyListIndexes(ctx, scratch, srvr, conn, desc)
		}
	}
	*/

	var retries int
	retryable := op.retryable(desc.Server)
	if retryable && op.RetryMode != nil {
		switch op.Type {
		case Write:
			if op.Client == nil {
				break
			}
			switch *op.RetryMode {
			case RetryOnce, RetryOncePerCommand:
				retries = 1
			case RetryContext:
				retries = -1
			}

			op.Client.RetryWrite = false
			if *op.RetryMode > RetryNone {
				op.Client.RetryWrite = true
				if !op.Client.Committing && !op.Client.Aborting {
					op.Client.IncrementTxnNumber()
				}
			}
		case Read:
			switch *op.RetryMode {
			case RetryOnce, RetryOncePerCommand:
				retries = 1
			case RetryContext:
				retries = -1
			}
		}
	}
	batching := op.Batches.Valid()
	retryEnabled := op.RetryMode != nil && op.RetryMode.Enabled()
	return &execSetup{
		srvr:         srvr,
		conn:         conn,
		batching:     batching,
		desc:         &desc,
		retryable:    retryable,
		retryEnabled: retryEnabled,
		retries:      retries,
	}, nil
}

func ReadWireMessageFromConn(ctx context.Context, conn Connection, dst []byte) (hdr *wiremessage.MsgHeader, body []byte, entireMsg []byte, err error) {
	entireMsg, err = conn.ReadWireMessage(ctx, dst)
	if err != nil {
		return nil, nil, nil, err
	}
	var ok bool
	hdr = &wiremessage.MsgHeader{}
	hdr.Length, hdr.RequestID, hdr.ResponseTo, hdr.Opcode, body, ok = wiremessage.ReadHeader(entireMsg)
	if !ok {
		return nil, nil, nil, errors.New("Incomplete header")
	}
	//TODO: decompress and decrypt would go here
	return
}

func (op Operation) roundTripDirect(ctx context.Context, conn Connection, wm []byte) (*wiremessage.MsgHeader, []byte, error) {
	err := conn.WriteWireMessage(ctx, wm)
	if err != nil {
		labels := []string{NetworkError}
		if op.Client != nil {
			op.Client.MarkDirty()
		}
		if op.Client != nil && op.Client.TransactionRunning() && !op.Client.Committing {
			labels = append(labels, TransientTransactionError)
		}
		if op.Client != nil && op.Client.Committing {
			labels = append(labels, UnknownTransactionCommitResult)
		}
		return nil, nil, Error{Message: err.Error(), Labels: labels, Wrapped: err}
	}

	hdr, body, _, err := ReadWireMessageFromConn(ctx, conn, wm)
	return hdr, body, err
}

func (op *Operation) moreToComeRoundTripDirect(ctx context.Context, conn Connection, wm []byte) (*wiremessage.MsgHeader, []byte, error) {
	err := conn.WriteWireMessage(ctx, wm)
	if err != nil {
		if op.Client != nil {
			op.Client.MarkDirty()
		}
		err = Error{Message: err.Error(), Labels: []string{TransientTransactionError, NetworkError}, Wrapped: err}
	}
	return nil, bsoncore.BuildDocument(nil, bsoncore.AppendInt32Element(nil, "ok", 1)), err
}

func (op Operation) processExecuteError(ctx context.Context, err error, operationErr WriteCommandError, setup *execSetup, currIndex int) (error, bool) {
	var original error

	switch tt := err.(type) {
	case WriteCommandError:
		if e := err.(WriteCommandError); setup.retryable && op.Type == Write && e.UnsupportedStorageEngine() {
			return ErrUnsupportedStorageEngine, false
		}

		connDesc := setup.conn.Description()
		retryableErr := tt.Retryable(connDesc.WireVersion)
		preRetryWriteLabelVersion := connDesc.WireVersion != nil && connDesc.WireVersion.Max < 9

		// If retry is enabled, add a RetryableWriteError label for retryable errors from pre-4.4 servers
		if retryableErr && preRetryWriteLabelVersion && setup.retryEnabled {
			tt.Labels = append(tt.Labels, RetryableWriteError)
		}

		if setup.retryable && retryableErr && setup.retries != 0 {
			setup.retries--
			original, err = err, nil
			setup.conn.Close() // Avoid leaking the connection.
			setup.srvr, err = op.selectServer(ctx)
			if err != nil {
				return original, false
			}
			setup.conn, err = setup.srvr.Connection(ctx)
			if err != nil || setup.conn == nil || !op.retryable(setup.conn.Description()) {
				if setup.conn != nil {
					setup.conn.Close()
				}
				return original, false
			}
			if op.Client != nil && op.Client.Committing {
				// Apply majority write concern for retries
				op.Client.UpdateCommitTransactionWriteConcern()
				op.WriteConcern = op.Client.CurrentWc
			}
			return nil, true
		}

		if setup.batching && len(tt.WriteErrors) > 0 && currIndex > 0 {
			for i := range tt.WriteErrors {
				tt.WriteErrors[i].Index += int64(currIndex)
			}
		}

		// If batching is enabled and either ordered is the default (which is true) or
		// explicitly set to true and we have write errors, return the errors.
		if setup.batching && (op.Batches.Ordered == nil || *op.Batches.Ordered == true) && len(tt.WriteErrors) > 0 {
			return tt, false
		}
		if op.Client != nil && op.Client.Committing && tt.WriteConcernError != nil {
			// When running commitTransaction we return WriteConcernErrors as an Error.
			err := Error{
				Name:    tt.WriteConcernError.Name,
				Code:    int32(tt.WriteConcernError.Code),
				Message: tt.WriteConcernError.Message,
				Labels:  tt.Labels,
			}
			// The UnknownTransactionCommitResult label is added to all writeConcernErrors besides unknownReplWriteConcernCode
			// and unsatisfiableWriteConcernCode
			if err.Code != unknownReplWriteConcernCode && err.Code != unsatisfiableWriteConcernCode {
				err.Labels = append(err.Labels, UnknownTransactionCommitResult)
			}
			if retryableErr && setup.retryEnabled {
				err.Labels = append(err.Labels, RetryableWriteError)
			}
			return err, false
		}
		operationErr.WriteConcernError = tt.WriteConcernError
		operationErr.WriteErrors = append(operationErr.WriteErrors, tt.WriteErrors...)
		operationErr.Labels = tt.Labels
	case Error:
		if tt.HasErrorLabel(TransientTransactionError) || tt.HasErrorLabel(UnknownTransactionCommitResult) {
			op.Client.ClearPinnedServer()
		}
		if e := err.(Error); setup.retryable && op.Type == Write && e.UnsupportedStorageEngine() {
			return ErrUnsupportedStorageEngine, false
		}

		connDesc := setup.conn.Description()
		var retryableErr bool
		if op.Type == Write {
			retryableErr = tt.RetryableWrite(connDesc.WireVersion)
			preRetryWriteLabelVersion := connDesc.WireVersion != nil && connDesc.WireVersion.Max < 9
			// If retryWrites is enabled, add a RetryableWriteError label for network errors and retryable errors from pre-4.4 servers
			if setup.retryEnabled && (tt.HasErrorLabel(NetworkError) || (retryableErr && preRetryWriteLabelVersion)) {
				tt.Labels = append(tt.Labels, RetryableWriteError)
			}
		} else {
			retryableErr = tt.RetryableRead()
		}

		if setup.retryable && retryableErr && setup.retries != 0 {
			setup.retries--
			original, err = err, nil
			setup.conn.Close() // Avoid leaking the connection.
			setup.srvr, err = op.selectServer(ctx)
			if err != nil {
				return original, false
			}
			setup.conn, err = setup.srvr.Connection(ctx)
			if err != nil || setup.conn == nil || !op.retryable(setup.conn.Description()) {
				if setup.conn != nil {
					setup.conn.Close()
				}
				return original, false
			}
			if op.Client != nil && op.Client.Committing {
				// Apply majority write concern for retries
				op.Client.UpdateCommitTransactionWriteConcern()
				op.WriteConcern = op.Client.CurrentWc
			}
			return nil, true
		}

		if op.Client != nil && op.Client.Committing && (retryableErr || tt.Code == 50) {
			// If we got a retryable error or MaxTimeMSExpired error, we add UnknownTransactionCommitResult.
			tt.Labels = append(tt.Labels, UnknownTransactionCommitResult)
		}
		return tt, false
	default:
		return nil, false
	}
	return nil, false
}

func (op Operation) createWireMessageFromParts(ctx context.Context, dst []byte, reqHeader *wiremessage.MsgHeader, body bsoncore.Document) ([]byte, startedInformation, error) {
	var info startedInformation
	var wmindex int32
	info.requestID = wiremessage.NextRequestID()
	wmindex, dst = wiremessage.AppendHeaderStart(dst, info.requestID, 0, reqHeader.Opcode)
	dst = append(dst, body...)
	return bsoncore.UpdateLength(dst, wmindex, int32(len(dst[wmindex:]))), info, nil
}

func (op Operation) ExecuteDirect(ctx context.Context, scratch []byte, commandName string, reqHeader *wiremessage.MsgHeader, body bsoncore.Document) (*wiremessage.MsgHeader, []byte, error) {
	var header *wiremessage.MsgHeader
	var res bsoncore.Document
	var operationErr WriteCommandError

	setup, err := op.executeValidateAndConfig(ctx, scratch)
	if err != nil {
		return nil, nil, err
	}
	defer setup.Close()

	currIndex := 0
	for {
		if setup.batching {
			targetBatchSize := setup.desc.MaxDocumentSize
			maxDocSize := setup.desc.MaxDocumentSize
			if op.shouldEncrypt() {
				// For client-side encryption, we want the batch to be split at 2 MiB instead of 16MiB.
				// If there's only one document in the batch, it can be up to 16MiB, so we set target batch size to
				// 2MiB but max document size to 16MiB. This will allow the AdvanceBatch call to create a batch
				// with a single large document.
				targetBatchSize = cryptMaxBsonObjectSize
			}

			err = op.Batches.AdvanceBatch(int(setup.desc.MaxBatchCount), int(targetBatchSize), int(maxDocSize))
			if err != nil {
				// TODO(GODRIVER-982): Should we also be returning operationErr?
				return nil, nil, err
			}
		}

		// convert to wire message
		if len(scratch) > 0 {
			scratch = scratch[:0]
		}
		//CREATE MESSAGE
		var wm []byte
		var startedInfo startedInformation
		wm, startedInfo, err = op.createWireMessageFromParts(ctx, scratch, reqHeader, body)
		if err != nil {
			return nil, nil, err
		}

		// set extra data and send event if possible
		startedInfo.connID = setup.conn.ID()
		startedInfo.cmdName = commandName
		startedInfo.redacted = op.redactCommand(startedInfo.cmdName, body)
		op.publishStartedEvent(ctx, startedInfo)

		// get the moreToCome flag information before we compress
		moreToCome := wiremessage.IsMsgMoreToCome(wm)

		// compress wiremessage if allowed
		if compressor, ok := setup.conn.(Compressor); ok && op.canCompress(startedInfo.cmdName) {
			wm, err = compressor.CompressWireMessage(wm, nil)
			if err != nil {
				return nil, nil, err
			}
		}

		finishedInfo := finishedInformation{
			cmdName:   startedInfo.cmdName,
			requestID: startedInfo.requestID,
			startTime: time.Now(),
			connID:    startedInfo.connID,
			redacted:  startedInfo.redacted,
		}
		//ROUND TRIP
		// roundtrip using either the full roundTripper or a special one for when the moreToCome
		// flag is set
		var roundTrip = op.roundTripDirect
		if moreToCome {
			roundTrip = op.moreToComeRoundTripDirect
		}
		header, res, err = roundTrip(ctx, setup.conn, wm)
		if ep, ok := setup.srvr.(ErrorProcessor); ok {
			ep.ProcessError(err)
		}

		finishedInfo.response = res
		finishedInfo.cmdErr = err
		op.publishFinishedEvent(ctx, finishedInfo)

		if err == nil && moreToCome {
			return nil, nil, ErrUnacknowledgedWrite
		} else {
			finalErr, shouldRetry := op.processExecuteError(ctx, err, operationErr, setup, currIndex)
			if finalErr != nil {
				return nil, nil, finalErr
			}
			if shouldRetry {
				continue
			}
		}

		if setup.batching && len(op.Batches.Documents) > 0 {
			if setup.retryable && op.Client != nil && op.RetryMode != nil {
				if *op.RetryMode > RetryNone {
					op.Client.IncrementTxnNumber()
				}
				if *op.RetryMode == RetryOncePerCommand {
					setup.retries = 1
				}
			}
			currIndex += len(op.Batches.Current)
			op.Batches.ClearBatch()
			continue
		}
		break
	}
	if len(operationErr.WriteErrors) > 0 || operationErr.WriteConcernError != nil {
		return nil, nil, operationErr
	}
	return header, res, err

}
