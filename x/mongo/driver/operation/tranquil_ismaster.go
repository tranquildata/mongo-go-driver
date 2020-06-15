package operation

import (
	"bytes"
	"fmt"

	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
)

func readStringElement(doc []byte, key string) (string, []byte, error) {
	bsonTy, bsonKey, rem, ok := bsoncore.ReadHeader(doc)
	if !ok || bsonKey != key || bsonTy != bsontype.String {
		return "", doc, fmt.Errorf("Error decoding string header")
	}
	str, rem, ok := bsoncore.ReadString(rem)
	if !ok {
		return "", doc, fmt.Errorf("Error reading string")
	}
	return str, rem, nil
}

func readDocumentElement(doc []byte, key string) ([]byte, []byte, error) {
	bsonTy, bsonKey, rem, ok := bsoncore.ReadHeader(doc)
	if !ok || bsonKey != key || bsonTy != bsontype.EmbeddedDocument {
		return nil, doc, fmt.Errorf("Error decoding document header")
	}
	readDoc, rem, ok := bsoncore.ReadDocument(rem)
	if !ok {
		return nil, doc, fmt.Errorf("Error decoding document")
	}
	return readDoc, rem, nil
}

func readArrayElementStart(doc []byte, key string) (int32, []byte, error) {
	bsonTy, bsonKey, rem, ok := bsoncore.ReadHeader(doc)
	if !ok || bsonKey != key || bsonTy != bsontype.Array {
		return -1, doc, fmt.Errorf("Error decoding array start header")
	}
	arrLen, rem, ok := bsoncore.ReadInt32(rem)
	if !ok {
		return -1, doc, fmt.Errorf("Error decoding array length")
	}
	return arrLen, rem, nil
}

type IsMasterReq struct {
	Appname            string
	Compressors        []string
	SaslSupportedMechs string
	SpeculativeAuth    bsoncore.Document
	DriverName         string //req
	DriverVersion      string //req
	OsType             string //req
	OsName             string
	OsArch             string
	OsVersion          string
	Platform           string
}

type LastWriteResponse struct {
	LastWriteDate int64 `bson:"lastWriteDate"`
}

type TopVersion struct {
	ProcessID primitive.ObjectID `bson:"processId"`
	Counter   int64              `bson:"counter"`
}

type IsMasterResponse struct {
	Arbiters                     []string
	ArbiterOnly                  bool `bson:"arbiterOnly"`
	Compression                  []string
	ElectionID                   primitive.ObjectID `bson:"electionId"`
	Hidden                       bool
	Hosts                        []string
	IsMaster                     bool
	IsReplicaSet                 bool
	LastWrite                    *LastWriteResponse
	LogicalSessionTimeoutMinutes int64  `bson:"logicalSessionTimeoutMinutes"`
	MaxBsonObjectSize            int64  `bson:"maxBsonObjectSize"`
	MaxMessageSizeBytes          int64  `bson:"maxMessageSizeBytes"`
	MaxWriteBatchSize            int64  `bson:"maxWriteBatchSize"`
	Me                           string //address
	MaxWireVersion               int32  `bson:"maxWireVersion"`
	MinWireVersion               int32  `bson:"minWireVersion"`
	Msg                          string
	Ok                           int32
	Passives                     []string
	ReadOnly                     bool
	SaslSupportedMechs           []string `bson:"saslSupportedMechs"`
	Secondary                    bool
	SetName                      string            `bson:"setName"`
	SetVersion                   int64             `bson:"setVersion"`
	SpeculativeAuthenticate      bsoncore.Document `bson:"speculativeAuthenticate"`
	Tags                         map[string]string
	TopVersion                   *TopVersion
	Localtime                    int64
}

func lookupAsType(doc bsoncore.Document, key string, expectedType bsontype.Type) (bsoncore.Value, error) {
	if val, err := doc.LookupErr(key); err == nil {
		if val.Type != expectedType {
			return bsoncore.Value{}, fmt.Errorf("Type error. Expected: %v, got: %v", expectedType, val.Type)
		}
		return val, nil
	} else {
		return bsoncore.Value{}, err
	}
}

func FromHandshake(docBytes []byte) (*IsMasterReq, error) {
	req := &IsMasterReq{}
	doc, err := bsoncore.NewDocumentFromReader(bytes.NewReader(docBytes))
	if err != nil {
		return nil, err
	}
	val, err := doc.LookupErr("isMaster")
	if err != nil {
		return nil, err
	}
	if val.Type != bsontype.Int32 || val.AsInt32() != 1 {
		return nil, fmt.Errorf("Invalid value found for isMaster: %v", val)
	}
	if val, err = lookupAsType(doc, "saslSupportedMechs", bsontype.String); err == nil {
		req.SaslSupportedMechs = val.StringValue()
	}
	if val, err = lookupAsType(doc, "speculativeAuthenticate", bsontype.EmbeddedDocument); err == nil {
		req.SpeculativeAuth = val.Document()
	}
	val, err = lookupAsType(doc, "compression", bsontype.Array)
	if err != nil {
		return nil, err
	}
	vals, err := val.Array().Values()
	if err != nil {
		return nil, err
	}
	comps := make([]string, len(vals))
	for idx, arrayVal := range vals {
		comps[idx] = arrayVal.StringValue()
	}
	req.Compressors = comps
	val, err = lookupAsType(doc, "client", bsontype.EmbeddedDocument)
	if err != nil {
		return nil, err
	}
	clientMetadata := val.Document()
	return parseClientMetadata(clientMetadata, req)
}

func parseClientMetadata(clientMeta bsoncore.Document, hsReq *IsMasterReq) (*IsMasterReq, error) {
	val, err := lookupAsType(clientMeta, "application", bsontype.EmbeddedDocument)
	if err == nil {
		if nm, e := lookupAsType(val.Document(), "name", bsontype.String); e == nil {
			hsReq.Appname = nm.StringValue()
		} else {
			return nil, fmt.Errorf("Missing name in application embedded doc")
		}
	}
	if val, err = lookupAsType(clientMeta, "driver", bsontype.EmbeddedDocument); err != nil {
		return nil, fmt.Errorf("Missing driver document")
	}
	driverDoc := val.Document()
	if val, err = lookupAsType(driverDoc, "name", bsontype.String); err != nil {
		return nil, fmt.Errorf("Missing driver name")
	}
	hsReq.DriverName = val.StringValue()
	if val, err = lookupAsType(driverDoc, "version", bsontype.String); err != nil {
		return nil, fmt.Errorf("Missing driver version")
	}
	hsReq.DriverVersion = val.StringValue()
	if val, err = lookupAsType(clientMeta, "os", bsontype.EmbeddedDocument); err != nil {
		return nil, fmt.Errorf("Missing os document")
	}
	osDoc := val.Document()
	if val, err = lookupAsType(osDoc, "type", bsontype.String); err != nil {
		return nil, fmt.Errorf("Missing os type")
	}
	hsReq.OsType = val.StringValue()
	if val, err = lookupAsType(osDoc, "architecture", bsontype.String); err == nil {
		hsReq.OsArch = val.StringValue()
	}
	if val, err = lookupAsType(osDoc, "name", bsontype.String); err == nil {
		hsReq.OsName = val.StringValue()
	}
	if val, err = lookupAsType(osDoc, "version", bsontype.String); err == nil {
		hsReq.OsVersion = val.StringValue()
	}
	if val, err = lookupAsType(clientMeta, "platform", bsontype.String); err == nil {
		hsReq.Platform = val.StringValue()
	}
	return hsReq, nil
}
