package main

import "go.mongodb.org/mongo-driver/v2/bson"

type SlowQueryByID struct {
	Driver     string `bson:"driver" json:"driver"`
	Hash       string `bson:"hash" json:"hash"`
	IsCollscan bool   `bson:"isCollscan" json:"isCollscan"`
}

type SlowQueryByDriver struct {
	ID                  SlowQueryByID `bson:"_id" json:"_id"`
	Count               int32         `bson:"count" json:"count"`
	TotalBytesRead      int64         `bson:"totalBytesRead" json:"totalBytesRead"`
	TotalBytesWritten   int64         `bson:"totalBytesWritten" json:"totalBytesWritten"`
	TotalDurationMillis int32         `bson:"totalDurationMillis" json:"totalDurationMillis"`
	TotalNumYields      int32         `bson:"totalNumYields" json:"totalNumYields"`
	MaxBytesRead        int64         `bson:"maxBytesRead" json:"maxBytesRead"`
	MaxWritten          *int64        `bson:"maxWritten" json:"maxWritten"` // Pointer to int64 to represent null
	MaxDurationMillis   int32         `bson:"maxDurationMillis" json:"maxDurationMillis"`
	MaxNumYields        int32         `bson:"maxNumYields" json:"maxNumYields"`
	AvgBytesRead        float64       `bson:"avgBytesRead" json:"avgBytesRead"`
	AvgWritten          float64       `bson:"avgWritten" json:"avgWritten"` // Pointer to int64 to represent null
	AvgDurationMillis   float64       `bson:"avgDurationMillis" json:"avgDurationMillis"`
	AvgNumYields        float64       `bson:"avgNumYields" json:"avgNumYields"`
	QueryExample        bson.M        `bson:"queryExample" json:"queryExample"` // bson.M to represent a generic BSON/JSON object, not omitted
}

type SlowQueryEntry struct {
	ID      bson.ObjectID `bson:"_id" json:"_id"`
	S       string        `bson:"s" json:"s"`
	C       string        `bson:"c" json:"c"`
	LogID   int64         `bson:"id" json:"id"`
	Ctx     string        `bson:"ctx" json:"ctx"`
	Msg     string        `bson:"msg" json:"msg"`
	Attr    bson.M        `bson:"attr" json:"attr"`
	Host    string        `bson:"host" json:"host"`
	CtxHost string        `bson:"ctxHost" json:"ctxHost"`
	Driver  string        `bson:"driver" json:"driver"`
}
