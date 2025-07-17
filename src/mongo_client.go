package main

import (
	"context"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

var (
	clientInstance    *mongo.Client
	clientInstanceErr error
	mongoOnce         sync.Once
)

func GetMongoClient(ctx context.Context) (*mongo.Client, error) {
	mongoOnce.Do(func() {
		cfg, err := GetConfig()
		if err != nil {
			panic(err)
		}
		uri := cfg.OutputMongoURI
		if uri == "" {
			uri = "mongodb://localhost:27017/?directConnection=true"
		}
		_, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		clientInstance, clientInstanceErr = mongo.Connect(options.Client().ApplyURI(uri))
	})
	return clientInstance, clientInstanceErr
}

func DisconnectMongoClient() error {
	if clientInstance == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return clientInstance.Disconnect(ctx)
}

var ErrMissingMongoURI = mongoConfigurationError("missing MONGODB_URI environment variable")

type mongoConfigurationError string

func (e mongoConfigurationError) Error() string {
	return string(e)
}

func CreateIndex(ctx context.Context, coll *mongo.Collection, keys bson.D) error {
	ixName, err := coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: keys,
	})
	if err != nil {
		return err
	}
	Logger.
		WithField("coll", coll.Name()).
		WithField("ixName", ixName).
		Info("Index created in the 'slowQueries' collection")

	return nil
}

func CreateIndexes(ctx context.Context, dbName string) error {
	client, err := GetMongoClient(ctx)
	if err != nil {
		return err
	}
	indexCtx, indexCancel := context.WithTimeout(ctx, 5*time.Second)
	defer indexCancel()
	slowQueriesColl := client.Database(dbName).Collection("slowQueries")
	clientMetadataColl := client.Database(dbName).Collection("clientMetadata")
	slowQueriesByDriverColl := client.Database(dbName).Collection("slowQueriesByDriver")
	err = CreateIndex(indexCtx, slowQueriesColl, bson.D{
		{"attr.queryHash", 1},
		{"attr.durationMillis", -1},
	})
	if err != nil {
		return err
	}
	err = CreateIndex(indexCtx, clientMetadataColl, bson.D{
		{"ctxhost", 1},
		{"attr.doc.driver", 1},
	})
	if err != nil {
		return err
	}
	err = CreateIndex(indexCtx, slowQueriesByDriverColl, bson.D{
		{"totalDurationMillis", -1},
	})
	if err != nil {
		return err
	}
	return nil
}

func InsertSlowQueriesBatch(ctx context.Context, docs []interface{}, dbName string) (*mongo.InsertManyResult, error) {
	client, err := GetMongoClient(ctx)
	if err != nil {
		return nil, err
	}
	collection := client.Database(dbName).Collection("slowQueries")
	return collection.InsertMany(ctx, docs)
}

func InsertPrimaryChangeEventBatch(ctx context.Context, docs []interface{}, dbName string) (*mongo.InsertManyResult, error) {
	client, err := GetMongoClient(ctx)
	if err != nil {
		return nil, err
	}
	collection := client.Database(dbName).Collection("primaryChangeEvents")
	return collection.InsertMany(ctx, docs)
}

func InsertClientMetadataBatch(ctx context.Context, docs []interface{}, dbName string) (*mongo.InsertManyResult, error) {
	client, err := GetMongoClient(ctx)
	if err != nil {
		return nil, err
	}
	collection := client.Database(dbName).Collection("clientMetadata")
	return collection.InsertMany(ctx, docs)
}

func CreateSlowQueriesByDriver(ctx context.Context, dbName string) error {
	client, err := GetMongoClient(ctx)
	if err != nil {
		return err
	}
	collection := client.Database(dbName).Collection("slowQueries")
	lookupStage := bson.D{
		{"$lookup", bson.D{
			{"from", "clientMetadata"},
			{"localField", "ctxhost"},
			{"foreignField", "ctxhost"},
			{"as", "driver"},
			{"pipeline", bson.A{
				bson.D{
					{"$project", bson.D{
						{"_id", 0},
						{"driver", bson.D{
							{"$concat", bson.A{
								"$attr.doc.driver.name",
								":",
								"$attr.doc.driver.version",
							}},
						}},
					}},
				},
			}},
		}}}
	unwind := bson.D{
		{"$unwind", "$driver"},
	}
	addFields := bson.D{
		{"$addFields", bson.D{
			{"queryCommand", bson.D{
				{"$ifNull", bson.A{
					"$attr.originatingCommand",
					"$attr.command",
				}},
			}},
			{"isCollscan", bson.D{
				{"$cond", bson.D{
					{"if", bson.D{
						{"$eq", bson.A{
							"$attr.planSummary", "COLLSCAN",
						}},
					}},
					{"then", true},
					{"else", false},
				}},
			}},
		}},
	}
	group := bson.D{
		{"$group", bson.D{
			{"_id", bson.D{
				{"driver", "$driver.driver"},
				{"hash", "$attr.queryHash"},
				{"isCollscan", "$isCollscan"},
			}},
			{"count", bson.D{{"$sum", 1}}},
			{"totalBytesRead", bson.D{{"$sum", "$attr.storage.data.bytesRead"}}},
			{"totalBytesWritten", bson.D{{"$sum", "$attr.storage.data.bytesWritten"}}},
			{"totalDurationMillis", bson.D{{"$sum", "$attr.durationMillis"}}},
			{"totalNumYields", bson.D{{"$sum", "$attr.numYields"}}},
			{"maxBytesRead", bson.D{{"$max", "$attr.storage.data.bytesRead"}}},
			{"maxWritten", bson.D{{"$max", "$attr.storage.data.bytesWritten"}}},
			{"maxDurationMillis", bson.D{{"$max", "$attr.durationMillis"}}},
			{"maxNumYields", bson.D{{"$max", "$attr.numYields"}}},
			{"avgBytesRead", bson.D{{"$avg", "$attr.storage.data.bytesRead"}}},
			{"avgWritten", bson.D{{"$avg", "$attr.storage.data.bytesWritten"}}},
			{"avgDurationMillis", bson.D{{"$avg", "$attr.durationMillis"}}},
			{"avgNumYields", bson.D{{"$avg", "$attr.numYields"}}},
			{"queryExample", bson.D{{"$first", "$$ROOT"}}},
		}},
	}
	out := bson.D{
		{"$out", bson.D{
			{"db", dbName},
			{"coll", "slowQueriesByDriver"},
		}},
	}
	pipeline := mongo.Pipeline{
		lookupStage,
		unwind,
		addFields,
		group,
		out,
	}

	_, err = collection.Aggregate(ctx, pipeline)
	if err != nil {
		Logger.Error(err)
	}
	return err
}

func GetTopQueryShapesByExecutionTime(ctx context.Context, dbName string, topN int) ([]SlowQueryByDriver, error) {
	client, err := GetMongoClient(ctx)
	if err != nil {
		return nil, err
	}
	collection := client.Database(dbName).Collection("slowQueriesByDriver")
	sort := bson.D{
		{"$sort", bson.D{
			{"totalDurationMillis", -1},
		}},
	}

	limit := bson.D{
		{"$limit", topN},
	}
	pipeline := mongo.Pipeline{
		sort,
		limit,
	}

	res, err := collection.Aggregate(ctx, pipeline)
	if err != nil {
		Logger.Error(err)
	}

	var docs []SlowQueryByDriver
	err = res.All(ctx, &docs)
	if err != nil {
		Logger.Error(err)
		return nil, err
	}
	return docs, nil
}

func GetSlowestQueryByShape(ctx context.Context, dbName string, queryHash string, driver string) (SlowQueryEntry, error) {
	Logger.WithFields(logrus.Fields{"queryHash": queryHash}).Info("Fetching the slowest query for query hash")
	client, err := GetMongoClient(ctx)
	if err != nil {
		Logger.Error(err)
		panic(err)
	}
	collection := client.Database(dbName).Collection("slowQueries")
	match := bson.D{
		{"$match", bson.D{
			{"attr.queryHash", queryHash},
		}},
	}
	sort := bson.D{
		{"$sort", bson.D{
			{"attr.durationMillis", -1},
		}},
	}

	limit := bson.D{
		{"$limit", 1},
	}
	pipeline := mongo.Pipeline{
		match,
		sort,
		limit,
	}
	res, err := collection.Aggregate(ctx, pipeline)
	if err != nil {
		Logger.Error(err)
	}

	var docs []SlowQueryEntry
	err = res.All(ctx, &docs)
	if err != nil {
		Logger.Error(err)
		panic(err)
	}
	if len(docs) == 1 {
		doc := docs[0]
		doc.Driver = driver
		return doc, nil
	}
	panic("Query hash not found")
}
