package kinesumer

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/guregu/dynamo"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func newTestDynamoDB(t *testing.T) *dynamo.DB {
	awsCfg := aws.NewConfig()
	awsCfg.WithRegion("ap-northeast-2")
	awsCfg.WithEndpoint("http://localhost:14566")
	sess, err := session.NewSession(awsCfg)
	if err != nil {
		t.Fatal("failed to init test env:", err.Error())
	}
	return dynamo.New(sess)
}

func cleanUpStateStore(t *testing.T, store *stateStore) {
	type PkSk struct {
		PK string `dynamo:"pk"`
		SK string `dynamo:"sk"`
	}

	var (
		pksks []*PkSk
		keys  []dynamo.Keyed
	)
	table := store.db.table
	if err := table.Scan().All(&pksks); err != nil {
		t.Fatal("failed to scan the state table:", err.Error())
	}
	for _, pksk := range pksks {
		keys = append(keys, &dynamo.Keys{pksk.PK, pksk.SK})
	}
	if _, err := table.
		Batch("pk", "sk").
		Write().
		Delete(keys...).
		Run(); err != nil {
		t.Fatal("failed to delete all test data:", err.Error())
	}
}

func TestStateStore_UpdateCheckPointsWorksFine(t *testing.T) {
	cfg := &Config{
		App:              "test",
		DynamoDBRegion:   "ap-northeast-2",
		DynamoDBTable:    "kinesumer-state-store",
		DynamoDBEndpoint: "http://localhost:14566",
	}
	store, err := newStateStore(cfg)
	assert.NoError(t, err, "there should be no error")

	s, _ := store.(*stateStore)
	defer cleanUpStateStore(t, s)

	expectedUpdatedAt := time.Date(2022, 7, 12, 12, 35, 0, 0, time.UTC)

	expected := []*stateCheckPoint{
		{
			StreamKey:      buildCheckPointKey("test", "foobar"),
			ShardID:        "shardId-000",
			SequenceNumber: "0",
			LastUpdate:     expectedUpdatedAt,
		},
	}

	err = s.UpdateCheckPoints(
		context.Background(),
		[]*ShardCheckPoint{
			{
				Stream:         "foobar",
				ShardID:        "shardId-000",
				SequenceNumber: "0",
				UpdatedAt:      expectedUpdatedAt,
			},
		},
	)
	if assert.NoError(t, err, "there should be no error") {
		assert.Eventually(
			t,
			func() bool {
				var result []*stateCheckPoint
				err := s.db.table.
					Batch("pk", "sk").
					Get(
						[]dynamo.Keyed{
							dynamo.Keys{buildCheckPointKey("test", "foobar"), "shardId-000"},
							dynamo.Keys{buildCheckPointKey("test", "foo"), "shardId-001"},
						}...,
					).All(&result)
				if assert.NoError(t, err) {
					return assert.EqualValues(t, expected, result)
				}
				return false
			},
			600*time.Millisecond,
			100*time.Millisecond,
			"they should be equal",
		)
	}
}
