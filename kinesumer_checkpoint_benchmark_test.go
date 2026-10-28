package kinesumer

import "testing"

func BenchmarkPollingRefreshCheckpointSubmissions(b *testing.B) {
	for _, testCase := range []struct {
		name        string
		streamCount int
	}{
		{name: "one_worker", streamCount: 1},
		{name: "two_streams", streamCount: 2},
	} {
		b.Run(testCase.name, func(b *testing.B) {
			store := newPollingCheckpointStore()
			fixture := newPollingKinesisFixture(b, false)
			k := newPollingKinesumer(b, store, testCase.streamCount, fixture)
			for stream, shards := range k.shards {
				markPollingOffset(k, stream, shards[0].ID, "100")
			}
			k.start()
			fixture.waitForRecords(b, testCase.streamCount)

			if testCase.streamCount == 1 {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					k.Refresh(k.streams)
				}
				b.StopTimer()
			} else {
				var calls, rows int
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					beforeCalls, beforeRows := store.stats()
					k.Refresh(k.streams)
					afterCalls, afterRows := store.stats()
					calls += afterCalls - beforeCalls
					rows += afterRows - beforeRows
				}
				b.StopTimer()
				b.ReportMetric(float64(calls)/float64(b.N), "UpdateCheckPoints/op")
				b.ReportMetric(float64(rows)/float64(b.N), "CheckpointRows/op")
			}
			k.pause()
		})
	}
}
