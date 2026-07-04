// Copyright (C) 2026 Talos, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cluster

import (
	"math"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/counters"
)

// tick drives checkForClockTick until a millisecond boundary is crossed, so
// the body of the tick actually runs.
func tick(agent *ClusteredServiceAgent) {
	for i := 0; i < 100; i++ {
		if agent.checkForClockTick() {
			return
		}
		time.Sleep(time.Millisecond / 4)
	}
}

// The driver reclaims the consensus module's counters when the module dies;
// the container must terminate rather than poll a dead cluster forever.
func TestCheckForClockTickPanicsWhenCommitPositionReclaimed(t *testing.T) {
	metaBuf := atomic.MakeBuffer(make([]byte, counters.MetadataLength*2))
	valuesBuf := atomic.MakeBuffer(make([]byte, counters.CounterLength*2))
	reader := counters.NewReader(valuesBuf, metaBuf)
	metaBuf.PutInt32(0, counters.RecordAllocated)
	commitPos, err := counters.NewReadableCounter(reader, 0)
	if err != nil {
		t.Fatal(err)
	}

	agent := &ClusteredServiceAgent{
		commitPosition:           commitPos,
		markFileUpdateDeadlineMs: math.MaxInt64,
		isServiceActive:          true,
	}
	tick(agent) // healthy counter: must not panic

	metaBuf.PutInt32(0, counters.RecordReclaimed)
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when commit position counter is reclaimed")
		}
		if agent.isServiceActive {
			t.Error("agent must not remain active after abort")
		}
	}()
	tick(agent)
}
