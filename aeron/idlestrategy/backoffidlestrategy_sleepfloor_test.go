package idlestrategy

import (
	"sort"
	"testing"
	"time"
)

// parkOnce drives a fresh strategy through spins and yields until it reaches
// the park state, then times a single park.
func parkOnce(t *testing.T, mk func() *BackoffIdleStrategy) time.Duration {
	t.Helper()
	s := mk()
	for i := int64(0); i <= s.maxSpins+s.maxYields+1; i++ {
		s.Idle(0)
	}
	t0 := time.Now()
	s.Idle(0)
	return time.Since(t0)
}

// medianPark samples parkOnce n times. A single sample is not enough to
// separate the two paths: their tails overlap (an unfixed park can be as quick
// as ~75µs, a yielding one as slow as ~274µs) even though their medians differ
// by ~20x.
func medianPark(t *testing.T, n int, mk func() *BackoffIdleStrategy) time.Duration {
	t.Helper()
	ds := make([]time.Duration, n)
	for i := range ds {
		ds[i] = parkOnce(t, mk)
	}
	sort.Slice(ds, func(a, b int) bool { return ds[a] < ds[b] })
	return ds[n/2]
}

// A park well below the sleep floor must not cost the runtime timer's
// granularity. Go's time.Sleep overshoots short requests by orders of
// magnitude — a 6µs request can cost ~425µs — so the plain backoff ladder
// collapses to ~1ms parks by its fourth rung. This is the regression guard.
func TestYieldingBackoffHonoursShortParks(t *testing.T) {
	// minPark 50µs, far below the 1ms floor, so the first park yields.
	mk := func() *BackoffIdleStrategy {
		return NewYieldingBackoffIdleStrategy(DefaultMaxSpins, DefaultMaxYields,
			50_000, int64(time.Millisecond), DefaultSleepFloorNs)
	}
	// Measured: yielding ~50µs median, always-sleep ~1.08ms median. 250µs sits
	// between them with room on both sides.
	if got := medianPark(t, 21, mk); got > 250*time.Microsecond {
		t.Fatalf("median 50µs park took %v; expected ~50µs — the yield path is "+
			"not being taken, or time.Sleep is being used below the floor", got)
	}
}

// The guard above must actually separate the two paths: the same park through
// the unfixed always-sleep path has to trip it, or it is not a regression test.
func TestPlainBackoffTripsTheShortParkGuard(t *testing.T) {
	mk := func() *BackoffIdleStrategy {
		return NewBackoffIdleStrategy(DefaultMaxSpins, DefaultMaxYields, 50_000, int64(time.Millisecond))
	}
	if got := medianPark(t, 21, mk); got <= 250*time.Microsecond {
		t.Skipf("always-sleep path served a 50µs park in %v; this host's timer is "+
			"finer than the one the sleep floor exists for", got)
	}
}

// A park at or above the floor should still sleep, so a genuinely idle agent
// releases its processor rather than spinning forever.
func TestYieldingBackoffSleepsAtOrAboveFloor(t *testing.T) {
	floor := int64(2 * time.Millisecond)
	mk := func() *BackoffIdleStrategy {
		return NewYieldingBackoffIdleStrategy(DefaultMaxSpins, DefaultMaxYields, floor, floor, floor)
	}
	if got := medianPark(t, 5, mk); got < time.Millisecond {
		t.Fatalf("park at the floor took %v; expected it to actually sleep", got)
	}
}

// The plain constructor must be unchanged: sleepFloorNs zero means always sleep.
func TestPlainBackoffStillSleeps(t *testing.T) {
	s := NewDefaultBackoffIdleStrategy()
	if s.sleepFloorNs != 0 {
		t.Fatalf("plain backoff has sleepFloorNs=%d, want 0 (unchanged behaviour)", s.sleepFloorNs)
	}
}
