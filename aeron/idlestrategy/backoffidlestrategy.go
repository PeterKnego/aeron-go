package idlestrategy

import (
	"fmt"
	"runtime"
	"time"
)

// BackoffIdleStrategy is an idling strategy for threads when they have no work to do.
// Spin for maxSpins, then yield with runtime.Gosched for maxYields,
// then park with time.Sleep on an exponential backoff to maxParkPeriodNs.
type BackoffIdleStrategy struct {
	// configured max spins, yield, and min / max park period
	maxSpins, maxYields, minParkPeriodNs, maxParkPeriodNs int64
	// sleepFloorNs is the shortest park handed to time.Sleep; shorter parks
	// yield to a deadline instead. Zero keeps the original always-sleep
	// behaviour.
	sleepFloorNs int64
	// current state
	state backoffState
	// current number of spins, yield, and park period.
	spins, yields, parkPeriodNs int64
}

// NewBackoffIdleStrategy returns a BackoffIdleStrategy with the given parameters.
func NewBackoffIdleStrategy(maxSpins, maxYields, minParkPeriodNs, maxParkPeriodNs int64) *BackoffIdleStrategy {
	return &BackoffIdleStrategy{
		maxSpins:        maxSpins,
		maxYields:       maxYields,
		minParkPeriodNs: minParkPeriodNs,
		maxParkPeriodNs: maxParkPeriodNs,
	}
}

// DefaultMaxSpins is the default number of times the strategy will spin without work before going to next state.
const DefaultMaxSpins = 10

// DefaultMaxYields is the default number of times the strategy will yield without work before going to next state.
const DefaultMaxYields = 20

// DefaultMinParkNs is the default interval the strategy will park the thread on entering the park state.
const DefaultMinParkNs = 1000

// DefaultMaxParkNs is the default interval the strategy will park the thread will expand interval to as a max.
const DefaultMaxParkNs = int64(1 * time.Millisecond)

// NewDefaultBackoffIdleStrategy returns a BackoffIdleStrategy using DefaultMaxSpins, DefaultMaxYields,
// DefaultMinParkNs, and DefaultMaxParkNs.
func NewDefaultBackoffIdleStrategy() *BackoffIdleStrategy {
	return NewBackoffIdleStrategy(DefaultMaxSpins, DefaultMaxYields, DefaultMinParkNs, DefaultMaxParkNs)
}

// DefaultSleepFloorNs is the shortest park worth handing to time.Sleep.
//
// Go's runtime timer overshoots short sleeps by orders of magnitude: measured
// on Linux/amd64, requests up to ~4µs cost ~6-10µs, but a 6µs request costs
// ~425µs and anything from ~8µs upwards costs the same as a 1ms request. The
// exact threshold moves with host and load, so this floor is set well above it.
//
// The consequence for the plain backoff ladder is that its 1µs->1ms doubling
// does not ramp: by the fourth park it is already paying ~1ms, and every rung
// above that is indistinguishable. maxParkPeriodNs is effectively inert.
const DefaultSleepFloorNs = int64(time.Millisecond)

// NewYieldingBackoffIdleStrategy returns a BackoffIdleStrategy that serves
// parks shorter than sleepFloorNs by yielding to a deadline rather than
// sleeping, so the ladder's short rungs are honoured instead of collapsing to
// the runtime timer's granularity. Parks at or above the floor still sleep, so
// a genuinely idle agent still releases its processor.
//
// This trades CPU for latency on the short rungs and suits a duty cycle whose
// wakeup latency matters — a clustered service container, say. Prefer the plain
// NewBackoffIdleStrategy for background agents that should stay cheap when idle.
func NewYieldingBackoffIdleStrategy(maxSpins, maxYields, minParkPeriodNs, maxParkPeriodNs, sleepFloorNs int64) *BackoffIdleStrategy {
	s := NewBackoffIdleStrategy(maxSpins, maxYields, minParkPeriodNs, maxParkPeriodNs)
	s.sleepFloorNs = sleepFloorNs
	return s
}

// NewDefaultYieldingBackoffIdleStrategy is NewYieldingBackoffIdleStrategy with
// the default spin/yield/park parameters and DefaultSleepFloorNs.
func NewDefaultYieldingBackoffIdleStrategy() *BackoffIdleStrategy {
	return NewYieldingBackoffIdleStrategy(DefaultMaxSpins, DefaultMaxYields,
		DefaultMinParkNs, DefaultMaxParkNs, DefaultSleepFloorNs)
}

type backoffState int8

const (
	// Denotes a non-idle state.
	backoffNotIdle backoffState = iota
	// Denotes a spinning state.
	backoffSpinning
	// Denotes an yielding state.
	backoffYielding
	// Denotes a parking state.
	backoffParking
)

func (s *BackoffIdleStrategy) Idle(workCount int) {
	if workCount > 0 {
		s.reset()
	} else {
		s.idle()
	}
}

func (s *BackoffIdleStrategy) String() string {
	return fmt.Sprintf("BackoffIdleStrategy(MaxSpins:%d, MaxYields:%d, MinParkPeriodNs:%d, MaxParkPeriodNs:%d, SleepFloorNs:%d)",
		s.maxSpins, s.maxYields, s.minParkPeriodNs, s.maxParkPeriodNs, s.sleepFloorNs)
}

// park waits ns, yielding rather than sleeping when the wait is shorter than
// sleepFloorNs — see DefaultSleepFloorNs for why a short time.Sleep is not
// short.
func (s *BackoffIdleStrategy) park(ns int64) {
	if s.sleepFloorNs > 0 && ns < s.sleepFloorNs {
		deadline := time.Now().Add(time.Duration(ns))
		for time.Now().Before(deadline) {
			runtime.Gosched()
		}
		return
	}
	time.Sleep(time.Duration(ns))
}

func (s *BackoffIdleStrategy) reset() {
	s.spins = 0
	s.yields = 0
	s.parkPeriodNs = s.minParkPeriodNs
	s.state = backoffNotIdle
}

func (s *BackoffIdleStrategy) idle() {
	switch s.state {
	case backoffNotIdle:
		s.state = backoffSpinning
		s.spins++
	case backoffSpinning:
		// We should call procyield here, see https://golang.org/src/runtime/lock_futex.go
		s.spins++
		if s.spins > s.maxSpins {
			s.state = backoffYielding
			s.yields = 0
		}
	case backoffYielding:
		s.yields++
		if s.yields > s.maxYields {
			s.state = backoffParking
			s.parkPeriodNs = s.minParkPeriodNs
		} else {
			runtime.Gosched()
		}
	case backoffParking:
		s.park(s.parkPeriodNs)
		s.parkPeriodNs = s.parkPeriodNs << 1
		if s.parkPeriodNs > s.maxParkPeriodNs {
			s.parkPeriodNs = s.maxParkPeriodNs
		}
	}
}
