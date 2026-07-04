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

package clustertests

import (
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	atomicbuf "github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logging"
	"github.com/lirm/aeron-go/cluster"
	"github.com/lirm/aeron-go/cluster/client"
	"go.uber.org/zap/zapcore"
)

type egressCollector struct {
	messages chan string
}

func (c *egressCollector) OnConnect(cluster *client.AeronCluster) {}

func (c *egressCollector) OnDisconnect(cluster *client.AeronCluster, details string) {}

func (c *egressCollector) OnMessage(
	cluster *client.AeronCluster,
	timestamp int64,
	buffer *atomicbuf.Buffer,
	offset int32,
	length int32,
	header *logbuffer.Header,
) {
	c.messages <- string(buffer.GetBytesArray(offset, length))
}

func (c *egressCollector) OnNewLeader(cluster *client.AeronCluster, leadershipTermId int64, leaderMemberId int32) {
}

func (c *egressCollector) OnError(cluster *client.AeronCluster, details string) {
	logger.Errorf("egress error: %s", details)
}

// serviceRunner drives a clustered service agent's duty cycle on its own
// goroutine until shutdown is requested.
type serviceRunner struct {
	service *echoService
	agent   *cluster.ClusteredServiceAgent
	stop    atomic.Bool
	done    chan struct{}
}

func startServiceAgent(t *testing.T, driver *ClusteredMediaDriver) *serviceRunner {
	t.Helper()
	opts := cluster.NewOptions()
	opts.ClusterDir = driver.ClusterDir
	if os.Getenv("CLUSTER_TEST_DEBUG") != "" {
		opts.Loglevel = zapcore.DebugLevel
	}
	service := newEchoService()
	agent, err := cluster.NewClusteredServiceAgent(aeron.NewContext().AeronDir(driver.AeronDir), opts, service)
	if err != nil {
		t.Fatalf("failed to create service agent: %v", err)
	}

	runner := &serviceRunner{service: service, agent: agent, done: make(chan struct{})}
	started := make(chan error, 1)
	go func() {
		defer close(runner.done)
		// The client library panics when the media driver goes away at
		// shutdown; that must not take the test process down.
		defer func() {
			if r := recover(); r != nil && !runner.stop.Load() {
				t.Errorf("service agent panicked: %v", r)
			}
		}()
		if err := agent.OnStart(); err != nil {
			started <- err
			return
		}
		started <- nil
		for !runner.stop.Load() {
			agent.Idle(agent.DoWork())
		}
	}()

	select {
	case err := <-started:
		if err != nil {
			t.Fatalf("service agent failed to start: %v", err)
		}
	case <-time.After(30 * time.Second):
		dumpClusterState(t, driver)
		t.Fatal("timed out waiting for service agent to join the cluster")
	}
	return runner
}

func (runner *serviceRunner) shutdown() {
	runner.stop.Store(true)
	select {
	case <-runner.done:
	case <-time.After(10 * time.Second):
		logger.Errorf("service agent duty cycle did not stop in time")
	}
	runner.agent.Close()
}

func connectClient(t *testing.T, driver *ClusteredMediaDriver) (*client.AeronCluster, *egressCollector) {
	t.Helper()
	return connectClusterClient(t, driver, "0="+driver.IngressEndpoint)
}

// connectClusterClient connects a cluster client through the given driver's
// media driver, with ingress publications to every listed member endpoint.
func connectClusterClient(
	t *testing.T,
	driver *ClusteredMediaDriver,
	ingressEndpoints string,
) (*client.AeronCluster, *egressCollector) {
	t.Helper()
	clientOpts := client.NewOptions()
	clientOpts.IngressChannel = "aeron:udp?alias=cluster-client-ingress"
	clientOpts.IngressEndpoints = ingressEndpoints
	clientOpts.RetryBackoff = time.Second
	if os.Getenv("CLUSTER_TEST_DEBUG") != "" {
		// The client library never applies Options.Loglevel; set it directly.
		logging.SetLevel(zapcore.DebugLevel, "cluster-client")
	}
	collector := &egressCollector{messages: make(chan string, 100)}
	clusterClient, err := client.NewAeronCluster(aeron.NewContext().AeronDir(driver.AeronDir), clientOpts, collector)
	if err != nil {
		t.Fatalf("failed to create cluster client: %v", err)
	}
	awaitClientConnected(t, clusterClient, driver)
	return clusterClient, collector
}

func awaitClientConnected(t *testing.T, clusterClient *client.AeronCluster, driver *ClusteredMediaDriver) {
	t.Helper()
	idler := client.NewOptions().IdleStrategy
	deadline := time.Now().Add(60 * time.Second)
	for !clusterClient.IsConnected() {
		if time.Now().After(deadline) {
			dumpClusterState(t, driver)
			t.Fatal("timed out waiting for cluster client to connect")
		}
		idler.Idle(clusterClient.Poll())
	}
}

// awaitLeader returns the index of the runner whose node is the cluster
// leader, waiting for an election to conclude if necessary.
func awaitLeader(t *testing.T, runners []*serviceRunner, nodes []*ClusteredMediaDriver) int {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		for i, runner := range runners {
			if runner != nil && runner.agent.Role() == cluster.Leader {
				return i
			}
		}
		if time.Now().After(deadline) {
			for i, node := range nodes {
				if runners[i] != nil {
					t.Logf("--- node %d ---", i)
					dumpClusterState(t, node)
				}
			}
			t.Fatal("timed out waiting for a leader to be elected")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// awaitAllMessageCounts waits until every live runner's service has processed
// the expected number of messages, i.e. every node is caught up on the log.
func awaitAllMessageCounts(t *testing.T, runners []*serviceRunner, expected int32) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for _, runner := range runners {
		if runner == nil {
			continue
		}
		for runner.service.messageCount.Load() != expected {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for all nodes to reach %d messages", expected)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// dumpClusterState logs the consensus module's view of the cluster and the
// driver process output, for diagnosing timeouts.
func dumpClusterState(t *testing.T, driver *ClusteredMediaDriver) {
	t.Helper()
	t.Logf("driver process exited=%v", driver.Exited())
	if data, err := os.ReadFile(driver.LogPath); err == nil {
		out := string(data)
		if len(out) > 4000 {
			out = out[len(out)-4000:]
		}
		t.Logf("driver log tail:\n%s", out)
	}
	for _, command := range []string{"describe", "errors"} {
		out, err := driver.ClusterTool(command)
		t.Logf("ClusterTool %s (err=%v):\n%s", command, err, out)
	}
}

// sendAndAwaitEchoes offers every payload to the cluster and waits until each
// one has come back on egress.
func sendAndAwaitEchoes(
	t *testing.T,
	clusterClient *client.AeronCluster,
	collector *egressCollector,
	payloads []string,
) {
	t.Helper()
	sendBuf := atomicbuf.MakeBuffer(make([]byte, 256))
	for _, payload := range payloads {
		payloadBytes := []byte(payload)
		sendBuf.PutBytesArray(0, &payloadBytes, 0, int32(len(payloadBytes)))
		// Generous deadline: after a leader kill the client may need several
		// reconnect cycles before an offer can succeed.
		deadline := time.Now().Add(30 * time.Second)
		for clusterClient.Offer(sendBuf, 0, int32(len(payloadBytes))) < 0 {
			if time.Now().After(deadline) {
				t.Fatalf("timed out offering %q", payload)
			}
			clusterClient.Poll()
		}
	}

	received := make(map[string]bool)
	deadline := time.After(30 * time.Second)
	for len(received) < len(payloads) {
		clusterClient.Poll()
		select {
		case msg := <-collector.messages:
			received[msg] = true
		case <-deadline:
			t.Fatalf("timed out waiting for echoes - received %d of %d", len(received), len(payloads))
		default:
			time.Sleep(time.Millisecond)
		}
	}
	for _, payload := range payloads {
		if !received[payload] {
			t.Errorf("missing echo for %q", payload)
		}
	}
}

// triggerSnapshot asks the consensus module for a snapshot via ClusterTool
// and waits for the service to take it. The tool only returns success once
// the snapshot recording is acknowledged by the service.
func triggerSnapshot(t *testing.T, driver *ClusteredMediaDriver, service *echoService) {
	t.Helper()
	before := service.snapshots.Load()
	if out, err := driver.ClusterTool("snapshot"); err != nil {
		t.Fatalf("ClusterTool snapshot failed: %v - output: %s", err, out)
	}
	deadline := time.Now().Add(30 * time.Second)
	for service.snapshots.Load() == before {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for service to take a snapshot")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func awaitMessageCount(t *testing.T, service *echoService, expected int32) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for service.messageCount.Load() != expected {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for message count %d, have %d", expected, service.messageCount.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func payloads(prefix string, count int) []string {
	result := make([]string, count)
	for i := range result {
		result[i] = fmt.Sprintf("%s-%d", prefix, i)
	}
	return result
}

func requireDriver(t *testing.T) *ClusteredMediaDriver {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping cluster system test in short mode")
	}
	if jar, ok := JarAvailable(); !ok {
		t.Skipf("aeron-all jar not found at %s - see harness.go for fetch instructions", jar)
	}
	driver, err := StartClusteredMediaDriver()
	if err != nil {
		t.Fatalf("failed to start ClusteredMediaDriver: %v", err)
	}
	return driver
}

// TestClusterEchoAndSnapshot runs the Go service container and cluster
// client against a Java 1.52 ClusteredMediaDriver: client messages must be
// replicated through the log, echoed by the service, and delivered on
// egress; a ClusterTool-triggered snapshot must invoke OnTakeSnapshot and
// be acked (with its recording id) so the tool completes.
func TestClusterEchoAndSnapshot(t *testing.T) {
	driver := requireDriver(t)
	defer driver.Stop()

	runner := startServiceAgent(t, driver)
	defer runner.shutdown()

	clusterClient, collector := connectClient(t, driver)
	defer clusterClient.Close()

	sendAndAwaitEchoes(t, clusterClient, collector, payloads("echo-test-message", 10))
	triggerSnapshot(t, driver, runner.service)

	if got := runner.service.messageCount.Load(); got != 10 {
		t.Errorf("service processed %d messages, expected 10", got)
	}
}

// TestClusterTimers verifies deterministic cluster timers, modeled on
// upstream's ClusterTimerTest: a timer scheduled by the service (from within
// session message processing) must expire through the replicated log and
// reach OnTimerEvent; a cancelled timer must not fire.
func TestClusterTimers(t *testing.T) {
	driver := requireDriver(t)
	defer driver.Stop()

	runner := startServiceAgent(t, driver)
	defer runner.shutdown()

	clusterClient, collector := connectClient(t, driver)
	defer clusterClient.Close()

	// Schedule a short timer and a long one, then cancel the long one.
	sendAndAwaitEchoes(t, clusterClient, collector, []string{
		"timer:100:500",
		"timer:101:60000",
		"cancel-timer:101",
	})

	select {
	case correlationId := <-runner.service.timerEvents:
		if correlationId != 100 {
			t.Errorf("expected timer 100 to fire, got %d", correlationId)
		}
	case <-time.After(15 * time.Second):
		dumpClusterState(t, driver)
		t.Fatal("timed out waiting for timer to fire")
	}

	// The cancelled timer must stay silent; watch briefly for stragglers.
	select {
	case correlationId := <-runner.service.timerEvents:
		t.Errorf("unexpected timer fired: %d", correlationId)
	case <-time.After(2 * time.Second):
	}
}

// TestClusterMultiNodeFailover runs a three node cluster, each node a Java
// ClusteredMediaDriver with a Go service container, and verifies leadership
// failover: when the leader node is killed, a survivor must win the election,
// the service containers must change role, and the client must reconnect and
// continue to have messages served.
func TestClusterMultiNodeFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cluster system test in short mode")
	}
	if jar, ok := JarAvailable(); !ok {
		t.Skipf("aeron-all jar not found at %s - see harness.go for fetch instructions", jar)
	}
	testCluster, err := StartTestCluster(3)
	if err != nil {
		t.Fatalf("failed to start test cluster: %v", err)
	}
	defer testCluster.Stop()

	runners := make([]*serviceRunner, len(testCluster.Nodes))
	for i, node := range testCluster.Nodes {
		runners[i] = startServiceAgent(t, node)
	}
	defer func() {
		for _, runner := range runners {
			if runner != nil {
				runner.stop.Store(true)
			}
		}
	}()

	leader := awaitLeader(t, runners, testCluster.Nodes)
	t.Logf("leader is node %d", leader)

	// Run the client via a follower's media driver so it survives the kill.
	clientNode := testCluster.Nodes[(leader+1)%len(testCluster.Nodes)]
	clusterClient, collector := connectClusterClient(t, clientNode, testCluster.IngressEndpoints)
	defer clusterClient.Close()

	sendAndAwaitEchoes(t, clusterClient, collector, payloads("before-failover", 5))
	// Every node must be caught up before the kill, so the survivors hold
	// the full log and can win the election.
	awaitAllMessageCounts(t, runners, 5)

	// Kill the leader node: stop its service container loop first (without
	// closing, its driver is about to die under it), then the process.
	runners[leader].stop.Store(true)
	<-runners[leader].done
	killed := runners[leader]
	runners[leader] = nil
	testCluster.Nodes[leader].Stop()
	_ = killed

	newLeader := awaitLeader(t, runners, testCluster.Nodes)
	t.Logf("new leader is node %d", newLeader)

	// The client must fail over to the new leader and messages must flow.
	awaitClientConnected(t, clusterClient, testCluster.Nodes[newLeader])
	sendAndAwaitEchoes(t, clusterClient, collector, payloads("after-failover", 5))

	awaitAllMessageCounts(t, runners, 10)
}

// TestClusterRestartFromSnapshot verifies the recovery path: after a
// snapshot and more messages, the cluster and the Go service container are
// restarted over the same cluster and archive directories. The service must
// restore its state from the snapshot (replayed through the archive), replay
// the post-snapshot log entries, and then serve new clients.
func TestClusterRestartFromSnapshot(t *testing.T) {
	driver := requireDriver(t)
	currentDriver := driver
	defer func() { currentDriver.Stop() }()

	runner := startServiceAgent(t, driver)
	clusterClient, collector := connectClient(t, driver)

	sendAndAwaitEchoes(t, clusterClient, collector, payloads("pre-snapshot", 10))
	triggerSnapshot(t, driver, runner.service)
	sendAndAwaitEchoes(t, clusterClient, collector, payloads("post-snapshot", 5))

	// Take the whole node down: client, service container, then the driver
	// (gracefully, keeping the cluster and archive directories).
	clusterClient.Close()
	runner.shutdown()
	driver.Shutdown()

	restarted, err := driver.Restart()
	if err != nil {
		t.Fatalf("failed to restart ClusteredMediaDriver: %v", err)
	}
	currentDriver = restarted

	// The new service instance must recover the snapshot state (10) and
	// replay the 5 post-snapshot log entries.
	recoveredRunner := startServiceAgent(t, restarted)
	defer recoveredRunner.shutdown()
	awaitMessageCount(t, recoveredRunner.service, 15)

	// And it must serve new clients.
	newClient, newCollector := connectClient(t, restarted)
	defer newClient.Close()
	sendAndAwaitEchoes(t, newClient, newCollector, payloads("after-restart", 5))
	awaitMessageCount(t, recoveredRunner.service, 20)
}
