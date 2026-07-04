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
	"os/exec"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	atomicbuf "github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/cluster"
	"github.com/lirm/aeron-go/cluster/client"
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

// runServiceAgent drives the clustered service agent until stop is set. It
// runs the agent's duty cycle on its own goroutine, recovering from the
// panics the client library raises when the media driver is killed at
// test shutdown.
func runServiceAgent(t *testing.T, agent *cluster.ClusteredServiceAgent, stop *atomic.Bool, started chan<- error) {
	defer func() {
		if r := recover(); r != nil && !stop.Load() {
			t.Errorf("service agent panicked: %v", r)
		}
	}()
	if err := agent.OnStart(); err != nil {
		started <- err
		return
	}
	started <- nil
	for !stop.Load() {
		agent.Idle(agent.DoWork())
	}
}

// TestClusterEchoAndSnapshot runs the Go service container and cluster
// client against a Java 1.52 ClusteredMediaDriver: client messages must be
// replicated through the log, echoed by the service, and delivered on
// egress; a ClusterTool-triggered snapshot must invoke OnTakeSnapshot and
// be acked (with its recording id) so the tool completes.
func TestClusterEchoAndSnapshot(t *testing.T) {
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
	defer driver.Stop()

	// Start the Go clustered service container.
	serviceCtx := aeron.NewContext().AeronDir(driver.AeronDir)
	opts := cluster.NewOptions()
	opts.ClusterDir = driver.ClusterDir
	service := newEchoService()
	agent, err := cluster.NewClusteredServiceAgent(serviceCtx, opts, service)
	if err != nil {
		t.Fatalf("failed to create service agent: %v", err)
	}
	stop := &atomic.Bool{}
	defer stop.Store(true)
	started := make(chan error, 1)
	go runServiceAgent(t, agent, stop, started)
	select {
	case err := <-started:
		if err != nil {
			t.Fatalf("service agent failed to start: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for service agent to join the cluster")
	}

	// Connect the cluster client.
	clientCtx := aeron.NewContext().AeronDir(driver.AeronDir)
	clientOpts := client.NewOptions()
	clientOpts.IngressChannel = "aeron:udp?alias=cluster-client-ingress|endpoint=" + ingressEndpoint
	clientOpts.IngressEndpoints = "0=" + ingressEndpoint
	collector := &egressCollector{messages: make(chan string, 100)}
	clusterClient, err := client.NewAeronCluster(clientCtx, clientOpts, collector)
	if err != nil {
		t.Fatalf("failed to create cluster client: %v", err)
	}
	defer clusterClient.Close()

	connectDeadline := time.Now().Add(30 * time.Second)
	for !clusterClient.IsConnected() {
		if time.Now().After(connectDeadline) {
			t.Fatal("timed out waiting for cluster client to connect")
		}
		clientOpts.IdleStrategy.Idle(clusterClient.Poll())
	}

	// Send messages and expect them all echoed back.
	const messageCount = 10
	sendBuf := atomicbuf.MakeBuffer(make([]byte, 64))
	for i := 0; i < messageCount; i++ {
		payload := fmt.Sprintf("echo-test-message-%d", i)
		payloadBytes := []byte(payload)
		sendBuf.PutBytesArray(0, &payloadBytes, 0, int32(len(payloadBytes)))
		offerDeadline := time.Now().Add(10 * time.Second)
		for clusterClient.Offer(sendBuf, 0, int32(len(payload))) < 0 {
			if time.Now().After(offerDeadline) {
				t.Fatalf("timed out offering message %d", i)
			}
			clientOpts.IdleStrategy.Idle(clusterClient.Poll())
		}
	}

	received := make(map[string]bool)
	echoDeadline := time.After(30 * time.Second)
	for len(received) < messageCount {
		clusterClient.Poll()
		select {
		case msg := <-collector.messages:
			received[msg] = true
		case <-echoDeadline:
			t.Fatalf("timed out waiting for echoes - received %d of %d", len(received), messageCount)
		default:
			clientOpts.IdleStrategy.Idle(0)
		}
	}
	for i := 0; i < messageCount; i++ {
		payload := fmt.Sprintf("echo-test-message-%d", i)
		if !received[payload] {
			t.Errorf("missing echo for %q", payload)
		}
	}

	// Trigger a snapshot via ClusterTool and verify the service takes it.
	// The tool only returns success once the consensus module has seen the
	// snapshot completed, which requires the service's ack (with the
	// snapshot recording id) to be processed.
	jar, _ := JarAvailable()
	tool := exec.Command(
		"java",
		"--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
		"--add-exports=java.base/jdk.internal.misc=ALL-UNNAMED",
		"-cp", jar,
		"io.aeron.cluster.ClusterTool",
		driver.ClusterDir,
		"snapshot",
	)
	if out, err := tool.CombinedOutput(); err != nil {
		t.Fatalf("ClusterTool snapshot failed: %v - output: %s", err, out)
	}
	snapshotDeadline := time.Now().Add(30 * time.Second)
	for service.snapshots.Load() == 0 {
		if time.Now().After(snapshotDeadline) {
			t.Fatal("timed out waiting for service to take a snapshot")
		}
		time.Sleep(10 * time.Millisecond)
	}

	if got := service.messageCount.Load(); got != messageCount {
		t.Errorf("service processed %d messages, expected %d", got, messageCount)
	}
}
