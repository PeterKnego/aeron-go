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

// Package cluster contains integration tests that run the Go clustered
// service container and cluster client against a real Java
// ClusteredMediaDriver (media driver + archive + consensus module).
package clustertests

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/logging"
)

// The jar is not committed; fetch it with:
//
//	curl -fLO --output-dir systests/cluster \
//	  https://repo1.maven.org/maven2/io/aeron/aeron-all/1.52.0/aeron-all-1.52.0.jar
//
// or point AERON_ALL_JAR at an existing copy.
const defaultJarName = "aeron-all-1.52.0.jar"

const clusteredMediaDriverClassName = "io.aeron.cluster.ClusteredMediaDriver"

// Single node member config, mirroring upstream's ClusterTestConstants. The
// ports are derived from the pid so that back-to-back test invocations don't
// race a previous driver's socket release.
var (
	basePort        = 20000 + 20*(os.Getpid()%1000)
	ingressEndpoint = fmt.Sprintf("localhost:%d", basePort)
	clusterMembers  = fmt.Sprintf("0,localhost:%d,localhost:%d,localhost:%d,localhost:0,localhost:%d",
		basePort, basePort+1, basePort+2, basePort+10)
	archiveControl = fmt.Sprintf("aeron:udp?endpoint=localhost:%d", basePort+10)
)

var logger = logging.MustGetLogger("clustertests")

// ClusteredMediaDriver wraps a Java ClusteredMediaDriver child process and
// the temp directories it runs in.
type ClusteredMediaDriver struct {
	AeronDir   string
	ClusterDir string
	ArchiveDir string
	cmd        *exec.Cmd
}

func jarPath() string {
	if path := os.Getenv("AERON_ALL_JAR"); path != "" {
		return path
	}
	return defaultJarName
}

// JarAvailable reports whether the aeron-all jar needed to launch the driver
// exists; tests should skip when it does not.
func JarAvailable() (string, bool) {
	path := jarPath()
	_, err := os.Stat(path)
	return path, err == nil
}

func StartClusteredMediaDriver() (*ClusteredMediaDriver, error) {
	jar, ok := JarAvailable()
	if !ok {
		return nil, fmt.Errorf("aeron-all jar not found at %s", jar)
	}

	id := uuid.New().String()
	aeronDir := fmt.Sprintf("%s/aeron-%s/%s/driver", aeron.DefaultAeronDir, aeron.UserName, id)
	baseDir, err := os.MkdirTemp("", "aeron-go-cluster-systest")
	if err != nil {
		return nil, err
	}
	clusterDir := baseDir + "/cluster"
	archiveDir := baseDir + "/archive"
	// The Go service container writes its mark file into the cluster dir,
	// possibly before the consensus module has created it.
	if err := os.MkdirAll(clusterDir, 0o755); err != nil {
		return nil, err
	}

	cmd := exec.Command(
		"java",
		"--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
		"--add-exports=java.base/jdk.internal.misc=ALL-UNNAMED",
		"-XX:+UnlockDiagnosticVMOptions",
		"-XX:GuaranteedSafepointInterval=300000",
		fmt.Sprintf("-Daeron.dir=%s", aeronDir),
		"-Daeron.dir.delete.on.start=true",
		"-Daeron.dir.delete.on.shutdown=true",
		"-Daeron.threading.mode=SHARED",
		fmt.Sprintf("-Daeron.client.liveness.timeout=%d", time.Minute.Nanoseconds()),
		fmt.Sprintf("-Daeron.publication.unblock.timeout=%d", 15*time.Minute.Nanoseconds()),
		fmt.Sprintf("-Daeron.archive.dir=%s", archiveDir),
		fmt.Sprintf("-Daeron.archive.control.channel=%s", archiveControl),
		"-Daeron.archive.replication.channel=aeron:udp?endpoint=localhost:0",
		"-Daeron.archive.threading.mode=SHARED",
		fmt.Sprintf("-Daeron.cluster.dir=%s", clusterDir),
		fmt.Sprintf("-Daeron.cluster.members=%s", clusterMembers),
		"-Daeron.cluster.ingress.channel=aeron:udp?term-length=64k",
		"-Daeron.cluster.replication.channel=aeron:udp?endpoint=localhost:0",
		"-Daeron.cluster.service.count=1",
		"-cp",
		jar,
		clusteredMediaDriverClassName,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	setupPdeathsig(cmd)

	logger.Infof("starting ClusteredMediaDriver: %s", cmd)
	if err := cmd.Start(); err != nil {
		return nil, err
	}

	driver := &ClusteredMediaDriver{
		AeronDir:   aeronDir,
		ClusterDir: clusterDir,
		ArchiveDir: archiveDir,
		cmd:        cmd,
	}
	if err := driver.awaitMediaDriverReady(); err != nil {
		driver.Stop()
		return nil, err
	}
	return driver, nil
}

func (driver *ClusteredMediaDriver) Stop() {
	if err := driver.cmd.Process.Kill(); err != nil {
		logger.Errorf("couldn't kill ClusteredMediaDriver: %v", err)
	}
	_, _ = driver.cmd.Process.Wait()
	for _, dir := range []string{driver.AeronDir, driver.ClusterDir, driver.ArchiveDir} {
		if err := os.RemoveAll(dir); err != nil {
			logger.Errorf("failed to remove %s: %v", dir, err)
		}
	}
}

func (driver *ClusteredMediaDriver) awaitMediaDriverReady() error {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ctx := aeron.NewContext().AeronDir(driver.AeronDir).MediaDriverTimeout(10 * time.Second)
		cxn, err := aeron.Connect(ctx)
		if err == nil {
			return cxn.Close()
		}
		time.Sleep(50 * time.Millisecond)
	}
	return errors.New("timed out waiting for ClusteredMediaDriver to start")
}

// Setting Pdeathsig kills the child process when the test process dies, but
// this only works on linux; elsewhere a panicking test can strand the driver.
func setupPdeathsig(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	pdeathsig := reflect.ValueOf(cmd.SysProcAttr).Elem().FieldByName("Pdeathsig")
	if pdeathsig.IsValid() {
		pdeathsig.Set(reflect.ValueOf(syscall.SIGTERM))
	}
}
