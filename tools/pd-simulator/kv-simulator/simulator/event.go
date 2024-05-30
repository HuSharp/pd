// Copyright 2018 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package simulator

import (
	"context"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/tools/pd-simulator/kv-simulator/simulator/cases"
	"go.uber.org/zap"
	"math/rand"
	"sync"
)

// Event affects the status of the cluster.
type Event interface {
	Run(raft *RaftEngine, tickCount int64) bool
}

// EventRunner includes all events.
type EventRunner struct {
	sync.RWMutex
	events     []Event
	raftEngine *RaftEngine
}

// NewEventRunner creates an event runner.
func NewEventRunner(events []cases.EventDescriptor, raftEngine *RaftEngine) *EventRunner {
	er := &EventRunner{events: make([]Event, 0, len(events)), raftEngine: raftEngine}
	for _, e := range events {
		event := parserEvent(e)
		if event != nil {
			er.events = append(er.events, event)
		}
	}
	return er
}

func (er *EventRunner) AddEvent(e Event) {
	er.Lock()
	defer er.Unlock()
	er.events = append(er.events, e)
}

func parserEvent(e cases.EventDescriptor) Event {
	switch t := e.(type) {
	case *cases.WriteFlowOnSpotDescriptor:
		return &WriteFlowOnSpot{descriptor: t}
	case *cases.WriteFlowOnRegionDescriptor:
		return &WriteFlowOnRegion{descriptor: t}
	case *cases.ReadFlowOnRegionDescriptor:
		return &ReadFlowOnRegion{descriptor: t}
	}
	return nil
}

// Tick ticks the event run
func (er *EventRunner) Tick(tickCount int64) {
	var finishedIndex int
	for i, e := range er.events {
		isFinished := e.Run(er.raftEngine, tickCount)
		if isFinished {
			er.events[i], er.events[finishedIndex] = er.events[finishedIndex], er.events[i]
			finishedIndex++
		}
	}
	er.events = er.events[finishedIndex:]
}

// WriteFlowOnSpot writes bytes in some range.
type WriteFlowOnSpot struct {
	descriptor *cases.WriteFlowOnSpotDescriptor
}

// Run implements the event interface.
func (e *WriteFlowOnSpot) Run(raft *RaftEngine, tickCount int64) bool {
	res := e.descriptor.Step(tickCount)
	for key, size := range res {
		region := raft.GetRegionByKey([]byte(key))
		log.Debug("[kv-simulator] search the region", zap.Reflect("region", region.GetMeta()))
		if region == nil {
			log.Error("[kv-simulator] region not found for key", zap.String("key", key))
			continue
		}
		raft.updateRegionStore(region, size)
	}
	return false
}

// WriteFlowOnRegion writes bytes in some region.
type WriteFlowOnRegion struct {
	descriptor *cases.WriteFlowOnRegionDescriptor
}

// Run implements the event interface.
func (e *WriteFlowOnRegion) Run(raft *RaftEngine, tickCount int64) bool {
	res := e.descriptor.Step(tickCount)
	for id, bytes := range res {
		region := raft.GetRegion(id)
		if region == nil {
			log.Error("[kv-simulator] region is not found", zap.Uint64("region-id", id))
			continue
		}
		raft.updateRegionStore(region, bytes)
	}
	return false
}

// ReadFlowOnRegion reads bytes in some region
type ReadFlowOnRegion struct {
	descriptor *cases.ReadFlowOnRegionDescriptor
}

// Run implements the event interface.
func (e *ReadFlowOnRegion) Run(raft *RaftEngine, tickCount int64) bool {
	res := e.descriptor.Step(tickCount)
	raft.updateRegionReadBytes(res)
	return false
}

// AddNode adds nodes.
type AddNode struct{}

// Run implements the event interface.
func (e *AddNode) Run(raft *RaftEngine, _ int64) bool {
	config := raft.storeConfig
	nodes := raft.conn.getNodes()
	id, err := nodes[0].client.AllocID(context.Background())
	if err != nil {
		log.Error("[kv-simulator] alloc node id failed", zap.Error(err))
		return false
	}
	s := &cases.Store{
		ID:       id,
		Status:   metapb.StoreState_Up,
		Capacity: uint64(config.RaftStore.Capacity),
		Version:  config.StoreVersion,
	}
	n, err := NewNode(s, raft.conn.pdAddr, config)
	if err != nil {
		log.Error("[kv-simulator] create node failed", zap.Error(err))
		return false
	}

	raft.conn.Nodes[s.ID] = n
	n.raftEngine = raft
	err = n.Start()
	if err != nil {
		delete(raft.conn.Nodes, s.ID)
		log.Error("[kv-simulator] start node failed", zap.Uint64("node-id", s.ID), zap.Error(err))
		return false
	}
	log.Info("[kv-simulator] add node", zap.Uint64("node-id", s.ID))
	return true
}

// DownNode deletes nodes.
type DownNode struct {
	ID int
}

// Run implements the event interface.
func (e *DownNode) Run(raft *RaftEngine, _ int64) bool {
	nodes := raft.conn.getNodes()
	if len(nodes) == 0 {
		log.Error("[kv-simulator] can not find any node")
		return false
	}
	i := e.ID
	if e.ID == 0 {
		i = rand.Intn(len(nodes))
	}
	node := nodes[i]
	if node == nil {
		log.Error("[kv-simulator] node is not existed", zap.Uint64("node-id", node.Id))
		return false
	}
	delete(raft.conn.Nodes, node.Id)
	node.Stop()

	regions := raft.GetRegions()
	for _, region := range regions {
		storeIDs := region.GetStoreIDs()
		if _, ok := storeIDs[node.Id]; ok {
			downPeer := &pdpb.PeerStats{
				Peer:        region.GetStorePeer(node.Id),
				DownSeconds: 24 * 60 * 60,
			}
			region = region.Clone(core.WithDownPeers(append(region.GetDownPeers(), downPeer)))
			raft.SetRegion(region)
		}
	}
	log.Info("[kv-simulator] down node", zap.Uint64("node-id", node.Id))
	return true
}
