// Copyright 2018 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package datanode

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/cubefs/cubefs/datanode/storage"
	"github.com/cubefs/cubefs/depends/tiglabs/raft"
	raftproto "github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/auditlog"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

/* The functions below implement the interfaces defined in the raft library. */

// Apply puts the data onto the disk.
func (dp *DataPartition) Apply(command []byte, index uint64) (resp interface{}, err error) {
	buff := bytes.NewBuffer(command)
	var version uint32
	if err = binary.Read(buff, binary.BigEndian, &version); err != nil {
		return
	}
	resp = proto.OpOk
	if version != BinaryMarshalMagicVersion {
		var opItem *RaftCmdItem
		if opItem, err = UnmarshalRaftCmd(command); err != nil {
			log.LogErrorf("[ApplyRandomWrite] ApplyID(%v) Partition(%v) unmarshal failed(%v)", index, dp.partitionID, err)
			return
		}
		log.LogInfof("[ApplyRandomWrite] ApplyID(%v) Partition(%v) opItem Op(%v)", index, dp.partitionID, opItem.Op)
		if opItem.Op == uint32(proto.OpVersionOp) {
			dp.fsmVersionOp(opItem)
			return
		}
		return
	}
	if index > dp.metaAppliedID {
		resp, err = dp.ApplyRandomWrite(command, index)
		return
	}
	log.LogDebugf("[DataPartition.Apply] dp[%v] metaAppliedID(%v) index(%v) no need apply", dp.partitionID, dp.metaAppliedID, index)
	return
}

// ApplyMemberChange supports adding new raft member or deleting an existing raft member.
// It does not support updating an existing member at this point.
func (dp *DataPartition) ApplyMemberChange(confChange *raftproto.ConfChange, index uint64) (resp interface{}, err error) {
	// Change memory the status
	var (
		isUpdated bool
		msg       string
	)
	defer func(index uint64) {
		if err == nil {
			dp.uploadApplyID(index)
			// persist apply id immediately
			req := PersistApplyIdRequest{}
			req.done = make(chan struct{}, 1)
			dp.PersistApplyIdChan <- req
			<-req.done
		} else {
			err = fmt.Errorf("[ApplyMemberChange] ApplyID(%v) Partition(%v) apply err(%v)]", index, dp.partitionID, err)
			exporter.Warning(err.Error())
			// panic(newRaftApplyError(err))
		}
		auditlog.LogDataNodeOp("DataPartitionMemberChange exit", msg, err)
	}(index)

	if dp.extentStore.IsClosed() {
		log.LogWarnf("[ApplyMemberChange] vol(%v) dp(%v) delay apply member change, apply id(%v), dp already stop!", dp.volumeID, dp.partitionID, index)
		err = fmt.Errorf("dp(%v) already stop", dp.partitionID)
		return
	}

	switch confChange.Type {
	case raftproto.ConfAddNode:
		req := &proto.AddDataPartitionRaftMemberRequest{}
		if err = json.Unmarshal(confChange.Context, req); err != nil {
			return
		}
		msg = fmt.Sprintf("ConfAddNode [%v], partitionId [%v] index(%v)", req.AddPeer, req.PartitionId, index)
		log.LogInfof("action[ApplyMemberChange] %v", msg)
		isUpdated, err = dp.addRaftNode(req, index)
		if isUpdated && err == nil {
			// Perform the update replicas operation asynchronously after the execution of the member change applying
			// related process.
			updateWG := sync.WaitGroup{}
			updateWG.Add(1)

			go func() {
				defer updateWG.Done()
				//may fetch old replica, e.g. 3-replica back to 2-replica for adding raft member not return
				//if err = dp.updateReplicas(true); err != nil {
				//	log.LogErrorf("ApplyMemberChange: update partition %v replicas failed: %v", dp.partitionID, err)
				//	return
				//}
				if dp.isLeader {
					dp.ExtentStore().MoveAllToBrokenTinyExtentC(storage.TinyExtentCount)
				}
			}()
			updateWG.Wait()
		}
		auditlog.LogDataNodeOp("DataPartitionMemberChange", msg, err)
	case raftproto.ConfRemoveNode:
		req := &proto.RemoveDataPartitionRaftMemberRequest{}
		if err = json.Unmarshal(confChange.Context, req); err != nil {
			return
		}
		msg = fmt.Sprintf(" ConfRemoveNode [%v], partitionId [%v] index(%v)", req.RemovePeer, req.PartitionId, index)
		log.LogInfof("action[ApplyMemberChange] %v", msg)
		isUpdated, err = dp.removeRaftNode(req, index)
		auditlog.LogDataNodeOp("DataPartitionMemberChange", msg, err)
	case raftproto.ConfUpdateNode:
		log.LogDebugf("[updateRaftNode]: not support.")
	default:
		// do nothing
	}
	if err != nil {
		log.LogErrorf("action[ApplyMemberChange] dp(%v) type(%v) err(%v) index(%v).", dp.partitionID, confChange.Type, err, index)
		return
	}
	if isUpdated {
		dp.DataPartitionCreateType = proto.NormalCreateDataPartition
		if err = dp.PersistMetadata(); err != nil {
			log.LogErrorf("action[ApplyMemberChange] dp(%v) PersistMetadata err(%v).", dp.partitionID, err)
			dp.checkIsDiskError(err, WriteFlag)
			return
		}
	}
	return
}

// Snapshot persists the in-memory data (as a snapshot) to the disk.
// Note that the data in each data partition has already been saved on the disk. Therefore there is no need to take the
// snapshot in this case.
func (dp *DataPartition) Snapshot() (raftproto.Snapshot, error) {
	snapIterator := NewItemIterator(dp.raftPartition.AppliedIndex())
	log.LogInfof("SendSnapShot PartitionID(%v) Snapshot lastTruncateID(%v) currentApplyID(%v) firstCommitID(%v)",
		dp.partitionID, dp.lastTruncateID, dp.appliedID, dp.raftPartition.CommittedIndex())
	return snapIterator, nil
}

// ApplySnapshot asks the raft leader for the snapshot data to recover the contents on the local disk.
func (dp *DataPartition) ApplySnapshot(peers []raftproto.Peer, iterator raftproto.SnapIterator) (err error) {
	// Never delete the raft log which hadn't applied, so snapshot no need.
	log.LogInfof("PartitionID(%v) ApplySnapshot to (%v)", dp.partitionID, dp.raftPartition.CommittedIndex())
	return
}

// HandleFatalEvent notifies the application when panic happens.
func (dp *DataPartition) HandleFatalEvent(err *raft.FatalError) {
	if isRaftApplyError(err.Err.Error()) || IsDiskErr(err.Err.Error()) {
		dp.stopRaft()
		dp.checkIsDiskError(err.Err, 0)
		log.LogCriticalf("action[HandleFatalEvent] raft apply err(%v), partitionId:%v", err, dp.partitionID)
	} else {
		log.LogFatalf("action[HandleFatalEvent] err(%v), partitionId:%v", err, dp.partitionID)
	}
}

// HandleLeaderChange notifies the application when the raft leader has changed.
func (dp *DataPartition) HandleLeaderChange(leader uint64) {
	defer func() {
		if r := recover(); r != nil {
			mesg := fmt.Sprintf("HandleLeaderChange(%v)  Raft Panic (%v)", dp.partitionID, r)
			panic(mesg)
		}
	}()
	if dp.config.NodeID == leader {
		dp.isRaftLeader = true
	}
}

// Put submits the raft log to the raft store.
func (dp *DataPartition) Put(key interface{}, val interface{}) (resp interface{}, err error) {
	if dp.raftStopped() {
		err = fmt.Errorf("%s key=%v", RaftNotStarted, key)
		return
	}
	resp, err = dp.raftPartition.Submit(val.([]byte))
	return
}

// Get returns the raft log based on the given key. It is not needed for replicating data partition.
func (dp *DataPartition) Get(key interface{}) (interface{}, error) {
	return nil, nil
}

// Del deletes the raft log based on the given key. It is not needed for replicating data partition.
func (dp *DataPartition) Del(key interface{}) (interface{}, error) {
	return nil, nil
}

func (dp *DataPartition) uploadApplyID(applyID uint64) {
	if applyID == 0 {
		return
	}
	log.LogDebugf("[uploadApplyID] dp(%v) upload apply id(%v)", dp.partitionID, applyID)
	atomic.StoreUint64(&dp.appliedID, applyID)
	atomic.StoreUint64(&dp.extentStore.ApplyId, applyID)
}
