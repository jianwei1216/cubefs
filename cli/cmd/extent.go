package cmd

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/cli/cmd/data_check"
	"github.com/cubefs/cubefs/cli/cmd/util"
	util_sdk "github.com/cubefs/cubefs/cli/cmd/util/sdk"
	"github.com/cubefs/cubefs/metanode"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/http_client"
	sdk "github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/storage"
	"github.com/cubefs/cubefs/util/connpool"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/unit"
	"github.com/spf13/cobra"
)

const (
	cmdExtentUse                = "extent [command]"
	cmdExtentShort              = "Check extent consistency"
	cmdExtentInfo               = "info [partition] [extent]"
	cmdExtentInfoShort          = "show extent info"
	cmdExtentRepair             = "repair [partition] [extent(split by `-`)] [host]"
	cmdExtentRepairShort        = "repair extent"
	cmdCheckReplicaCrcUse       = "check-crc volumeName"
	cmdCheckReplicaCrcShort     = "Check replica crc consistency"
	cmdCheckEkNumUse            = "check-ek-num volumeName"
	cmdCheckEkNumShort          = "Check inode extent key num between all hosts"
	cmdCheckNlinkUse            = "check-nlink volumeName"
	cmdCheckNlinkShort          = "Check inode nlink"
	cmdSearchExtentUse          = "search volumeName"
	cmdSearchExtentShort        = "Search extent key"
	cmdCheckGarbageUse          = "check-garbage volumeName"
	cmdCheckGarbageShort        = "Check garbage extents"
	cmdCheckTinyExtentHoleUse   = "check-tiny-hole"
	cmdCheckTinyExtentHoleShort = "check tiny extent hole size and available size"
	cmdCheckExtentReplicaShort  = "check extent replica "
	cmdExtentDelParse           = "parse"
	cmdExtentDelParseShort      = "parse meta/data extent del file"
)

var client *sdk.MasterClient

type ExtentMd5 struct {
	PartitionID uint64 `json:"PartitionID"`
	ExtentID    uint64 `json:"ExtentID"`
	Md5         string `json:"md5"`
}

func newExtentCmd(mc *sdk.MasterClient) *cobra.Command {
	client = mc
	var cmd = &cobra.Command{
		Use:   cmdExtentUse,
		Short: cmdExtentShort,
		Args:  cobra.MinimumNArgs(1),
	}
	cmd.AddCommand(
		newExtentCheckCmd(data_check.CheckTypeExtentCrc),
		newExtentCheckCmd(data_check.CheckTypeInodeEkNum),
		newExtentCheckCmd(data_check.CheckTypeInodeNlink),
		newExtentSearchCmd(),
		newExtentGarbageCheckCmd(),
		newTinyExtentCheckHoleCmd(),
		newExtentGetCmd(),
		newExtentRepairCmd(),
		newExtentCheckByIdCmd(mc),
		newExtentParseCmd(),
	)
	return cmd
}

func newExtentGetCmd() *cobra.Command {
	var checkRetry bool
	var getMd5 bool
	var cmd = &cobra.Command{
		Use:   cmdExtentInfo,
		Short: cmdExtentInfoShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					stdout(err.Error())
				}
			}()
			partitionID, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return
			}
			extentID, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				return
			}
			dp, err := client.AdminAPI().GetDataPartition("", partitionID)
			if err != nil {
				return
			}
			fmt.Printf("%-30v: %v\n", "Volume", dp.VolName)
			fmt.Printf("%-30v: %v\n", "Data Partition", partitionID)
			fmt.Printf("%-30v: %v\n", "Extent", extentID)
			fmt.Printf("%-30v: %v\n", "Hosts", strings.Join(dp.Hosts, ","))
			fmt.Println()
			if proto.IsTinyExtent(extentID) {
				stdout("%v\n", formatTinyExtentTableHeader())
			} else {
				stdout("%v\n", formatNormalExtentTableHeader())
			}

			minSize := uint64(math.MaxUint64)
			for _, r := range dp.Replicas {
				dHost := fmt.Sprintf("%v:%v", strings.Split(r.Addr, ":")[0], client.DataNodeProfPort)
				dataClient := http_client.NewDataClient(dHost, false)

				extent, err1 := dataClient.GetExtentInfo(partitionID, extentID)
				if err1 != nil {
					continue
				}
				var md5Sum = "N/A"
				if getMd5 && extent[proto.ExtentInfoSize] < 16*unit.GB {
					md5Sum, err = dataClient.ComputeExtentMd5(partitionID, extentID, 0, extent[proto.ExtentInfoSize]-uint64(proto.PageSize))
					if err != nil {
						md5Sum = "N/A"
					}
				}
				if proto.IsTinyExtent(extentID) {
					extentHoles, _ := dataClient.GetExtentHoles(partitionID, extentID)
					stdout("%v\n", formatTinyExtent(r, extent, extentHoles, md5Sum))
				} else {
					stdout("%v\n", formatNormalExtent(r, extent, md5Sum))
				}
				if extent == nil {
					continue
				}
				if minSize > extent[proto.ExtentInfoSize] {
					minSize = extent[proto.ExtentInfoSize]
				}
			}
			blockSize := 128
			var wrongBlocks []int
			if !proto.IsTinyExtent(extentID) {
				stdout("wrongBlocks:\n")
				if wrongBlocks, err = data_check.CheckExtentBlockCrc(dp.Replicas, client.DataNodeProfPort, partitionID, extentID); err != nil {
					stdout("err: %v", err)
					return
				}
				stdout("found: %v blocks at first scan(block size: %vKB), wrong block: %v\n", len(wrongBlocks), blockSize, wrongBlocks)
				stdout("wrong offsets:")
				for _, b := range wrongBlocks {
					stdout("%v-%v,", b*128*1024, b*128*1024+128*1024)
				}
				stdout("\n")
				if len(wrongBlocks) == 0 {
					return
				}
				if !checkRetry {
					return
				}
				if minSize == math.MaxUint64 {
					return
				}
				stdout("begin retry check:\n")
				ek := proto.ExtentKey{
					Size:        uint32(minSize),
					PartitionId: partitionID,
					ExtentId:    extentID,
				}
				if wrongBlocks, err = data_check.RetryCheckBlockMd5(dp.Replicas, client.Nodes()[0], client.DataNodeProfPort, &ek, 0, 4, uint64(blockSize), wrongBlocks); err != nil {
					return
				}
				if len(wrongBlocks) == 0 {
					return
				}
				stdout("found: %v blocks at retry scan(block size: %vKB), wrong block index: %v\n", len(wrongBlocks), blockSize, wrongBlocks)
				stdout("\n")
				blockSize4K := 4
				stdout("begin check %v block:\n", blockSize4K)
				wrong4KBlocks := make([]int, 0)
				for _, b := range wrongBlocks {
					for i := 0; i < blockSize/blockSize4K; i++ {
						wrong4KBlocks = append(wrong4KBlocks, b*blockSize/blockSize4K+i)
					}
				}
				if wrong4KBlocks, err = data_check.RetryCheckBlockMd5(dp.Replicas, client.Nodes()[0], client.DataNodeProfPort, &ek, 0, 4, uint64(blockSize4K), wrong4KBlocks); err != nil {
					return
				}
				if len(wrong4KBlocks) == 0 {
					return
				}
				stdout("found: %v blocks at retry scan(block size: %vKB), wrong block index: %v\n", len(wrong4KBlocks), blockSize4K, wrong4KBlocks)
				for _, b := range wrong4KBlocks {
					var output string
					if _, output, _, err = data_check.CheckExtentReplicaByBlock(dp.Replicas, client.Nodes()[0], client.DataNodeProfPort, &ek, uint32(b), 0, uint64(blockSize4K)); err != nil {
						stdout("err: %v", err)
						return
					}
					stdout(output)
				}
			}
		},
	}
	cmd.Flags().BoolVar(&checkRetry, "check-retry", false, "check extent more times for accuracy")
	cmd.Flags().BoolVar(&getMd5, "check-md5", false, "get extent md5 info")
	return cmd
}

func newExtentRepairCmd() *cobra.Command {
	var fromFile bool
	var cmd = &cobra.Command{
		Use:   cmdExtentRepair,
		Short: cmdExtentRepairShort,
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					stdout(err.Error())
				}
			}()
			extentsMap := make(map[string]map[uint64]map[uint64]bool, 0)
			if fromFile {
				extentsMap = loadRepairExtents()
			} else {
				var partitionID uint64
				if len(args) < 3 {
					stdout("arguments not enough, must be 3")
				}
				partitionID, err = strconv.ParseUint(args[0], 10, 64)
				if err != nil {
					return
				}
				extentIDs := make(map[uint64]bool, 0)
				extentStrs := strings.Split(args[1], "-")
				for _, idStr := range extentStrs {
					if eid, err1 := strconv.ParseUint(idStr, 10, 64); err1 != nil {
						stdout("invalid extent id", err1)
						return
					} else {
						extentIDs[eid] = true
					}
				}
				extentsMap[args[2]] = make(map[uint64]map[uint64]bool, 0)
				extentsMap[args[2]][partitionID] = extentIDs
			}
			for host, dpExtents := range extentsMap {
				for pid, extents := range dpExtents {
					exts := make([]uint64, 0)
					for k := range extents {
						exts = append(exts, k)
					}
					if len(exts) == 0 {
						continue
					}
					repairExtents(host, pid, exts)
				}
			}
			fmt.Println("extent repair finished")
		},
	}
	cmd.Flags().BoolVar(&fromFile, "from-file", false, "specify extents file name to repair, file name is repair_extents, format:`partitionID extentID host`")
	return cmd
}

func repairExtents(host string, partitionID uint64, extentIDs []uint64) {
	if partitionID < 0 || len(extentIDs) == 0 {
		return
	}
	dp, err := client.AdminAPI().GetDataPartition("", partitionID)
	if err != nil {
		return
	}
	var exist bool
	for _, h := range dp.Hosts {
		if h == host {
			exist = true
			break
		}
	}
	if !exist {
		err = fmt.Errorf("host[%v] not exist in hosts[%v]", host, dp.Hosts)
		return
	}
	dHost := fmt.Sprintf("%v:%v", strings.Split(host, ":")[0], client.DataNodeProfPort)
	dataClient := http_client.NewDataClient(dHost, false)
	partition, err := dataClient.GetPartitionFromNode(partitionID)
	if err != nil {
		fmt.Printf("repair failed: %v %v %v\n", partitionID, extentIDs, host)
		return
	}
	partitionPath := fmt.Sprintf("datapartition_%v_%v", partitionID, dp.Replicas[0].Total)
	if len(extentIDs) == 1 {
		err = dataClient.RepairExtent(extentIDs[0], partition.Path, partitionID)
		if err != nil {
			fmt.Printf("repair failed: %v %v %v %v\n", partitionID, extentIDs[0], host, partition.Path)
			if _, err = dataClient.GetPartitionFromNode(partitionID); err == nil {
				return
			}
			for i := 0; i < 3; i++ {
				if err = dataClient.ReLoadPartition(partitionPath, strings.Split(partition.Path, "/datapartition")[0]); err == nil {
					break
				}
			}
			return
		}
		fmt.Printf("repair success: %v %v %v %v\n", partitionID, extentIDs[0], host, partition.Path)
	} else {
		var extMap map[uint64]string
		extentsStrs := make([]string, 0)
		for _, e := range extentIDs {
			extentsStrs = append(extentsStrs, strconv.FormatUint(e, 10))
		}
		extMap, err = dataClient.RepairExtentBatch(strings.Join(extentsStrs, "-"), partition.Path, partitionID)
		if err != nil {
			fmt.Printf("repair failed: %v %v %v %v\n", partitionID, extentsStrs, host, partition.Path)
			if _, err = dataClient.GetPartitionFromNode(partitionID); err == nil {
				return
			}
			for i := 0; i < 3; i++ {
				if err = dataClient.ReLoadPartition(partitionPath, strings.Split(partition.Path, "/datapartition")[0]); err == nil {
					break
				}
			}
			return
		}
		fmt.Printf("repair success: %v %v %v %v\n", partitionID, extentsStrs, host, partition.Path)
		fmt.Printf("repair result: %v\n", extMap)
	}
}

func newExtentCheckCmd(checkType int) *cobra.Command {
	var (
		use              string
		short            string
		specifyPath      string
		inodeStr         string
		tinyOnly         bool
		tinyInUse        bool
		concurrency      uint64
		modifyTimeMin    string
		modifyTimeMax    string
		profPort         uint64
		fromFile         bool
		volFilter        string
		volExcludeFilter string
	)
	if checkType == data_check.CheckTypeExtentCrc {
		use = cmdCheckReplicaCrcUse
		short = cmdCheckReplicaCrcShort
	} else if checkType == data_check.CheckTypeInodeEkNum {
		use = cmdCheckEkNumUse
		short = cmdCheckEkNumShort
	} else if checkType == data_check.CheckTypeInodeNlink {
		use = cmdCheckNlinkUse
		short = cmdCheckNlinkShort
	}

	var cmd = &cobra.Command{
		Use:   use,
		Short: short,
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err           error
				checkEngine   *data_check.CheckEngine
				specifyInodes []uint64
			)
			if len(inodeStr) > 0 {
				inodeSlice := strings.Split(inodeStr, ",")
				for _, inode := range inodeSlice {
					ino, err1 := strconv.Atoi(inode)
					if err1 != nil {
						continue
					}
					specifyInodes = append(specifyInodes, uint64(ino))
				}
			}
			ids := util.LoadSpecifiedPartitions()
			vols := make([]string, 0)
			vols = append(vols, args[0])
			if fromFile {
				vols = util.LoadSpecifiedVolumes(volFilter, volExcludeFilter)
			}
			outputDir, _ := os.Getwd()
			checkEngine, err = data_check.NewCheckEngine(
				outputDir,
				client,
				tinyOnly,
				tinyInUse,
				concurrency,
				data_check.CheckTypeExtentCrc,
				modifyTimeMin,
				modifyTimeMax,
				specifyInodes,
				ids,
				specifyPath,
				func() bool {
					return false
				})
			if err != nil {
				return
			}
			defer checkEngine.Close()
			checkEngine.CheckVols(vols)
			fmt.Printf("results saved in local file")
			return
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validVols(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}

	cmd.Flags().StringVar(&specifyPath, "path", "", "path")
	cmd.Flags().StringVar(&inodeStr, "inode", "", "comma separated inodes")
	cmd.Flags().BoolVar(&tinyOnly, "tinyOnly", false, "check tiny extents only")
	cmd.Flags().BoolVar(&tinyInUse, "tinyInUse", false, "check tiny extents in use")
	cmd.Flags().Uint64Var(&concurrency, "concurrency", 1, "max concurrent checking meta partitions & inode & extent")
	cmd.Flags().StringVar(&modifyTimeMin, "modifyTimeMin", "", "min modify time for inode")
	cmd.Flags().StringVar(&modifyTimeMax, "modifyTimeMax", "", "max modify time for inode")
	cmd.Flags().Uint64Var(&profPort, "profPort", 6007, "go pprof port")
	cmd.Flags().BoolVar(&fromFile, "from-file", false, "repair vols from file[filename:vols]")
	cmd.Flags().StringVar(&volFilter, "vol-filter", "", "check volume by filter")
	cmd.Flags().StringVar(&volExcludeFilter, "vol-exclude-filter", "", "exclude volume by filter")
	return cmd
}

func newExtentSearchCmd() *cobra.Command {
	var (
		use          = cmdSearchExtentUse
		short        = cmdSearchExtentShort
		concurrency  uint64
		dpStr        string
		extentStr    string
		extentOffset uint
		size         uint
	)
	var cmd = &cobra.Command{
		Use:   use,
		Short: short,
		Run: func(cmd *cobra.Command, args []string) {
			var (
				dps     []uint64
				extents []uint64
			)
			if len(dpStr) > 0 {
				for _, v := range strings.Split(dpStr, ",") {
					dp, err := strconv.Atoi(v)
					if err != nil {
						continue
					}
					dps = append(dps, uint64(dp))
				}
			}
			if len(extentStr) > 0 {
				for _, v := range strings.Split(extentStr, ",") {
					extentRange := strings.Split(v, "-")
					if len(extentRange) == 2 {
						begin, err := strconv.Atoi(extentRange[0])
						if err != nil {
							continue
						}
						end, err := strconv.Atoi(extentRange[1])
						if err != nil {
							continue
						}
						for i := begin; i <= end; i++ {
							extents = append(extents, uint64(i))
						}
						continue
					}
					extent, err := strconv.Atoi(v)
					if err != nil {
						continue
					}
					extents = append(extents, uint64(extent))
				}
			}
			if len(dps) == 0 || (len(dps) > 1 && len(dps) != len(extents)) {
				stdout("invalid parameters.\n")
				return
			}
			searchExtent(dps, extents, extentOffset, size, concurrency)
			return
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validVols(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().StringVar(&dpStr, "dps", "", "comma separated data partitions")
	cmd.Flags().StringVar(&extentStr, "extents", "", "comma separated extents")
	cmd.Flags().UintVar(&extentOffset, "extentOffset", 0, "")
	cmd.Flags().UintVar(&size, "size", 0, "")
	cmd.Flags().Uint64Var(&concurrency, "concurrency", 1, "max concurrent searching inodes")
	return cmd
}

func searchExtent(dps []uint64, extents []uint64, extentOffset uint, size uint, concurrency uint64) {
	dataPartition, err := client.AdminAPI().GetDataPartition("", dps[0])
	if err != nil {
		return
	}
	vol := dataPartition.VolName
	mps, err := client.ClientAPI().GetMetaPartitions(vol)
	if err != nil {
		return
	}
	inodes, err := util_sdk.GetFileInodesByMp(mps, 0, concurrency, 0, 0, client.MetaNodeProfPort, true)
	extentMap := make(map[string]bool)
	var dp uint64
	for i := 0; i < len(extents); i++ {
		if len(dps) == 1 {
			dp = dps[0]
		} else {
			dp = dps[i]
		}
		extentMap[fmt.Sprintf("%d-%d", dp, extents[i])] = true
	}

	var wg sync.WaitGroup
	wg.Add(len(inodes))
	for i := 0; i < int(concurrency); i++ {
		go func(i int) {
			idx := 0
			for {
				if idx*int(concurrency)+i >= len(inodes) {
					break
				}
				inode := inodes[idx*int(concurrency)+i]
				mp := locateMpByInode(mps, inode)
				mtClient := meta.NewMetaHttpClient(fmt.Sprintf("%v:%v", strings.Split(mp.LeaderAddr, ":")[0], client.MetaNodeProfPort), false)
				extentsResp, err := mtClient.GetExtentsByInode(mp.PartitionID, inode)
				if err != nil {
					stdout("get extents error: %v, inode: %d\n", err, inode)
					wg.Done()
					idx++
					continue
				}
				for _, ek := range extentsResp.Extents {
					_, ok := extentMap[fmt.Sprintf("%d-%d", ek.PartitionId, ek.ExtentId)]
					if ok {
						if size == 0 ||
							(ek.ExtentOffset >= uint64(extentOffset) && ek.ExtentOffset < uint64(extentOffset+size)) ||
							(ek.ExtentOffset+uint64(ek.Size) >= uint64(extentOffset) && ek.ExtentOffset+uint64(ek.Size) < uint64(extentOffset+size)) {
							stdout("inode: %d, ek: %s\n", inode, ek)
						}
					}
				}
				wg.Done()
				idx++
			}
		}(i)
	}
	wg.Wait()
}

func locateMpByInode(mps []*proto.MetaPartitionView, inode uint64) *proto.MetaPartitionView {
	for _, mp := range mps {
		if inode >= mp.Start && inode < mp.End {
			return mp
		}
	}
	return nil
}

func newExtentGarbageCheckCmd() *cobra.Command {
	var (
		use              = cmdCheckGarbageUse
		short            = cmdCheckGarbageShort
		all              bool
		active           bool
		dir              string
		clean            bool
		dpConcurrency    uint64
		mpConcurrency    uint64
		inodeConcurrency uint64
	)
	var cmd = &cobra.Command{
		Use:   use,
		Short: short,
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				vol = args[0]
			)
			garbageCheck(vol, all, active, dir, clean, dpConcurrency, mpConcurrency, inodeConcurrency)
			return
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validVols(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().BoolVar(&all, "all", false, "Check all garbage extents (only for extents modified in last 7 days by default)")
	cmd.Flags().BoolVar(&active, "active", false, "Check garbage extents using active inodes of user file system (all inodes of metanode by default)")
	cmd.Flags().StringVar(&dir, "dir", ".", "Output file dir")
	cmd.Flags().BoolVar(&clean, "clean", false, "Clean garbage extents")
	cmd.Flags().Uint64Var(&dpConcurrency, "dpConcurrency", 1, "max concurrent checking data partitions")
	cmd.Flags().Uint64Var(&mpConcurrency, "mpConcurrency", 1, "max concurrent checking meta partitions")
	cmd.Flags().Uint64Var(&inodeConcurrency, "inodeConcurrency", 1, "max concurrent checking extents")
	return cmd
}

func newTinyExtentCheckHoleCmd() *cobra.Command {
	var (
		use        = cmdCheckTinyExtentHoleUse
		short      = cmdCheckTinyExtentHoleShort
		scanLimit  uint64
		volumeStr  string
		autoRepair bool
		dpid       uint64
	)
	var cmd = &cobra.Command{
		Use:   use,
		Short: short,
		Run: func(cmd *cobra.Command, args []string) {
			if scanLimit > 150 {
				stdout("scanLimit too high: %d\n", scanLimit)
				return
			}

			rServer := newRepairServer(autoRepair)
			log.LogInfof("fix tiny extent for master: %v", client.Leader())

			vols := util.LoadSpecifiedVolumes("", "")
			ids := util.LoadSpecifiedPartitions()

			if dpid > 0 {
				ids = []uint64{dpid}
			}
			if volumeStr != "" {
				vols = []string{volumeStr}
			}

			log.LogInfo("check start")
			rServer.start()
			defer rServer.stop()

			rangeAllDataPartitions(scanLimit, vols, ids, func(vol *proto.SimpleVolView) {
				rServer.holeNumFd.Sync()
				rServer.holeSizeFd.Sync()
				rServer.availSizeFd.Sync()
				rServer.failedGetExtFd.Sync()
			}, rServer.checkAndRepairTinyExtents)
			log.LogInfo("check end")
			return
		},
	}
	cmd.Flags().Uint64Var(&scanLimit, "limit", 10, "limit rate")
	cmd.Flags().StringVar(&volumeStr, "volume", "", "fix by volume name")
	cmd.Flags().Uint64Var(&dpid, "partition", 0, "fix by data partition id")
	cmd.Flags().BoolVar(&autoRepair, "auto-repair", false, "true:scan bad tiny extent and send repair cmd to datanode automatically; false:only scan and record result, do not repair it")
	return cmd
}

func garbageCheck(vol string, all bool, active bool, dir string, clean bool, dpConcurrency uint64, mpConcurrency uint64, inodeConcurrency uint64) {
	var (
		// map[dp][extent]size
		dataExtentMap = make(map[uint64]map[uint64]uint64)
		view          *proto.DataPartitionsView
		err           error
		wg            sync.WaitGroup
		ch            = make(chan uint64, 1000)
		mu            sync.Mutex
	)
	// get all extents from datanode, MUST get extents from datanode first in case of newly added extents being deleted
	view, err = client.ClientAPI().GetDataPartitions(vol)
	if err != nil {
		stdout("get data partitions error: %v\n", err)
		return
	}
	year, month, day := time.Now().Date()
	today := time.Date(year, month, day, 0, 0, 0, 0, time.Local)
	wg.Add(len(view.DataPartitions))
	go func() {
		for _, dp := range view.DataPartitions {
			ch <- dp.PartitionID
		}
		close(ch)
	}()
	for i := 0; i < int(dpConcurrency); i++ {
		go func() {
			var dpInfo *util_sdk.DataPartition
			for dp := range ch {
				dpInfo, err = util_sdk.GetExtentsByDp(client, dp, "")
				if err != nil {
					stdout("get extents error: %v, dp: %d\n", err, dp)
					os.Exit(0)
				}
				mu.Lock()
				_, ok := dataExtentMap[dp]
				if !ok {
					dataExtentMap[dp] = make(map[uint64]uint64)
				}
				for _, extent := range dpInfo.Files {
					if (all || today.Unix()-int64(extent[storage.ModifyTime]) >= 604800) && extent[storage.FileID] > storage.MinNormalExtentID {
						dataExtentMap[dp][extent[storage.FileID]] = extent[storage.Size]
					}
				}
				mu.Unlock()
				wg.Done()
			}
			dpInfo = nil
		}()
	}
	wg.Wait()
	view = nil

	// get all extents from metanode
	var inodes []uint64
	mps, err := client.ClientAPI().GetMetaPartitions(vol)
	if err != nil {
		return
	}
	if active {
		inodes, err = util_sdk.GetAllInodesByPath(client.Nodes(), vol, "")
	} else {
		inodes, err = util_sdk.GetFileInodesByMp(mps, 0, mpConcurrency, 0, 0, client.MetaNodeProfPort, true)
	}
	if err != nil {
		stdout("get all inodes error: %v\n", err)
		return
	}

	metaExtentMap := make(map[uint64]map[uint64]bool)
	extents, err := getExtentsByInodes(inodes, inodeConcurrency, mps, client)
	inodes, mps = nil, nil
	if err != nil {
		stdout("get extents error: %v\n", err)
		return
	}
	for _, ek := range extents {
		_, ok := metaExtentMap[ek.PartitionId]
		if !ok {
			metaExtentMap[ek.PartitionId] = make(map[uint64]bool)
		}
		metaExtentMap[ek.PartitionId][ek.ExtentId] = true
	}
	extents = nil

	garbage := make(map[uint64][]uint64)
	var total uint64
	for dp := range dataExtentMap {
		for extent, size := range dataExtentMap[dp] {
			_, ok := metaExtentMap[dp]
			if ok {
				_, ok = metaExtentMap[dp][extent]
			}
			if !ok {
				garbage[dp] = append(garbage[dp], extent)
				total += size
			}
		}
	}

	stdout("garbageCheck, vol: %s, garbage size: %d\n", vol, total)
	os.Mkdir(fmt.Sprintf("%s/%s", dir, vol), os.ModePerm)
	for dp := range garbage {
		sort.Slice(garbage[dp], func(i, j int) bool { return garbage[dp][i] < garbage[dp][j] })
		strSlice := make([]string, len(garbage[dp]))
		for i, extent := range garbage[dp] {
			strSlice[i] = fmt.Sprintf("%d", extent)
		}
		ioutil.WriteFile(fmt.Sprintf("%s/%s/%d", dir, vol, dp), []byte(strings.Join(strSlice, "\n")), 0666)
		if clean {
			batchDeleteExtent(dp, garbage[dp])
		}
	}
}

func batchDeleteExtent(partitionId uint64, extents []uint64) (err error) {
	if len(extents) == 0 {
		return
	}
	stdout("start delete extent, partitionId: %d, extents len: %d\n", partitionId, len(extents))
	partition, err := client.AdminAPI().GetDataPartition("", partitionId)
	if err != nil {
		stdout("GetDataPartition error: %v, PartitionId: %v\n", err, partitionId)
		return
	}
	var gConnPool = connpool.NewConnectPool()
	conn, err := gConnPool.GetConnect(partition.Hosts[0])
	defer func() {
		if err != nil {
			gConnPool.PutConnect(conn, true)
		} else {
			gConnPool.PutConnect(conn, false)
		}
	}()

	if err != nil {
		stdout("get conn from pool error: %v, partitionId: %d\n", err, partitionId)
		return
	}
	dp := &metanode.DataPartition{
		PartitionID: partitionId,
		Hosts:       partition.Hosts,
	}
	eks := make([]*proto.MetaDelExtentKey, len(extents))
	for i := 0; i < len(extents); i++ {
		eks[i] = &proto.MetaDelExtentKey{
			ExtentKey: proto.ExtentKey{
				PartitionId:  partitionId,
				ExtentId:     extents[i],
			},
		}
	}
	packet := metanode.NewPacketToBatchDeleteExtent(context.Background(), dp, eks)
	if err = packet.WriteToConn(conn, proto.WriteDeadlineTime); err != nil {
		stdout("write to dataNode error: %v, logId: %s\n", err, packet.GetUniqueLogId())
		return
	}
	if err = packet.ReadFromConn(conn, proto.ReadDeadlineTime*10); err != nil {
		stdout("read response from dataNode error: %s, logId: %s\n", err, packet.GetUniqueLogId())
		return
	}
	if packet.ResultCode != proto.OpOk {
		stdout("batch delete extent response: %s, logId: %s\n", packet.GetResultMsg(), packet.GetUniqueLogId())
	}
	stdout("finish delete extent, partitionId: %d, extents len: %v\n", partitionId, len(extents))
	return
}

func getExtentsByInodes(inodes []uint64, concurrency uint64, mps []*proto.MetaPartitionView, c *sdk.MasterClient) (extents []proto.ExtentKey, err error) {
	var wg sync.WaitGroup
	inoCh := make(chan uint64, 1024)
	wg.Add(len(inodes))
	go func() {
		for _, ino := range inodes {
			inoCh <- ino
		}
		close(inoCh)
	}()

	resultCh := make(chan *proto.GetExtentsResponse, 1024)
	for i := 0; i < int(concurrency); i++ {
		go func() {
			for ino := range inoCh {
				mp := locateMpByInode(mps, ino)
				mtClient := meta.NewMetaHttpClient(fmt.Sprintf("%v:%v", strings.Split(mp.LeaderAddr, ":")[0], client.MetaNodeProfPort), false)
				re, tmpErr := mtClient.GetExtentsByInode(mp.PartitionID, ino)
				if tmpErr != nil {
					err = fmt.Errorf("get extents from inode err: %v, inode: %d", tmpErr, ino)
					resultCh <- nil
				} else {
					resultCh <- re
				}
				wg.Done()
			}
		}()
	}

	var wgResult sync.WaitGroup
	wgResult.Add(len(inodes))
	go func() {
		for re := range resultCh {
			if re == nil {
				wgResult.Done()
				continue
			}
			extents = append(extents, re.Extents...)
			wgResult.Done()
		}
	}()
	wg.Wait()
	close(resultCh)
	wgResult.Wait()
	if err != nil {
		extents = extents[:0]
	}
	return
}

func parseResp(resp []byte) (data []byte, err error) {
	var body = &struct {
		Code int32           `json:"code"`
		Msg  string          `json:"msg"`
		Data json.RawMessage `json:"data"`
	}{}
	if err = json.Unmarshal(resp, &body); err != nil {
		return
	}
	data = body.Data
	return
}

func getExtentInfo(dp *proto.DataPartitionInfo, client *sdk.MasterClient) (extentInfo []uint64, err error) {
	var (
		dnPartition   *proto.DNDataPartitionInfo
		extentInfoMap = make(map[uint64]bool)
		errCount      int
		errMsg        error
	)

	for _, host := range dp.Hosts {
		if errCount > len(dp.Hosts)/2 {
			break
		}
		arr := strings.Split(host, ":")
		if dnPartition, err = client.NodeAPI().DataNodeGetPartition(arr[0], dp.PartitionID); err != nil {
			errMsg = fmt.Errorf("%v DataNodeGetPartition err(%v) ", host, err)
			errCount++
			continue
		}

		for _, ei := range dnPartition.Files {
			if ei[storage.Size] == 0 {
				continue
			}
			_, ok := extentInfoMap[ei[storage.FileID]]
			if ok {
				continue
			}
			extentInfoMap[ei[storage.FileID]] = true
			extentInfo = append(extentInfo, ei[storage.FileID])
		}
	}
	if errCount > len(dp.Hosts)/2 {
		err = errMsg
	} else {
		err = nil
		sort.Slice(extentInfo, func(i, j int) bool {
			return extentInfo[i] < extentInfo[j]
		})
	}
	return
}
func newExtentCheckByIdCmd(mc *sdk.MasterClient) *cobra.Command {
	var partitionID uint64
	var extentID uint64
	var checkTiny bool
	var cmd = &cobra.Command{
		Use:   CliOpCheck,
		Short: cmdCheckExtentReplicaShort,
		Run: func(cmd *cobra.Command, args []string) {
			if partitionID == 0 || extentID == 0 {
				stdout("invalid id, pid[%d], extent id[%d]\n", partitionID, extentID)
				return
			}

			dpInfo, err := mc.AdminAPI().GetDataPartition("", partitionID)
			if err != nil {
				stdout("get data partitin[%d] failed :%v\n", partitionID, err.Error())
				return
			}

			dpAddr := fmt.Sprintf("%s:%d", strings.Split(dpInfo.Hosts[0], ":")[0], mc.DataNodeProfPort)
			dataClient := http_client.NewDataClient(dpAddr, false)
			ekInfo, _ := dataClient.GetExtentInfo(partitionID, extentID)
			ek := proto.ExtentKey{
				PartitionId: partitionID, ExtentId: extentID, Size: uint32(ekInfo[proto.ExtentInfoSize]),
			}
			data_check.CheckExtentReplicaInfo(mc.Nodes()[0], mc.DataNodeProfPort, dpInfo.Replicas, &ek, 0, dpInfo.VolName, nil, checkTiny)
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validDataNodes(mc, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().Uint64Var(&partitionID, "pid", 0, "Specify partition id")
	cmd.Flags().Uint64Var(&extentID, "eid", 0, "Specify extent id")
	cmd.Flags().BoolVar(&checkTiny, "check-tiny", false, "check tiny extent")
	return cmd
}

func parseMetaExtentDelFiles(dir string) {
	allFiles, _ := ioutil.ReadDir(dir)
	for _, file := range allFiles {
		if file.Mode().IsDir() || (!strings.Contains(file.Name(), "deleteExtentList") && !strings.Contains(file.Name(), "inodeDeleteExtentList")) {
			continue
		}
		fmt.Printf("parse file [%s] begin \n", path.Join(dir, file.Name()))

		fp, err := os.Open(path.Join(dir, file.Name()))
		if err != nil {
			fmt.Printf("parse file failed, open file [%s] failed:%v\n", path.Join(dir, file.Name()), err)
			continue
		}

		buffer, err := io.ReadAll(fp)
		if err != nil {
			fmt.Printf("parse file failed, read file [%s] failed:%v\n", path.Join(dir, file.Name()), err)
			fp.Close()
			continue
		}
		fp.Close()
		buff := bytes.NewBuffer(buffer)
		for {
			if buff.Len() == 0 {
				break
			}

			if buff.Len() < proto.ExtentDbKeyLengthWithIno {
				fmt.Printf("file parese failed, last ek len:%d < %d\n", buff.Len(), proto.ExtentDbKeyLengthWithIno)
				break
			}

			ek := proto.MetaDelExtentKey{}
			if err := ek.UnmarshalDbKeyByBuffer(buff); err != nil {
				fmt.Printf("parese failed:%v\n", err)
				break
			}
			fmt.Printf("%v\n", ek.String())
		}
		fmt.Printf("parse file [%s] success\n", path.Join(dir, file.Name()))
	}
}

func parseDataExtentDelFiles(dir string) {
	allFiles, _ := ioutil.ReadDir(dir)
	for _, file := range allFiles {
		if file.Mode().IsDir() || (!strings.Contains(file.Name(), "NORMALEXTENT_DELETE")) {
			continue
		}
		fmt.Printf("parse file [%s] begin \n", path.Join(dir, file.Name()))

		fp, err := os.Open(path.Join(dir, file.Name()))
		if err != nil {
			fmt.Printf("parse file failed, open file [%s] failed\n", path.Join(dir, file.Name()))
			continue
		}

		buffer := make([]byte, 8*1024)
		for {
			realLen, err := fp.Read(buffer)
			if err != nil {
				if err == io.EOF {
					break
				}

				fmt.Printf("parse file failed, read file [%s] failed\n", path.Join(dir, file.Name()))
				break
			}

			for off := 0; off < realLen && off+8 < realLen; off += 8 {
				ekID := binary.BigEndian.Uint64(buffer[off : off+8])
				fmt.Printf("ek: %d\n", ekID)
			}

		}
		fp.Close()
		fmt.Printf("parse file [%s] success\n", path.Join(dir, file.Name()))
	}
}

func newExtentParseCmd() *cobra.Command {
	var srcDir string
	var decoder string
	var cmd = &cobra.Command{
		Use:   cmdExtentDelParse,
		Short: cmdExtentDelParseShort,
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					stdout(err.Error())
				}
			}()
			if decoder != "meta" && decoder != "data" {
				err = fmt.Errorf("invalid type param :%s", decoder)
				return
			}

			if decoder == "meta" {
				parseMetaExtentDelFiles(srcDir)
				return
			}

			if decoder == "data" {
				parseDataExtentDelFiles(srcDir)
				return
			}

			fmt.Printf("parser success")
		},
	}
	cmd.Flags().StringVar(&srcDir, "dir", ".", "src file")
	cmd.Flags().StringVar(&decoder, "type", "meta", "meta/data")
	return cmd
}