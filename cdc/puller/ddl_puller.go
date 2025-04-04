// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package puller

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/entry/schema"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/cdc/kv/sharedconn"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/puller/memorysorter"
	"github.com/pingcap/tiflow/engine/pkg/clock"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/ddl"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/pingcap/tiflow/pkg/txnutil"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	ddlPullerStuckWarnDuration = 30 * time.Second
	// ddl puller should never filter any DDL jobs even if
	// the changefeed is in BDR mode, because the DDL jobs should
	// be filtered before they are sent to the sink
	ddlPullerFilterLoop = false
)

// DDLJobPuller is used to pull ddl job from TiKV.
// It's used by processor and ddlPullerImpl.
type DDLJobPuller interface {
	util.Runnable

	// Output the DDL job entry, it contains the DDL job and the error.
	Output() <-chan *model.DDLJobEntry
}

// Note: All unexported methods of `ddlJobPullerImpl` should
// be called in the same one goroutine.
type ddlJobPullerImpl struct {
	changefeedID model.ChangeFeedID
	mp           *MultiplexingPuller
	// memorysorter is used to sort the DDL events.
	sorter        *memorysorter.EntrySorter
	kvStorage     tidbkv.Storage
	schemaStorage entry.SchemaStorage
	resolvedTs    uint64
	filter        filter.Filter
	// ddlJobsTable is initialized when receive the first concurrent DDL job.
	// It holds the info of table `tidb_ddl_jobs` of upstream TiDB.
	ddlJobsTable *model.TableInfo
	// It holds the column id of `job_meta` in table `tidb_ddl_jobs`.
	jobMetaColumnID int64
	// outputCh sends the DDL job entries to the caller.
	outputCh chan *model.DDLJobEntry
}

// NewDDLJobPuller creates a new NewDDLJobPuller,
// which fetches ddl events starting from checkpointTs.
func NewDDLJobPuller(
	up *upstream.Upstream,
	checkpointTs uint64,
	cfg *config.ServerConfig,
	changefeed model.ChangeFeedID,
	schemaStorage entry.SchemaStorage,
	filter filter.Filter,
) DDLJobPuller {
	pdCli := up.PDClient
	regionCache := up.RegionCache
	kvStorage := up.KVStorage
	pdClock := up.PDClock

	ddlSpans := spanz.GetAllDDLSpan()
	for i := range ddlSpans {
		// NOTE(qupeng): It's better to use different table id when use sharedKvClient.
		ddlSpans[i].TableID = int64(-1) - int64(i)
	}

	ddlJobPuller := &ddlJobPullerImpl{
		changefeedID:  changefeed,
		schemaStorage: schemaStorage,
		kvStorage:     kvStorage,
		filter:        filter,
		outputCh:      make(chan *model.DDLJobEntry, defaultPullerOutputChanSize),
	}
	ddlJobPuller.sorter = memorysorter.NewEntrySorter(changefeed)

	grpcPool := sharedconn.NewConnAndClientPool(up.SecurityConfig, kv.GetGlobalGrpcMetrics())
	client := kv.NewSharedClient(
		changefeed, cfg, ddlPullerFilterLoop,
		pdCli, grpcPool, regionCache, pdClock,
		txnutil.NewLockerResolver(kvStorage.(tikv.Storage), changefeed),
	)

	slots, hasher := 1, func(tablepb.Span, int) int { return 0 }
	ddlJobPuller.mp = NewMultiplexingPuller(changefeed, client, up.PDClock, ddlJobPuller.Input, slots, hasher, 1)
	ddlJobPuller.mp.Subscribe(ddlSpans, checkpointTs, memorysorter.DDLPullerTableName, func(_ *model.RawKVEntry) bool { return false })

	return ddlJobPuller
}

// Run implements util.Runnable.
func (p *ddlJobPullerImpl) Run(ctx context.Context, _ ...chan<- error) error {
	eg, ctx := errgroup.WithContext(ctx)

	// Only nil in unit test.
	if p.mp != nil {
		eg.Go(func() error {
			return p.mp.Run(ctx)
		})
	}

	eg.Go(func() error {
		return p.sorter.Run(ctx)
	})

	eg.Go(func() error {
		for {
			var sortedDDLEvent *model.PolymorphicEvent
			select {
			case <-ctx.Done():
				return ctx.Err()
			case sortedDDLEvent = <-p.sorter.Output():
			}
			if err := p.handleRawKVEntry(ctx, sortedDDLEvent.RawKV); err != nil {
				return errors.Trace(err)
			}
		}
	})

	return eg.Wait()
}

// WaitForReady implements util.Runnable.
func (p *ddlJobPullerImpl) WaitForReady(_ context.Context) {}

// Close implements util.Runnable.
func (p *ddlJobPullerImpl) Close() {
	if p.mp != nil {
		p.mp.Close()
	}
}

// Output implements DDLJobPuller, it returns the output channel of DDL job.
func (p *ddlJobPullerImpl) Output() <-chan *model.DDLJobEntry {
	return p.outputCh
}

// Input receives the raw kv entry and put it into the input channel.
func (p *ddlJobPullerImpl) Input(
	ctx context.Context,
	rawDDL *model.RawKVEntry,
	_ []tablepb.Span,
	_ model.ShouldSplitKVEntry,
) error {
	p.sorter.AddEntry(ctx, model.NewPolymorphicEvent(rawDDL))
	return nil
}

// handleRawKVEntry converts the raw kv entry to DDL job and sends it to the output channel.
func (p *ddlJobPullerImpl) handleRawKVEntry(ctx context.Context, ddlRawKV *model.RawKVEntry) error {
	if ddlRawKV == nil {
		return nil
	}

	if ddlRawKV.OpType == model.OpTypeResolved {
		// Only nil in unit test case.
		if p.schemaStorage != nil {
			p.schemaStorage.AdvanceResolvedTs(ddlRawKV.CRTs)
		}
		if ddlRawKV.CRTs > p.getResolvedTs() {
			p.setResolvedTs(ddlRawKV.CRTs)
		}
	}

	job, err := p.unmarshalDDL(ctx, ddlRawKV)
	if err != nil {
		return errors.Trace(err)
	}

	if job != nil {
		skip, err := p.handleJob(job)
		if err != nil {
			return err
		}
		if skip {
			return nil
		}
		log.Info("a new ddl job is received",
			zap.String("namespace", p.changefeedID.Namespace),
			zap.String("changefeed", p.changefeedID.ID),
			zap.String("schema", job.SchemaName),
			zap.String("table", job.TableName),
			zap.Uint64("startTs", job.StartTS),
			zap.Uint64("finishedTs", job.BinlogInfo.FinishedTS),
			zap.String("query", job.Query),
			zap.Any("job", job))
	}

	jobEntry := &model.DDLJobEntry{
		Job:    job,
		OpType: ddlRawKV.OpType,
		CRTs:   ddlRawKV.CRTs,
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case p.outputCh <- jobEntry:
	}
	return nil
}

func (p *ddlJobPullerImpl) unmarshalDDL(ctx context.Context, rawKV *model.RawKVEntry) (*timodel.Job, error) {
	if rawKV.OpType != model.OpTypePut {
		return nil, nil
	}
	if p.ddlJobsTable == nil && !entry.IsLegacyFormatJob(rawKV) {
		err := p.initJobTableMeta(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return entry.ParseDDLJob(p.ddlJobsTable, rawKV, p.jobMetaColumnID)
}

func (p *ddlJobPullerImpl) getResolvedTs() uint64 {
	return atomic.LoadUint64(&p.resolvedTs)
}

func (p *ddlJobPullerImpl) setResolvedTs(ts uint64) {
	atomic.StoreUint64(&p.resolvedTs, ts)
}

func (p *ddlJobPullerImpl) initJobTableMeta(ctx context.Context) error {
	version, err := p.kvStorage.CurrentVersion(tidbkv.GlobalTxnScope)
	if err != nil {
		return errors.Trace(err)
	}
	snap := kv.GetSnapshotMeta(p.kvStorage, version.Ver)

	dbInfos, err := snap.ListDatabases()
	if err != nil {
		return cerror.WrapError(cerror.ErrMetaListDatabases, err)
	}

	db, err := findDBByName(dbInfos, mysql.SystemDB)
	if err != nil {
		return errors.Trace(err)
	}

	tbls, err := snap.ListTables(ctx, db.ID)
	if err != nil {
		return errors.Trace(err)
	}
	tableInfo, err := findTableByName(tbls, "tidb_ddl_job")
	if err != nil {
		return errors.Trace(err)
	}

	col, err := findColumnByName(tableInfo.Columns, "job_meta")
	if err != nil {
		return errors.Trace(err)
	}

	p.ddlJobsTable = model.WrapTableInfo(db.ID, db.Name.L, 0, tableInfo)
	p.jobMetaColumnID = col.ID
	return nil
}

// handleJob determines whether to filter out the DDL job.
// If the DDL job is not filtered out, it will be applied to the schemaStorage
// and the job will be sent to the output channel.
func (p *ddlJobPullerImpl) handleJob(job *timodel.Job) (skip bool, err error) {
	// Only nil in test.
	if p.schemaStorage == nil {
		return false, nil
	}

	if job.BinlogInfo.FinishedTS <= p.getResolvedTs() ||
		job.BinlogInfo.SchemaVersion == 0 /* means the ddl is ignored in upstream */ {
		log.Info("ddl job finishedTs less than puller resolvedTs,"+
			"discard the ddl job",
			zap.String("namespace", p.changefeedID.Namespace),
			zap.String("changefeed", p.changefeedID.ID),
			zap.String("schema", job.SchemaName),
			zap.String("table", job.TableName),
			zap.Uint64("startTs", job.StartTS),
			zap.Uint64("finishedTs", job.BinlogInfo.FinishedTS),
			zap.String("query", job.Query),
			zap.Uint64("pullerResolvedTs", p.getResolvedTs()))
		return true, nil
	}

	defer func() {
		if skip && err == nil {
			log.Info("ddl job schema or table does not match, discard it",
				zap.String("namespace", p.changefeedID.Namespace),
				zap.String("changefeed", p.changefeedID.ID),
				zap.String("schema", job.SchemaName),
				zap.String("table", job.TableName),
				zap.String("query", job.Query),
				zap.Uint64("startTs", job.StartTS),
				zap.Uint64("finishTs", job.BinlogInfo.FinishedTS))
		}
		if err != nil {
			log.Warn("handle ddl job failed",
				zap.String("namespace", p.changefeedID.Namespace),
				zap.String("changefeed", p.changefeedID.ID),
				zap.String("schema", job.SchemaName),
				zap.String("table", job.TableName),
				zap.String("query", job.Query),
				zap.Uint64("startTs", job.StartTS),
				zap.Uint64("finishTs", job.BinlogInfo.FinishedTS),
				zap.Error(err))
		}
	}()

	snap := p.schemaStorage.GetLastSnapshot()
	if err = snap.FillSchemaName(job); err != nil {
		log.Info("failed to fill schema name for ddl job",
			zap.String("namespace", p.changefeedID.Namespace),
			zap.String("changefeed", p.changefeedID.ID),
			zap.String("schema", job.SchemaName),
			zap.String("table", job.TableName),
			zap.String("query", job.Query),
			zap.Uint64("startTs", job.StartTS),
			zap.Uint64("finishTs", job.BinlogInfo.FinishedTS),
			zap.Error(err))
		if p.filter.ShouldDiscardDDL(job.Type, job.SchemaName, job.TableName) {
			return true, nil
		}
		return false, cerror.WrapError(cerror.ErrHandleDDLFailed,
			errors.Trace(err), job.Query, job.StartTS, job.StartTS)
	}

	switch job.Type {
	case timodel.ActionRenameTables:
		skip, err = p.handleRenameTables(job)
		if err != nil {
			log.Warn("handle rename tables ddl job failed",
				zap.String("namespace", p.changefeedID.Namespace),
				zap.String("changefeed", p.changefeedID.ID),
				zap.String("schema", job.SchemaName),
				zap.String("table", job.TableName),
				zap.String("query", job.Query),
				zap.Uint64("startTs", job.StartTS),
				zap.Uint64("finishTs", job.BinlogInfo.FinishedTS),
				zap.Error(err))
			return false, cerror.WrapError(cerror.ErrHandleDDLFailed,
				errors.Trace(err), job.Query, job.StartTS, job.StartTS)
		}
	case timodel.ActionCreateTables:
		querys, err := ddl.SplitQueries(job.Query)
		if err != nil {
			return false, errors.Trace(err)
		}
		// we only use multiTableInfos and Querys when we generate job event
		// So if some table should be discard, we just need to delete the info from multiTableInfos and Querys
		if len(querys) != len(job.BinlogInfo.MultipleTableInfos) {
			log.Error("the number of queries in `Job.Query` is not equal to "+
				"the number of `TableInfo` in `Job.BinlogInfo.MultipleTableInfos`",
				zap.Int("numQueries", len(querys)),
				zap.Int("numTableInfos", len(job.BinlogInfo.MultipleTableInfos)),
				zap.String("Job.Query", job.Query),
				zap.Any("Job.BinlogInfo.MultipleTableInfos", job.BinlogInfo.MultipleTableInfos),
				zap.Error(cerror.ErrTiDBUnexpectedJobMeta.GenWithStackByArgs()))
			return false, cerror.ErrTiDBUnexpectedJobMeta.GenWithStackByArgs()
		}

		var newMultiTableInfos []*timodel.TableInfo
		var newQuerys []string

		multiTableInfos := job.BinlogInfo.MultipleTableInfos

		for index, tableInfo := range multiTableInfos {
			// judge each table whether need to be skip
			if p.filter.ShouldDiscardDDL(job.Type, job.SchemaName, tableInfo.Name.O) {
				continue
			}
			newMultiTableInfos = append(newMultiTableInfos, multiTableInfos[index])
			newQuerys = append(newQuerys, querys[index])
		}

		skip = len(newMultiTableInfos) == 0

		job.BinlogInfo.MultipleTableInfos = newMultiTableInfos
		job.Query = strings.Join(newQuerys, "")
	case timodel.ActionRenameTable:
		oldTable, ok := snap.PhysicalTableByID(job.TableID)
		if !ok {
			// 1. If we can not find the old table, and the new table name is in filter rule, return error.
			discard := p.filter.ShouldDiscardDDL(job.Type, job.SchemaName, job.BinlogInfo.TableInfo.Name.O)
			if !discard {
				return false, cerror.ErrSyncRenameTableFailed.GenWithStackByArgs(job.TableID, job.Query)
			}
			log.Warn("skip rename table ddl since cannot found the old table info",
				zap.String("namespace", p.changefeedID.Namespace),
				zap.String("changefeed", p.changefeedID.ID),
				zap.Int64("tableID", job.TableID),
				zap.Int64("newSchemaID", job.SchemaID),
				zap.String("newSchemaName", job.SchemaName),
				zap.String("oldTableName", job.BinlogInfo.TableInfo.Name.O),
				zap.String("newTableName", job.TableName))
			return true, nil
		}
		// since we can find the old table, it must be able to find the old schema.
		// 2. If we can find the preTableInfo, we filter it by the old table name.
		skipByOldTableName := p.filter.ShouldDiscardDDL(job.Type, oldTable.TableName.Schema, oldTable.TableName.Table)
		skipByNewTableName := p.filter.ShouldDiscardDDL(job.Type, job.SchemaName, job.BinlogInfo.TableInfo.Name.O)
		if err != nil {
			return false, cerror.WrapError(cerror.ErrHandleDDLFailed,
				errors.Trace(err), job.Query, job.StartTS, job.StartTS)
		}
		// 3. If its old table name is not in filter rule, and its new table name in filter rule, return error.
		if skipByOldTableName {
			if !skipByNewTableName {
				return false, cerror.ErrSyncRenameTableFailed.GenWithStackByArgs(job.TableID, job.Query)
			}
			return true, nil
		}
		log.Info("ddl puller receive rename table ddl job",
			zap.String("namespace", p.changefeedID.Namespace),
			zap.String("changefeed", p.changefeedID.ID),
			zap.String("schema", job.SchemaName),
			zap.String("table", job.TableName),
			zap.String("query", job.Query),
			zap.Uint64("startTs", job.StartTS),
			zap.Uint64("finishedTs", job.BinlogInfo.FinishedTS))
	default:
		// nil means it is a schema ddl job, it's no need to fill the table name.
		if job.BinlogInfo.TableInfo != nil {
			job.TableName = job.BinlogInfo.TableInfo.Name.O
		}
		skip = p.filter.ShouldDiscardDDL(job.Type, job.SchemaName, job.TableName)
	}

	if skip {
		return true, nil
	}

	err = p.schemaStorage.HandleDDLJob(job)
	if err != nil {
		return false, cerror.WrapError(cerror.ErrHandleDDLFailed,
			errors.Trace(err), job.Query, job.StartTS, job.StartTS)
	}
	p.setResolvedTs(job.BinlogInfo.FinishedTS)

	return p.checkIneligibleTableDDL(snap, job)
}

// checkIneligibleTableDDL checks if the table is ineligible before and after the DDL.
//  1. If it is not a table DDL, we shouldn't check it.
//  2. If the table after the DDL is ineligible:
//     a. If the table is not exist before the DDL, we should ignore the DDL.
//     b. If the table is ineligible before the DDL, we should ignore the DDL.
//     c. If the table is eligible before the DDL, we should return an error.
func (p *ddlJobPullerImpl) checkIneligibleTableDDL(snapBefore *schema.Snapshot, job *timodel.Job) (skip bool, err error) {
	if filter.IsSchemaDDL(job.Type) {
		return false, nil
	}

	snapAfter := p.schemaStorage.GetLastSnapshot()

	if job.Type == timodel.ActionCreateTable {
		// For create table, oldTableID is the new table ID.
		isEligibleAfter := !snapAfter.IsIneligibleTableID(job.BinlogInfo.TableInfo.ID)
		if isEligibleAfter {
			return false, nil
		}
	}

	// For create tables, we always apply the DDL here.
	if job.Type == timodel.ActionCreateTables {
		return false, nil
	}

	oldTableID := job.TableID
	newTableID := job.BinlogInfo.TableInfo.ID

	// If the table is eligible after the DDL, we should apply the DDL.
	// No matter its status before the DDL.
	isEligibleAfter := !p.schemaStorage.GetLastSnapshot().IsIneligibleTableID(newTableID)
	if isEligibleAfter {
		return false, nil
	}

	// Steps here means this table is ineligible after the DDL.
	// We need to check if its status before the DDL.

	// 1. If the table is not in the snapshot before the DDL,
	// we should ignore the DDL.
	_, exist := snapBefore.PhysicalTableByID(oldTableID)
	if !exist {
		return true, nil
	}

	// 2. If the table is ineligible before the DDL, we should ignore the DDL.
	isIneligibleBefore := snapBefore.IsIneligibleTableID(oldTableID)
	if isIneligibleBefore {
		log.Warn("Ignore the DDL event of ineligible table",
			zap.String("changefeed", p.changefeedID.ID), zap.Any("ddl", job))
		return true, nil
	}

	// 3. If the table is eligible before the DDL, we should return an error.
	return false, cerror.New(fmt.Sprintf("An eligible table become ineligible after DDL: [%s] "+
		"it is a dangerous operation and may cause data loss. If you want to replicate this ddl safely, "+
		"pelase pause the changefeed and update the `force-replicate=true` "+
		"in the changefeed configuration, "+
		"then resume the changefeed.", job.Query))
}

// handleRenameTables gets all the tables that are renamed
// in the DDL job out and filter them one by one,
// if all the tables are filtered, skip it.
func (p *ddlJobPullerImpl) handleRenameTables(job *timodel.Job) (skip bool, err error) {
	var args *timodel.RenameTablesArgs
	args, err = timodel.GetRenameTablesArgs(job)
	if err != nil {
		return true, errors.Trace(err)
	}

	multiTableInfos := job.BinlogInfo.MultipleTableInfos
	if len(multiTableInfos) != len(args.RenameTableInfos) {
		return true, cerror.ErrInvalidDDLJob.GenWithStackByArgs(job.ID)
	}

	// we filter subordinate rename table ddl by these principles:
	// 1. old table name matches the filter rule, remain it.
	// 2. old table name does not match and new table name matches the filter rule, return error.
	// 3. old table name and new table name do not match the filter rule, skip it.
	remainTables := make([]*timodel.TableInfo, 0, len(multiTableInfos))
	snap := p.schemaStorage.GetLastSnapshot()

	argsForRemaining := &timodel.RenameTablesArgs{}
	for i, tableInfo := range multiTableInfos {
		info := args.RenameTableInfos[i]
		var shouldDiscardOldTable, shouldDiscardNewTable bool
		oldTable, ok := snap.PhysicalTableByID(tableInfo.ID)
		if !ok {
			shouldDiscardOldTable = true
		} else {
			shouldDiscardOldTable = p.filter.ShouldDiscardDDL(job.Type, info.OldSchemaName.O, oldTable.Name.O)
		}

		newSchemaName, ok := snap.SchemaByID(info.NewSchemaID)
		if !ok {
			// the new table name does not hit the filter rule, so we should discard the table.
			shouldDiscardNewTable = true
		} else {
			shouldDiscardNewTable = p.filter.ShouldDiscardDDL(job.Type, newSchemaName.Name.O, info.NewTableName.O)
		}

		if shouldDiscardOldTable && shouldDiscardNewTable {
			// skip a rename table ddl only when its old table name and new table name are both filtered.
			log.Info("RenameTables is filtered",
				zap.Int64("tableID", tableInfo.ID),
				zap.String("schema", info.OldSchemaName.O),
				zap.String("query", job.Query))
			continue
		}
		if shouldDiscardOldTable && !shouldDiscardNewTable {
			// if old table is not in filter rule and its new name is in filter rule, return error.
			return true, cerror.ErrSyncRenameTableFailed.GenWithStackByArgs(tableInfo.ID, job.Query)
		}
		// old table name matches the filter rule, remain it.
		argsForRemaining.RenameTableInfos = append(argsForRemaining.RenameTableInfos, &timodel.RenameTableArgs{
			OldSchemaID:   info.OldSchemaID,
			NewSchemaID:   info.NewSchemaID,
			TableID:       info.TableID,
			NewTableName:  info.NewTableName,
			OldSchemaName: info.OldSchemaName,
			OldTableName:  info.OldTableName,
		})
		remainTables = append(remainTables, tableInfo)
	}

	if len(remainTables) == 0 {
		return true, nil
	}

	bakJob, err := entry.GetNewJobWithArgs(job, argsForRemaining)
	if err != nil {
		return true, errors.Trace(err)
	}
	job.RawArgs = bakJob.RawArgs
	job.BinlogInfo.MultipleTableInfos = remainTables
	return false, nil
}

// DDLPuller is the interface for DDL Puller, used by owner only.
type DDLPuller interface {
	// Run runs the DDLPuller
	Run(ctx context.Context) error
	// PopFrontDDL returns and pops the first DDL job in the internal queue
	PopFrontDDL() (uint64, *timodel.Job)
	// ResolvedTs returns the resolved ts of the DDLPuller
	ResolvedTs() uint64
	// Close closes the DDLPuller
	Close()
}

type ddlPullerImpl struct {
	ddlJobPuller DDLJobPuller

	mu             sync.Mutex
	resolvedTS     uint64
	pendingDDLJobs []*timodel.Job
	lastDDLJobID   int64
	cancel         context.CancelFunc

	changefeedID model.ChangeFeedID
}

// NewDDLPuller return a puller for DDL Event
func NewDDLPuller(
	up *upstream.Upstream,
	startTs uint64,
	changefeed model.ChangeFeedID,
	schemaStorage entry.SchemaStorage,
	filter filter.Filter,
) DDLPuller {
	var puller DDLJobPuller
	// storage can be nil only in the test
	if up.KVStorage != nil {
		changefeed.ID += "_owner_ddl_puller"
		puller = NewDDLJobPuller(up, startTs, config.GetGlobalServerConfig(),
			changefeed, schemaStorage, filter)
	}

	return &ddlPullerImpl{
		ddlJobPuller: puller,
		resolvedTS:   startTs,
		cancel:       func() {},
		changefeedID: changefeed,
	}
}

func (h *ddlPullerImpl) addToPending(job *timodel.Job) {
	if job == nil {
		return
	}
	if job.ID == h.lastDDLJobID {
		log.Warn("ignore duplicated DDL job",
			zap.String("namespace", h.changefeedID.Namespace),
			zap.String("changefeed", h.changefeedID.ID),
			zap.String("schema", job.SchemaName),
			zap.String("table", job.TableName),

			zap.String("query", job.Query),
			zap.Uint64("startTs", job.StartTS),
			zap.Uint64("finishTs", job.BinlogInfo.FinishedTS),
			zap.Int64("jobID", job.ID))
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.pendingDDLJobs = append(h.pendingDDLJobs, job)
	h.lastDDLJobID = job.ID
	log.Info("ddl puller receives new pending job",
		zap.String("namespace", h.changefeedID.Namespace),
		zap.String("changefeed", h.changefeedID.ID),
		zap.String("schema", job.SchemaName),
		zap.String("table", job.TableName),
		zap.String("query", job.Query),
		zap.Uint64("startTs", job.StartTS),
		zap.Uint64("finishTs", job.BinlogInfo.FinishedTS),
		zap.Int64("jobID", job.ID))
}

// Run the ddl puller to receive DDL events
func (h *ddlPullerImpl) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	ctx, cancel := context.WithCancel(ctx)
	h.cancel = cancel

	g.Go(func() error { return h.ddlJobPuller.Run(ctx) })

	g.Go(func() error {
		cc := clock.New()
		ticker := cc.Ticker(ddlPullerStuckWarnDuration)
		defer ticker.Stop()
		lastResolvedTsAdvancedTime := cc.Now()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
				duration := cc.Since(lastResolvedTsAdvancedTime)
				if duration > ddlPullerStuckWarnDuration {
					log.Warn("ddl puller resolved ts has not advanced",
						zap.String("namespace", h.changefeedID.Namespace),
						zap.String("changefeed", h.changefeedID.ID),
						zap.Duration("duration", duration),
						zap.Uint64("resolvedTs", atomic.LoadUint64(&h.resolvedTS)))
				}
			case e := <-h.ddlJobPuller.Output():
				if e.OpType == model.OpTypeResolved {
					if e.CRTs > atomic.LoadUint64(&h.resolvedTS) {
						atomic.StoreUint64(&h.resolvedTS, e.CRTs)
						lastResolvedTsAdvancedTime = cc.Now()
						continue
					}
				}
				h.addToPending(e.Job)
			}
		}
	})

	log.Info("DDL puller started",
		zap.String("namespace", h.changefeedID.Namespace),
		zap.String("changefeed", h.changefeedID.ID),
		zap.Uint64("resolvedTS", atomic.LoadUint64(&h.resolvedTS)))

	defer func() {
		log.Info("DDL puller stopped",
			zap.String("namespace", h.changefeedID.Namespace),
			zap.String("changefeed", h.changefeedID.ID))
	}()

	return g.Wait()
}

// PopFrontDDL return the first pending DDL job and remove it from the pending list
func (h *ddlPullerImpl) PopFrontDDL() (uint64, *timodel.Job) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.pendingDDLJobs) == 0 {
		return atomic.LoadUint64(&h.resolvedTS), nil
	}
	job := h.pendingDDLJobs[0]
	h.pendingDDLJobs = h.pendingDDLJobs[1:]
	return job.BinlogInfo.FinishedTS, job
}

// Close the ddl puller, release all resources.
func (h *ddlPullerImpl) Close() {
	h.cancel()
	if h.ddlJobPuller != nil {
		h.ddlJobPuller.Close()
	}
	log.Info("DDL puller closed",
		zap.String("namespace", h.changefeedID.Namespace),
		zap.String("changefeed", h.changefeedID.ID))
}

func (h *ddlPullerImpl) ResolvedTs() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.pendingDDLJobs) == 0 {
		return atomic.LoadUint64(&h.resolvedTS)
	}
	job := h.pendingDDLJobs[0]
	return job.BinlogInfo.FinishedTS
}

// Below are some helper functions for ddl puller.
func findDBByName(dbs []*timodel.DBInfo, name string) (*timodel.DBInfo, error) {
	for _, db := range dbs {
		if db.Name.L == name {
			return db, nil
		}
	}
	return nil, cerror.WrapError(
		cerror.ErrDDLSchemaNotFound,
		errors.Errorf("can't find schema %s", name))
}

func findTableByName(tbls []*timodel.TableInfo, name string) (*timodel.TableInfo, error) {
	for _, t := range tbls {
		if t.Name.L == name {
			return t, nil
		}
	}
	return nil, cerror.WrapError(
		cerror.ErrDDLSchemaNotFound,
		errors.Errorf("can't find table %s", name))
}

func findColumnByName(cols []*timodel.ColumnInfo, name string) (*timodel.ColumnInfo, error) {
	for _, c := range cols {
		if c.Name.L == name {
			return c, nil
		}
	}
	return nil, cerror.WrapError(
		cerror.ErrDDLSchemaNotFound,
		errors.Errorf("can't find column %s", name))
}
