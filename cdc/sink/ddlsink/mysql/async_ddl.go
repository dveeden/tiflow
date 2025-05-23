// Copyright 2024 PingCAP, Inc.
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

package mysql

import (
	"context"
	"fmt"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/dumpling/export"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

var checkRunningAddIndexSQL = `
SELECT JOB_ID, JOB_TYPE, SCHEMA_STATE, SCHEMA_ID, TABLE_ID, STATE, QUERY
FROM information_schema.ddl_jobs
WHERE DB_NAME = "%s"
    AND TABLE_NAME = "%s"
    AND JOB_TYPE LIKE "add index%%"
    AND (STATE = "running" OR STATE = "queueing");
`

func (m *DDLSink) needWaitAsyncExecDone(t timodel.ActionType) bool {
	if !m.cfg.IsTiDB {
		return false
	}
	switch t {
	case timodel.ActionCreateTable, timodel.ActionCreateTables:
		return false
	case timodel.ActionCreateSchema:
		return false
	default:
		return true
	}
}

// wait for the previous asynchronous DDL to finish before executing the next ddl.
func (m *DDLSink) waitAsynExecDone(ctx context.Context, ddl *model.DDLEvent) {
	if !m.needWaitAsyncExecDone(ddl.Type) {
		return
	}

	tables := make(map[model.TableName]struct{})
	if ddl.TableInfo != nil {
		tables[ddl.TableInfo.TableName] = struct{}{}
	}
	if ddl.PreTableInfo != nil {
		tables[ddl.PreTableInfo.TableName] = struct{}{}
	}

	log.Debug("Wait for the previous asynchronous DDL to finish",
		zap.String("namespace", m.id.Namespace),
		zap.String("changefeed", m.id.ID),
		zap.Any("tableInfo", ddl.TableInfo),
		zap.Any("preTableInfo", ddl.PreTableInfo),
		zap.Uint64("commitTs", ddl.CommitTs),
		zap.String("ddl", ddl.Query))
	if len(tables) == 0 || m.checkAsyncExecDDLDone(ctx, tables) {
		return
	}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			done := m.checkAsyncExecDDLDone(ctx, tables)
			if done {
				return
			}
		}
	}
}

func (m *DDLSink) checkAsyncExecDDLDone(ctx context.Context, tables map[model.TableName]struct{}) bool {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for table := range tables {
		done := m.doCheck(ctx, table)
		if !done {
			return false
		}
	}
	return true
}

func (m *DDLSink) doCheck(ctx context.Context, table model.TableName) (done bool) {
	start := time.Now()

	rows, err := m.db.QueryContext(ctx, fmt.Sprintf(checkRunningAddIndexSQL, table.Schema, table.Table))
	defer func() {
		if rows != nil {
			_ = rows.Err()
		}
	}()
	if err != nil {
		log.Error("check previous asynchronous ddl failed",
			zap.String("namespace", m.id.Namespace),
			zap.String("changefeed", m.id.ID),
			zap.Error(err))
		return true
	}
	rets, err := export.GetSpecifiedColumnValuesAndClose(rows, "JOB_ID", "JOB_TYPE", "SCHEMA_STATE", "STATE")
	if err != nil {
		log.Error("check previous asynchronous ddl failed",
			zap.String("namespace", m.id.Namespace),
			zap.String("changefeed", m.id.ID),
			zap.Error(err))
		return true
	}

	if len(rets) == 0 {
		return true
	}
	ret := rets[0]
	jobID, jobType, schemaState, state := ret[0], ret[1], ret[2], ret[3]
	log.Info("The previous asynchronous ddl is still running",
		zap.String("namespace", m.id.Namespace),
		zap.String("changefeed", m.id.ID),
		zap.Duration("checkDuration", time.Since(start)),
		zap.String("table", table.String()),
		zap.String("jobID", jobID),
		zap.String("jobType", jobType),
		zap.String("schemaState", schemaState),
		zap.String("state", state))
	return false
}
