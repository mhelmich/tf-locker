/*
 * Copyright 2018 Marco Helmich
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package backend

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	// all go postgres driver
	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"
)

const (
	tableCreationQuery = `CREATE TABLE IF NOT EXISTS states
(
	state_id UUID NOT NULL,
	name VARCHAR(64) NOT NULL,
	version BIGINT NOT NULL DEFAULT 0,
	lock_info TEXT,
	blob TEXT NOT NULL,
	PRIMARY KEY (state_id, name, version)
)`

	upsertSelectForUpdateStr = "SELECT version, lock_info FROM states WHERE state_id = $1 AND name = $2 ORDER BY version DESC LIMIT 1 FOR UPDATE"
	upsertInsertStr          = "INSERT INTO states(state_id, name, version, lock_info, blob) VALUES($1, $2, $3, $4, $5)"
	getSelectStr             = "SELECT version, blob FROM states WHERE state_id = $1 AND name = $2 ORDER BY version DESC LIMIT 1"
	lockUpdateStr            = "UPDATE states SET lock_info = $1 WHERE state_id = $2 AND name = $3 AND version = $4"
)

var (
	timeout time.Duration = 5 * time.Second
)

type postgresStore struct {
	db *sql.DB
}

func NewPostgresStore(databaseUrl string) (*postgresStore, error) {
	db, err := connectToPostgres(databaseUrl)
	if err != nil {
		return nil, err
	}

	return &postgresStore{
		db: db,
	}, err
}

func connectToPostgres(databaseUrl string) (*sql.DB, error) {
	db, err := sql.Open("postgres", databaseUrl)
	if err != nil {
		logrus.Panicf("%s", err.Error())
	}

	err = ensureTableExists(db)
	if err != nil {
		logrus.Panicf("%s", err.Error())
	}

	return db, nil
}

func ensureTableExists(db *sql.DB) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, err := db.ExecContext(ctx, tableCreationQuery)
	return err
}

func (ps *postgresStore) UpsertState(stateID string, name string, lockInfo string, data []byte) error {
	txn, err := ps.db.Begin()
	if err != nil {
		return err
	}

	defer txn.Rollback()

	selectForUpdate, err := txn.Prepare(upsertSelectForUpdateStr)
	if err != nil {
		return err
	}

	defer selectForUpdate.Close()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var version int
	var queriedLockInfo sql.NullString
	err = selectForUpdate.QueryRowContext(ctx, stateID, name).Scan(&version, &queriedLockInfo)
	if err == sql.ErrNoRows {
		version = 0
	} else if err != nil {
		return err
	} else if !queriedLockInfo.Valid {
		logrus.Info("Queried lock id is nil")
	} else if queriedLockInfo.String != "" {
		// lockInfo is only the lock ID
		li := &LockInfo{}
		err = json.Unmarshal([]byte(queriedLockInfo.String), li)
		if err != nil {
			return err
		}

		if li.ID != lockInfo {
			return fmt.Errorf("Lock ids don't line up: want [%s] have [%s]", queriedLockInfo.String, lockInfo)
		}
	}

	insert, err := txn.Prepare(upsertInsertStr)
	if err != nil {
		return err
	}

	version++
	defer insert.Close()
	ctx, cancel = context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var res sql.Result
	if lockInfo == "" {
		res, err = insert.ExecContext(ctx, stateID, name, version, nil, data)
	} else {
		res, err = insert.ExecContext(ctx, stateID, name, version, queriedLockInfo.String, data)
	}
	if err != nil {
		return err
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return err
	} else if affected != int64(1) {
		return fmt.Errorf("Insert didn't work")
	}

	err = txn.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (ps *postgresStore) GetState(stateID string, name string) ([]byte, error) {
	txn, err := ps.db.Begin()
	if err != nil {
		return nil, err
	}

	defer txn.Rollback()

	selectStmt, err := txn.Prepare(getSelectStr)
	if err != nil {
		return nil, err
	}

	defer selectStmt.Close()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var bites []byte
	var version int
	err = selectStmt.QueryRowContext(ctx, stateID, name).Scan(&version, &bites)
	if err == sql.ErrNoRows {
		return make([]byte, 0), nil
	} else if err != nil {
		return nil, err
	}

	return bites, nil
}

func (ps *postgresStore) DeleteState(stateID string, name string) error {
	return ps.UpsertState(stateID, name, "", make([]byte, 0))
}

func (ps *postgresStore) LockState(stateID string, name string, lockInfo string) error {
	txn, err := ps.db.Begin()
	if err != nil {
		return err
	}

	defer txn.Rollback()

	selectForUpdate, err := txn.Prepare(upsertSelectForUpdateStr)
	if err != nil {
		return err
	}

	defer selectForUpdate.Close()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var version int
	var queriedLockInfo sql.NullString
	err = selectForUpdate.QueryRowContext(ctx, stateID, name).Scan(&version, &queriedLockInfo)
	if err == sql.ErrNoRows {
		version = 1

		var insert *sql.Stmt
		insert, err = txn.Prepare(upsertInsertStr)
		if err != nil {
			return err
		}

		defer insert.Close()
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
		defer cancel()
		var res sql.Result
		res, err = insert.ExecContext(ctx, stateID, name, version, lockInfo, make([]byte, 0))
		if err != nil {
			return err
		}

		var affected int64
		affected, err = res.RowsAffected()
		if err != nil {
			return err
		} else if affected != int64(1) {
			return fmt.Errorf("inserting didn't work")
		}

		queriedLockInfo.Valid = true
		queriedLockInfo.String = lockInfo

		err = txn.Commit()
		if err != nil {
			return err
		}

		txn, err = ps.db.Begin()
		if err != nil {
			return err
		}

		defer txn.Rollback()

		var selectForUpdate2 *sql.Stmt
		selectForUpdate2, err = txn.Prepare(upsertSelectForUpdateStr)
		if err != nil {
			return err
		}

		defer selectForUpdate2.Close()
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
		defer cancel()
		err = selectForUpdate2.QueryRowContext(ctx, stateID, name).Scan(&version, &queriedLockInfo)
		if err != nil {
			return err
		}
	} else if err != nil {
		return err
	}

	if queriedLockInfo.Valid && queriedLockInfo.String == lockInfo {
		return nil
	} else if queriedLockInfo.String != "" {
		return ErrAlreadyLocked
	}

	update, err := txn.Prepare(lockUpdateStr)
	if err != nil {
		return err
	}

	defer update.Close()
	ctx, cancel = context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var res sql.Result
	res, err = update.ExecContext(ctx, lockInfo, stateID, name, version)
	if err != nil {
		return err
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return err
	} else if affected != int64(1) {
		return fmt.Errorf("locking didn't work")
	}

	err = txn.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (ps *postgresStore) UnlockState(stateID string, name string, lockInfo string) error {
	txn, err := ps.db.Begin()
	if err != nil {
		return err
	}

	defer txn.Rollback()

	selectForUpdate, err := txn.Prepare(upsertSelectForUpdateStr)
	if err != nil {
		return err
	}

	defer selectForUpdate.Close()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var version int
	var queriedLockInfo sql.NullString
	err = selectForUpdate.QueryRowContext(ctx, stateID, name).Scan(&version, &queriedLockInfo)
	if err == sql.ErrNoRows {
		version = 0
	} else if err != nil {
		return err
	}

	if !queriedLockInfo.Valid || queriedLockInfo.String != lockInfo {
		return fmt.Errorf("Can't unlock [%s] [%s] because somebody else holds the lock: %s my lockinfo is: %s", name, stateID, queriedLockInfo.String, lockInfo)
	}

	update, err := txn.Prepare(lockUpdateStr)
	if err != nil {
		return err
	}

	defer update.Close()
	var res sql.Result
	ctx, cancel = context.WithTimeout(context.Background(), timeout)
	defer cancel()
	res, err = update.ExecContext(ctx, nil, stateID, name, version)
	if err != nil {
		return err
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return err
	} else if affected != int64(1) {
		return fmt.Errorf("locking didn't work")
	}

	err = txn.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (ps *postgresStore) Close() {
	ps.db.Close()
}
