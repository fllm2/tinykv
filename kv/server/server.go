package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	key := req.Key
	server.Latches.WaitForLatches([][]byte{key})
	defer server.Latches.ReleaseLatches([][]byte{key})

	resp := &kvrpcpb.GetResponse{}

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	txn := mvcc.NewMvccTxn(reader, req.Version)
	lock, err := txn.GetLock(key)
	if err != nil {
		return resp, err
	}

	if lock != nil && lock.Ts < txn.StartTS {
		resp.Error = &kvrpcpb.KeyError{
			Locked: &kvrpcpb.LockInfo{
				PrimaryLock: lock.Primary,
				LockVersion: lock.Ts,
				Key:         key,
				LockTtl:     lock.Ttl,
			}}
        return resp, nil  // Return early if locked
	}

	value, err := txn.GetValue(key)
	if err != nil {
		return resp, err
	}
	if value == nil {
		resp.NotFound = true
		return resp, nil // Return early if not found, avoid error return when not found
	}
	resp.Value = value

	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	keys := make([][]byte, len(req.Mutations))
	for i, mu := range req.Mutations {
		keys[i] = mu.Key
	}

	// Acquire latches for all keys.
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	resp := &kvrpcpb.PrewriteResponse{}

	// Create a reader for the storage.
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	// Check for write conflicts.
	for _, key := range keys {
		write, commitTs, err := txn.MostRecentWrite(key)
		if err != nil {
			return resp, err
		}
		if write != nil && commitTs > txn.StartTS {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    write.StartTS,
					ConflictTs: commitTs,
					Key:        key,
					Primary:    req.PrimaryLock,
				},
			})
			return resp, nil // Return immediately if a conflict is found.
		}
	}

	// Check for existing locks.
	for _, key := range keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return resp, err
		}
		if lock != nil {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
					Key:         key,
					LockTtl:     lock.Ttl,
				},
			})
			return resp, nil // Return immediately if a lock is found.
		}
	}

	// Apply mutations and create locks.
	for _, mu := range req.Mutations {
		var writeKind mvcc.WriteKind
		switch mu.Op {
		case kvrpcpb.Op_Put:
			txn.PutValue(mu.Key, mu.Value)
			writeKind = mvcc.WriteKindPut
		case kvrpcpb.Op_Del:
			txn.DeleteValue(mu.Key)
			writeKind = mvcc.WriteKindDelete
		case kvrpcpb.Op_Rollback:
			writeKind = mvcc.WriteKindRollback
		case kvrpcpb.Op_Lock:
			// Do nothing for lock op, lock will be handled after switch
		}

		// Create lock for the current key.
		txn.PutLock(mu.Key, &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      txn.StartTS,
			Ttl:     req.LockTtl,
			Kind:    writeKind,
		})
	}

	// Write all changes to storage.
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return resp, err
	}

	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	keys := req.Keys
    server.Latches.WaitForLatches(keys)
    defer server.Latches.ReleaseLatches(keys)

    // Initialize an empty commit response.
    resp := &kvrpcpb.CommitResponse{}

    // Create a storage reader.
    reader, err := server.storage.Reader(req.Context)
    if err != nil {
        return resp, err
    }
    // Create a new MVCC transaction.
    txn := mvcc.NewMvccTxn(reader, req.StartVersion)

    // Validate locks for all keys.
    for _, key := range keys {
        lock, err := txn.GetLock(key)
        if err != nil {
            return resp, err
        }

        if lock == nil {
            // If no lock exists, check if a rollback write exists for this start ts
            write, _, err := txn.CurrentWrite(key)
            if err != nil {
                return resp, err // Should this return an error? the previous code return nil
            }
            // If a rollback exists for this start ts, return retryable error.
            if write != nil && write.StartTS == txn.StartTS && write.Kind == mvcc.WriteKindRollback {
                resp.Error = &kvrpcpb.KeyError{
                    Retryable: "true",
                }
                return resp, nil
            }
            // No lock and no rollback, continue to the next key
            continue
        }
		// If the lock's timestamp doesn't match the transaction's start timestamp, return a retryable error.
        if lock.Ts != txn.StartTS {
            resp.Error = &kvrpcpb.KeyError{
                Retryable: "true",
            }
            return resp, nil
        }
    }

    // Commit the transaction for all keys.
    for _, key := range keys {
        lock, _ := txn.GetLock(key)
        if lock == nil {
            // Skip keys that don't have locks
            continue
        }
		// Record the commit write
        txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
            StartTS: txn.StartTS,
            Kind: lock.Kind,
        })
		// Delete the lock after committing
        txn.DeleteLock(key)
    }

	// Write the transaction changes to the underlying storage.
    err = server.storage.Write(req.Context, txn.Writes())
    if err != nil {
        return resp, err
    }

    // Return a successful response.
    return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ScanResponse{}

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err // Return error if reader creation fails.
	}

	txn := mvcc.NewMvccTxn(reader, req.Version)
	scanner := mvcc.NewScanner(req.StartKey, txn)

	var kvPairs []*kvrpcpb.KvPair
	for i := 0; i < int(req.Limit); i++ {
		if !scanner.Iter.Valid() {
			break // Exit loop if scanner is exhausted.
		}

		key, value, err := scanner.Next()
		if err != nil {
			return resp, err // Return error if scanner encounters an issue.
		}
		if key == nil {
			continue // Skip if key is nil.
		}

		lock, err := txn.GetLock(key)
		if err != nil {
			return resp, err // Return error if getting lock fails.
		}

		if lock != nil && lock.Ts < txn.StartTS {
			// Handle locked key scenario.
			kvPairs = append(kvPairs, &kvrpcpb.KvPair{
				Error: &kvrpcpb.KeyError{
					Locked: &kvrpcpb.LockInfo{
						PrimaryLock: lock.Primary,
						LockVersion: lock.Ts,
						Key:         key,
						LockTtl:     lock.Ttl,
					},
				},
			})
			continue // Skip adding key-value pair if locked.
		}


		if value != nil {
			// Add the key-value pair to results if valid and unlocked
			kvPairs = append(kvPairs, &kvrpcpb.KvPair{
				Key:   key,
				Value: value,
			})
		}
	}

	resp.Pairs = kvPairs
	return resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	response := &kvrpcpb.CheckTxnStatusResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return response, nil
	}
	txn := mvcc.NewMvccTxn(reader, req.LockTs)

	write, commitTs, err := txn.CurrentWrite(req.PrimaryKey)
	if err != nil {
		return response, err
	}
	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		return response, nil
	}

	// 如果不是回滚类型写入，说明已经被 commit，返回 commitTs
	if write != nil && write.Kind != mvcc.WriteKindRollback {
		response.CommitVersion = commitTs
		return response, nil
	}

	// 如果锁不存在
	if lock == nil {
		// 已经被回滚
		if write != nil && write.Kind == mvcc.WriteKindRollback {
			// rolled back: lock_ttl == 0 && commit_version == 0
			response.CommitVersion = 0
			response.LockTtl = 0
			response.Action = kvrpcpb.Action_NoAction
			return response, nil
		} else {
			// 回滚标记
			txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
				StartTS: req.LockTs,
				Kind:    mvcc.WriteKindRollback,
			})
			err := server.storage.Write(req.Context, txn.Writes())
			if err != nil {
				return response, err
			}
			response.Action = kvrpcpb.Action_LockNotExistRollback
			return response, nil
		}
	}

	// 锁超时，清除
	curTs := mvcc.PhysicalTime(req.CurrentTs)
	lockTs := mvcc.PhysicalTime(lock.Ts)
	if curTs > lockTs && curTs - lockTs >= lock.Ttl {
		txn.DeleteLock(req.PrimaryKey)
		txn.DeleteValue(req.PrimaryKey)
		// 回滚标记
		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		})
		err := server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			return response, err
		}
		response.Action = kvrpcpb.Action_TTLExpireRollback
	} else {
		// 直接返回，等锁自己超时
		response.Action = kvrpcpb.Action_NoAction
		return response, nil
	}
	return response, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.BatchRollbackResponse{}

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	keys := req.Keys

    // Pre-check if any key has already been committed.
    for _, key := range keys {
        write, _, err := txn.CurrentWrite(key)
        if err != nil {
            return resp, err
        }
        if write != nil && write.Kind != mvcc.WriteKindRollback {
            resp.Error = &kvrpcpb.KeyError{
                Abort: "true",
            }
            return resp, nil
        }
    }


	for _, key := range keys {
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return resp, err
		}

		lock, err := txn.GetLock(key)
        if err != nil{
            return resp,err
        }

        // If the lock belongs to a different transaction, mark the rollback and continue
		if lock != nil && lock.Ts != txn.StartTS {
			txn.PutWrite(key, txn.StartTS, &mvcc.Write{
				StartTS: txn.StartTS,
				Kind:    mvcc.WriteKindRollback,
			})
			continue
		}

		// Skip already rolled back keys.
		if write != nil && write.Kind == mvcc.WriteKindRollback {
			continue
		}
        
        // Perform the rollback operation: Delete lock, delete value and add rollback write.
		txn.DeleteLock(key)
		txn.DeleteValue(key)
		txn.PutWrite(key, txn.StartTS, &mvcc.Write{
			StartTS: txn.StartTS,
			Kind:    mvcc.WriteKindRollback,
		})
	}
   
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ResolveLockResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, nil
	}

	txn :=mvcc.NewMvccTxn(reader, req.StartVersion)
	iter := reader.IterCF(engine_util.CfLock)

	var keys [][]byte
	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		key := item.KeyCopy(nil)
		value, err := item.ValueCopy(nil)
		if err != nil {
			return resp, err
		}
		lock, err := mvcc.ParseLock(value)
		if err != nil {
			return resp, err
		}
		if lock.Ts == txn.StartTS {
			keys = append(keys, key)
		}
	}

	if req.CommitVersion == 0 {
		// rollback all
		rbReq :=  &kvrpcpb.BatchRollbackRequest{
			Keys: keys,
			StartVersion: txn.StartTS,
			Context: req.Context,
		}
		rbResp, err := server.KvBatchRollback(nil, rbReq)
		if err != nil {
			return resp, err
		}
		resp.Error = rbResp.Error
		resp.RegionError = rbResp.RegionError
		return resp, nil
	} else if req.CommitVersion > 0 {
		// commit those locks with the given commit timestamp
		cmReq := &kvrpcpb.CommitRequest{
			Keys: keys,
			StartVersion: txn.StartTS,
			CommitVersion: req.CommitVersion,
			Context: req.Context,
		}
		cmResp, err := server.KvCommit(nil, cmReq)
		if err != nil {
			return resp, err
		}
		resp.Error = cmResp.Error
		resp.RegionError = cmResp.RegionError
		return resp, nil
	}

	return resp, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
