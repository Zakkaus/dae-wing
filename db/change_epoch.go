/*
 * SPDX-License-Identifier: AGPL-3.0-only
 * Copyright (c) 2023, daeuniverse Organization <team@v2raya.org>
 */

package db

import "sync/atomic"

// changeEpoch counts node and subscription changes in this process. Group
// versions are only bumped for groups that are already running, so a change
// made while a reload is in flight to a group that is about to become running
// leaves no trace in the rows; Run compares the epoch across the reload
// instead and marks those groups modified.
var changeEpoch atomic.Uint64

// NoteNodeChange records a node or subscription change.
func NoteNodeChange() { changeEpoch.Add(1) }

// ChangeEpoch returns the current change counter.
func ChangeEpoch() uint64 { return changeEpoch.Load() }
