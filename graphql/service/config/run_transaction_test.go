package config

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/daeuniverse/dae-wing/dae"
	"github.com/daeuniverse/dae-wing/db"
	"github.com/daeuniverse/dae-wing/graphql/service/general"
	nodesvc "github.com/daeuniverse/dae-wing/graphql/service/node"
	"gorm.io/gorm"
)

func seedRun(t *testing.T) (context.Context, db.Group) {
	t.Helper()
	if err := db.InitDatabase(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	d := db.DB(ctx)
	sqlDB, err := d.DB()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sqlDB.Close() })
	group := db.Group{Name: "proxy", Policy: "random", Node: []db.Node{{Name: "one", Link: "socks5://127.0.0.1:1080"}}}
	for _, model := range []interface{}{
		&db.Config{Name: "config", Global: "global {}", Selected: true, Version: 2},
		&db.Dns{Name: "dns", Dns: "dns {}", Selected: true, Version: 3},
		&db.Routing{Name: "routing", Routing: "routing { fallback: proxy }", Selected: true, Version: 4},
		&group,
		&db.System{},
	} {
		if err := d.Create(model).Error; err != nil {
			t.Fatal(err)
		}
	}
	return ctx, group
}

func TestRunReleasesTransactionDuringReload(t *testing.T) {
	ctx, group := seedRun(t)
	oldReload := dae.ChReloadConfigs
	dae.ChReloadConfigs = make(chan *dae.ReloadMessage)
	defer func() { dae.ChReloadConfigs = oldReload }()
	runDone := make(chan struct{})
	var runErr error
	go func() {
		_, runErr = Run(ctx, false)
		close(runDone)
	}()
	var message *dae.ReloadMessage
	select {
	case message = <-dae.ChReloadConfigs:
	case <-runDone:
		t.Fatalf("Run before reload: %v", runErr)
	}
	writeDone := make(chan error, 1)
	defer func() {
		close(message.Callback)
		<-runDone
		if err := <-writeDone; err != nil {
			t.Errorf("concurrent commit: %v", err)
		}
		if runErr != nil {
			t.Errorf("Run: %v", runErr)
			return
		}
		var sys db.System
		if err := db.DB(ctx).Preload("RunningGroups").First(&sys).Error; err != nil || !sys.Running {
			t.Fatalf("running state: %+v, err=%v", sys, err)
		}
		if len(sys.RunningGroups) != 1 || sys.RunningGroups[0].ID != group.ID ||
			sys.RunningGroups[0].Name != "renamed" || sys.RunningGroupVersionSum != group.Version {
			t.Errorf("running snapshot overwrote concurrent group change: %+v", sys)
		}
		modified, err := (&general.DaeResolver{Ctx: ctx}).Modified()
		if err != nil || !modified {
			t.Errorf("concurrent change lost: modified=%v, err=%v", modified, err)
		}
	}()
	committed := make(chan struct{})
	go func() {
		err := db.DB(ctx).Transaction(func(tx *gorm.DB) error {
			return tx.Model(&group).Updates(map[string]interface{}{
				"name": "renamed", "version": gorm.Expr("version + 1"),
			}).Error
		})
		writeDone <- err
		close(committed)
	}()
	select {
	case <-committed:
	case <-time.After(time.Second):
		t.Fatal("concurrent write did not commit while reload was paused")
	}
}

func TestRunReloadAndPersistenceFailures(t *testing.T) {
	for _, failure := range []string{"reload", "statement", "commit"} {
		t.Run(failure, func(t *testing.T) {
			ctx, _ := seedRun(t)
			d := db.DB(ctx)
			sqlDB, err := d.DB()
			if err != nil {
				t.Fatal(err)
			}
			sqlDB.SetMaxOpenConns(1)
			if failure == "statement" {
				err = d.Exec(`CREATE TRIGGER fail_run BEFORE UPDATE ON systems
					BEGIN SELECT RAISE(ABORT, 'persist rejected'); END`).Error
			} else if failure == "commit" {
				for _, statement := range []string{
					"PRAGMA foreign_keys = ON",
					`CREATE TABLE invalid_state (system_id INTEGER REFERENCES systems(id) DEFERRABLE INITIALLY DEFERRED)`,
					`CREATE TRIGGER fail_run AFTER UPDATE ON systems
						BEGIN INSERT INTO invalid_state VALUES (999); END`,
				} {
					if err = d.Exec(statement).Error; err != nil {
						break
					}
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			oldReload := dae.ChReloadConfigs
			dae.ChReloadConfigs = make(chan *dae.ReloadMessage, 1)
			defer func() { dae.ChReloadConfigs = oldReload }()
			done := make(chan error, 1)
			go func() {
				_, err := Run(ctx, false)
				done <- err
			}()
			select {
			case message := <-dae.ChReloadConfigs:
				if failure == "reload" {
					message.Callback <- errors.New("reload rejected")
				} else {
					message.Callback <- nil
				}
			case err := <-done:
				t.Fatalf("Run before reload: %v", err)
			}
			err = <-done
			want := map[string]string{"reload": "reload rejected", "statement": "persist rejected", "commit": "FOREIGN KEY constraint failed"}[failure]
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("Run error = %v, want %q", err, want)
			}
			var sys db.System
			if err := d.Preload("RunningGroups").First(&sys).Error; err != nil || sys.Running || len(sys.RunningGroups) != 0 {
				t.Fatalf("failed run persisted state: %+v, err=%v", sys, err)
			}
			select {
			case <-dae.ChReloadConfigs:
				t.Fatal("persistence failure attempted another reload")
			default:
			}
		})
	}
}

func TestDryRunPreservesSnapshotAndSerializesReloads(t *testing.T) {
	ctx, _ := seedRun(t)
	sys := db.System{}
	d := db.DB(ctx)
	if err := d.First(&sys).Error; err != nil {
		t.Fatal(err)
	}
	if err := d.Model(&sys).Updates(map[string]interface{}{"running": true, "running_config_version": 7}).Error; err != nil {
		t.Fatal(err)
	}
	if err := d.Where("1 = 1").Delete(&db.Config{}).Error; err != nil {
		t.Fatal(err)
	}
	oldReload := dae.ChReloadConfigs
	dae.ChReloadConfigs = make(chan *dae.ReloadMessage)
	defer func() { dae.ChReloadConfigs = oldReload }()
	done := make(chan error, 1)
	go func() {
		_, err := Run(ctx, true)
		done <- err
	}()
	var message *dae.ReloadMessage
	select {
	case message = <-dae.ChReloadConfigs:
	case err := <-done:
		t.Fatalf("dry Run before reload: %v", err)
	}
	defer func() {
		close(message.Callback)
		if err := <-done; err != nil {
			t.Errorf("dry Run: %v", err)
		}
		var after db.System
		if err := d.First(&after).Error; err != nil || after.Running || after.RunningConfigVersion != 7 {
			t.Errorf("dry Run changed snapshot: %+v, err=%v", after, err)
		}
	}()
	if message.Config != dae.EmptyConfig {
		t.Error("dry Run did not reload the empty config")
	}
	if _, err := Run(ctx, true); err == nil {
		t.Error("overlapping reload was accepted")
	}
}

// A node change made while the reload is in flight cannot bump the version of
// a group that is not running yet; Run must still report the plane as modified.
func TestRunMarksGroupsModifiedAfterConcurrentNodeChange(t *testing.T) {
	ctx, group := seedRun(t)
	oldReload := dae.ChReloadConfigs
	dae.ChReloadConfigs = make(chan *dae.ReloadMessage)
	defer func() { dae.ChReloadConfigs = oldReload }()
	runDone := make(chan error, 1)
	go func() {
		_, err := Run(ctx, false)
		runDone <- err
	}()
	var message *dae.ReloadMessage
	select {
	case message = <-dae.ChReloadConfigs:
	case err := <-runDone:
		t.Fatalf("Run before reload: %v", err)
	}
	if err := nodesvc.AutoUpdateVersionByIds(db.DB(ctx), []uint{group.Node[0].ID}); err != nil {
		t.Fatal(err)
	}
	close(message.Callback)
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
	modified, err := (&general.DaeResolver{Ctx: ctx}).Modified()
	if err != nil || !modified {
		t.Fatalf("node change during reload not reported: modified=%v err=%v", modified, err)
	}
}
