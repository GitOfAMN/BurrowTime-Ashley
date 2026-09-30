package integrations

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/fabean/BurrowTime/internal/store"
)

func dailyFrames() []store.Frame {
	start := time.Date(2026, 9, 30, 9, 0, 0, 0, time.Local)
	frames := []store.Frame{}
	for i := range 4 {
		from := start.Add(time.Duration(i) * time.Hour).Unix()
		stop := from + 60
		frames = append(frames, store.Frame{ID: string(rune('a' + i)), Project: "portal", Tags: []string{"SEMA-123"}, Start: from, Stop: &stop})
	}
	return frames
}

func TestDailyCombinationBothConnectors(t *testing.T) {
	for _, plugin := range []string{"clockify", "timetable"} {
		t.Run(plugin, func(t *testing.T) {
			t.Parallel()
			c, _ := fixture()
			connection := c.Connections["work"]
			connection.Plugin = plugin
			connection.BaseURL = "https://timetable.example"
			c.Connections["work"] = connection
			frames := dailyFrames()
			dir := t.TempDir()
			creates := 0
			base := mockCall(&creates)
			call := func(ctx context.Context, request Request) (Response, error) {
				if request.Operation == "create" {
					start, _ := time.Parse(time.RFC3339, request.Entry.Start)
					end, _ := time.Parse(time.RFC3339, request.Entry.End)
					if end.Sub(start) != time.Hour || start.Unix() != frames[0].Start {
						t.Fatalf("expected four rounded quarters at earliest start: %+v", request.Entry)
					}
					ledger, err := LoadLedger(dir)
					if err != nil || len(ledger.Records) != 4 {
						t.Fatalf("missing durable member receipts: %+v %v", ledger, err)
					}
				}
				return base(ctx, request)
			}
			opts := Options{Connection: "work"}
			for range 2 {
				if err := Sync(context.Background(), dir, c, frames, opts, call, io.Discard); err != nil {
					t.Fatal(err)
				}
			}
			if creates != 1 {
				t.Fatalf("created %d entries", creates)
			}
			changed := *frames[2].Stop + 1
			frames[2].Stop = &changed
			if err := Sync(context.Background(), dir, c, frames, opts, call, io.Discard); err == nil {
				t.Fatal("accepted changed member")
			}
			if creates != 1 {
				t.Fatal("uploaded changed member")
			}
		})
	}
}

func TestDailyCombinationSeparatesDaysProjectsTagsAndBilling(t *testing.T) {
	c, _ := fixture()
	frames := dailyFrames()
	frames[1].Tags = []string{"SEMA-456"}
	frames[2].Project = "other"
	c.Projects["other"] = Mapping{Connection: "work", ProjectID: "project", Billable: true}
	frames[3].Start += 24 * 60 * 60
	stop := frames[3].Start + 60
	frames[3].Stop = &stop
	creates := 0
	if err := Sync(context.Background(), t.TempDir(), c, frames, Options{Connection: "work"}, mockCall(&creates), io.Discard); err != nil {
		t.Fatal(err)
	}
	if creates != 4 {
		t.Fatalf("merged distinct groups: %d", creates)
	}
}

func TestCombinedPendingRecovery(t *testing.T) {
	for _, plugin := range []string{"clockify", "timetable"} {
		t.Run(plugin, func(t *testing.T) {
			c, _ := fixture()
			connection := c.Connections["work"]
			connection.Plugin = plugin
			connection.BaseURL = "https://timetable.example"
			c.Connections["work"] = connection
			frames := dailyFrames()
			dir := t.TempDir()
			creates := 0
			base := mockCall(&creates)
			var first Request
			call := func(ctx context.Context, r Request) (Response, error) {
				if r.Operation == "create" && creates == 0 {
					creates++
					first = r
					return Response{}, errors.New("lost response")
				}
				if r.Operation == "create" && (r.Entry != first.Entry || r.FrameID != first.FrameID) {
					t.Fatal("retry changed payload or ID")
				}
				return base(ctx, r)
			}
			opts := Options{Connection: "work"}
			if err := Sync(context.Background(), dir, c, frames, opts, call, io.Discard); err == nil {
				t.Fatal("expected pending upload")
			}
			if plugin == "clockify" {
				ledger, err := LoadLedger(dir)
				if err != nil {
					t.Fatal(err)
				}
				pending := ledger.Records["b"]
				resolver := func(_ context.Context, r Request) (Response, error) {
					return Response{RemoteID: r.RemoteID, UserID: "user", Entry: &pending.Entry}, nil
				}
				if err := Resolve(context.Background(), dir, "work", "b", "resolved", connection, resolver); err != nil {
					t.Fatal(err)
				}
			}
			if err := Sync(context.Background(), dir, c, frames, opts, call, io.Discard); err != nil {
				t.Fatal(err)
			}
			want := 1
			if plugin == "timetable" {
				want = 2
			}
			if creates != want {
				t.Fatalf("create count %d, want %d", creates, want)
			}
			if err := Sync(context.Background(), dir, c, frames, opts, call, io.Discard); err != nil {
				t.Fatal(err)
			}
			if creates != want {
				t.Fatal("duplicated recovered group")
			}
		})
	}
}
