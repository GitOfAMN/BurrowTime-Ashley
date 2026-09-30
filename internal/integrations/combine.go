package integrations

import (
	"fmt"
	"time"

	"github.com/fabean/BurrowTime/internal/store"
)

// combineDaily sums already-rounded durations, using the earliest start for
// the combined interval. Previously attempted groups keep their exact payload.
func combineDaily(queue []Receipt, frames []store.Frame, config Config, name string) ([]Receipt, error) {
	type groupKey struct {
		day, project, remoteProject, description string
		billable                                 bool
	}
	sources := make(map[string]store.Frame, len(frames))
	for _, f := range frames {
		sources[f.ID] = f
	}
	groups := map[groupKey]int{}
	attempted := map[string]bool{}
	result := []Receipt{}
	for _, r := range queue {
		if r.GroupID != "" {
			if attempted[r.GroupID] {
				continue
			}
			for _, member := range r.Members {
				f, exists := sources[member.FrameID]
				if !exists {
					return nil, fmt.Errorf("upload group %s is missing source frame %s", r.GroupID, member.FrameID)
				}
				m, mapped := config.Mapping(name, f.Project)
				current, err := Projection(f, name, config.Connections[name], m)
				if !mapped || err != nil || current.Fingerprint != member.Fingerprint {
					return nil, fmt.Errorf("upload group %s: source frame %s changed; reconcile before retrying", r.GroupID, member.FrameID)
				}
			}
			attempted[r.GroupID] = true
			result = append(result, r)
			continue
		}
		f := sources[r.FrameID]
		key := groupKey{
			day:     time.Unix(f.Start, 0).In(time.Local).Format("2006-01-02"),
			project: f.Project, remoteProject: r.Entry.ProjectID,
			description: r.Entry.Description, billable: r.Entry.Billable,
		}
		index, exists := groups[key]
		if !exists {
			groups[key] = len(result)
			result = append(result, r)
			continue
		}
		combined := &result[index]
		if len(combined.Members) == 0 {
			combined.Members = []Receipt{*combined}
			combined.GroupID = combined.FrameID
		}
		if combined.ExportSeconds > (1<<63-1)-r.ExportSeconds || combined.RecordedSeconds > (1<<63-1)-r.RecordedSeconds {
			return nil, fmt.Errorf("combined duration overflows for %s", combined.FrameID)
		}
		combined.Members = append(combined.Members, r)
		combined.RecordedSeconds += r.RecordedSeconds
		combined.ExportSeconds += r.ExportSeconds
		start, err := time.Parse(time.RFC3339, combined.Entry.Start)
		if err != nil {
			return nil, err
		}
		if start.Unix() > (1<<63-1)-combined.ExportSeconds {
			return nil, fmt.Errorf("combined end overflows")
		}
		combined.Entry.End = time.Unix(start.Unix()+combined.ExportSeconds, 0).UTC().Format(time.RFC3339)
	}
	return result, nil
}
