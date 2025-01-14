package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"

	"github.com/samber/lo"
	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
	"github.com/tgoodwin/sleeve/pkg/tag"
)

var inFile = flag.String("logfile", "default.log", "path to the log file")

var logTypes = []string{tag.ControllerOperationKey, tag.ObjectVersionKey}
var pattern = regexp.MustCompile(`{"LogType": "(?:` + strings.Join(logTypes, "|") + `)"}`)

func stripLogtype(line string) string {
	return pattern.ReplaceAllString(line, "")
}

func stripLogtypeFromLines(lines []string) []string {
	return lo.Map(lines, func(line string, _ int) string {
		return stripLogtype(line)
	})
}

func main() {
	flag.Parse()
	f, err := os.Open(*inFile)
	if err != nil {
		panic(err.Error())
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		panic(err.Error())
	}

	lines = lo.FilterMap(lines, func(l string, _ int) (string, bool) {
		isSleeveLine := strings.Contains(l, tag.LoggerName)
		parts := strings.SplitN(l, tag.LoggerName, 2)
		if len(parts) > 1 {
			l = strings.TrimSpace(parts[1])
		}
		return l, isSleeveLine
	})
	// fmt.Println("Sleeveless lines:", lines[0])

	// filter controller ops
	controllerOps := lo.FilterMap(lines, func(l string, _ int) (string, bool) {
		return l, strings.Contains(l, tag.ControllerOperationKey)
	})
	controllerOps = stripLogtypeFromLines(controllerOps)
	fmt.Println("Controller Operations:", len(controllerOps))

	events := lo.Map(controllerOps, func(l string, _ int) event.Event {
		var e event.Event
		if err := e.UnmarshalJSON([]byte(l)); err != nil {
			panic(err.Error())
		}
		return e
	})
	fmt.Println("events:", len(events))

	versions := lo.FilterMap(lines, func(l string, _ int) (string, bool) {
		return l, strings.Contains(l, tag.ObjectVersionKey)
	})
	versions = stripLogtypeFromLines(versions)
	records := lo.Map(versions, func(v string, i int) snapshot.Record {
		r, err := snapshot.LoadFromString(v)
		if err != nil {
			panic(err.Error())
		}
		return r
	})

	comp := make(map[snapshot.VersionKey]event.CausalKey)
	for _, r := range records {
		obj := r.ToUnstructured()
		vkey := snapshot.VersionKey{
			Kind:     r.Kind,
			ObjectID: r.ObjectID,
			Version:  r.Version,
		}
		ckey, err := event.GetCausalKey(obj)
		if err != nil {
			fmt.Printf("Error getting causal key: %s\n", err.Error())
			continue
		}
		if existing, ok := comp[vkey]; ok {
			if existing != ckey {
				fmt.Printf("Conflict: %s\n", vkey)
			}
		}
		comp[vkey] = ckey
	}

	var sortedKeys []snapshot.VersionKey
	for k := range comp {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Slice(sortedKeys, func(a, b int) bool {
		return sortedKeys[a].Version < sortedKeys[b].Version
	})

	for _, k := range sortedKeys {
		v := comp[k]
		fmt.Printf("Kind: %s, VersionKey: %s\nCausalKey: %v\n", k.Kind, k.Version, v.ChangeID)
	}

	// recordsByVersion := lo.GroupBy(records, func(r snapshot.Record) string {
	// 	return r.GetID()
	// })
	// fmt.Println("Total object versions:", len(recordsByVersion))
	// for k := range recordsByVersion {
	// 	fmt.Printf("Witnessed version: %s\n", k)
	// }

	// // by object ID
	// grouped := snapshot.GroupByID(records)
	// for k, v := range grouped {
	// 	versionCount := len(v)
	// 	fmt.Printf("Kind: %s, Object ID: %s, versions seen: %d\n", v[0].Kind, k, versionCount)
	// 	for i, r := range v {
	// 		if i < versionCount-1 {
	// 			diff, err := r.Diff(v[i+1])
	// 			if err != nil {
	// 				fmt.Printf("Error diffing: %s\n", err.Error())
	// 			}
	// 			fmt.Printf("%s\n", diff)
	// 		}
	// 	}
	// }
}
