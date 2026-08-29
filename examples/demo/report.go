package main

import (
	"fmt"
	"sort"
	"strings"
)

// ledger is a snapshot of what the demo prefix holds, split by the role each
// object plays. Keeping the three apart is what makes the object count
// readable: garbage collection removes segments while retention and GC each
// add a small amount of metadata.
type ledger struct {
	segments    int
	catalog     int
	maintenance int
	bytes       int64
	objects     []object
}

func newLedger(objects []object) ledger {
	l := ledger{objects: append([]object(nil), objects...)}
	sort.Slice(l.objects, func(i, j int) bool { return l.objects[i].key < l.objects[j].key })
	for _, o := range objects {
		l.bytes += o.size
		switch {
		case strings.Contains(o.key, "/maintenance/"):
			l.maintenance++
		case strings.Contains(o.key, "/segments/"):
			l.segments++
		default:
			l.catalog++
		}
	}
	return l
}

func (l ledger) total() int { return l.segments + l.catalog + l.maintenance }

func (l ledger) String() string {
	return fmt.Sprintf("%2d objects · %s   %2d segments · %d catalog · %d maintenance",
		l.total(), humanBytes(l.bytes), l.segments, l.catalog, l.maintenance)
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	value, exp := float64(n)/unit, 0
	for value >= unit && exp < 3 {
		value /= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", value, "KMGT"[exp])
}

// delta renders the change from a previous snapshot, e.g. "-6 segments, +2 maintenance".
func (l ledger) delta(prev ledger) string {
	parts := make([]string, 0, 3)
	for _, change := range []struct {
		name string
		n    int
	}{
		{"segments", l.segments - prev.segments},
		{"catalog", l.catalog - prev.catalog},
		{"maintenance", l.maintenance - prev.maintenance},
	} {
		if change.n != 0 {
			parts = append(parts, fmt.Sprintf("%+d %s", change.n, change.name))
		}
	}
	if len(parts) == 0 {
		return "unchanged"
	}
	return strings.Join(parts, ", ")
}

const rule = "────────────────────────────────────────────────────────────"

// section prints a numbered banner plus the one thing to understand about it.
func section(n int, title, subtitle string) {
	banner(fmt.Sprintf("── %d · %s ", n, title))
	if subtitle != "" {
		fmt.Printf("   %s\n\n", subtitle)
	}
}

// sectionPlain prints an unnumbered banner.
func sectionPlain(title string) {
	banner(fmt.Sprintf("── %s ", title))
}

func banner(text string) {
	fill := max(0, len([]rune(rule))-len([]rune(text)))
	fmt.Printf("\n%s%s\n", text, string([]rune(rule)[:fill]))
}

// row prints an aligned label and value.
func row(label, format string, args ...any) {
	fmt.Printf("   %-15s %s\n", label, fmt.Sprintf(format, args...))
}

// keyList prints object keys with the shared prefix trimmed, under -v.
func keyList(l ledger, prefix string) {
	for _, o := range l.objects {
		fmt.Printf("   %-15s %8s  %s\n", "", humanBytes(o.size), strings.TrimPrefix(o.key, prefix+"/"))
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
