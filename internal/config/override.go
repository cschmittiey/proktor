package config

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

const envPrefix = "PROKTOR_"

// tomlKey returns the toml key of a struct field ("" if it has none or "-").
func tomlKey(f reflect.StructField) string {
	tag := f.Tag.Get("toml")
	if i := strings.IndexByte(tag, ','); i >= 0 {
		tag = tag[:i]
	}
	if tag == "-" {
		return ""
	}
	return tag
}

// newElem builds a fresh array element, pre-filled with the per-element
// defaults so that appending "upstream.1.name" yields a usable upstream.
func newElem(t reflect.Type) reflect.Value {
	v := reflect.New(t)
	switch e := v.Interface().(type) {
	case *Mount:
		applyMountDefaults(e)
	case *Upstream:
		applyUpstreamDefaults(e)
	case *Webhook:
		applyWebhookDefaults(e)
	}
	return v.Elem()
}

// Override sets one value by dotted path using TOML key names, for example
// "http.token", "metadata.delay_seconds", "ingest.mount.0.password" or
// "upstream.1.hold_seconds". Indexing one past the end of an array appends a
// new element (with per-element defaults). The value is parsed according to
// the field's kind: string, bool (strconv.ParseBool), int or float64.
func (c *Config) Override(path, value string) error {
	if path == "" {
		return errors.New("override: empty path")
	}
	// Work on a copy so a failed override cannot leave a half-appended element.
	work := *c
	cur := reflect.ValueOf(&work).Elem()
	segs := strings.Split(path, ".")
	for i, seg := range segs {
		here := strings.Join(segs[:i], ".")
		switch cur.Kind() {
		case reflect.Struct:
			next, ok := fieldByKey(cur, seg)
			if !ok {
				return fmt.Errorf("override %q: unknown key %q%s (valid: %s)", path, seg, inPath(here), strings.Join(structKeys(cur.Type()), ", "))
			}
			cur = next
		case reflect.Slice:
			idx, err := strconv.Atoi(seg)
			if err != nil || idx < 0 {
				return fmt.Errorf("override %q: %q is an array; expected an index, got %q", path, here, seg)
			}
			switch {
			case idx < cur.Len():
			case idx == cur.Len():
				cur.Set(reflect.Append(cur, newElem(cur.Type().Elem())))
			default:
				return fmt.Errorf("override %q: index %d out of range for %q (length %d; use %d to append)", path, idx, here, cur.Len(), cur.Len())
			}
			cur = cur.Index(idx)
		default:
			return fmt.Errorf("override %q: %q is a value, cannot descend into %q", path, here, seg)
		}
	}
	if err := setLeaf(cur, value); err != nil {
		return fmt.Errorf("override %q: %w", path, err)
	}
	work.normalize()
	*c = work
	return nil
}

func inPath(p string) string {
	if p == "" {
		return ""
	}
	return " in " + strconv.Quote(p)
}

func fieldByKey(v reflect.Value, key string) (reflect.Value, bool) {
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		if tomlKey(t.Field(i)) == key {
			return v.Field(i), true
		}
	}
	return reflect.Value{}, false
}

func structKeys(t reflect.Type) []string {
	var ks []string
	for i := 0; i < t.NumField(); i++ {
		if k := tomlKey(t.Field(i)); k != "" {
			ks = append(ks, k)
		}
	}
	return ks
}

func setLeaf(v reflect.Value, s string) error {
	switch v.Kind() {
	case reflect.String:
		v.SetString(s)
	case reflect.Bool:
		b, err := strconv.ParseBool(strings.TrimSpace(s))
		if err != nil {
			return fmt.Errorf("%q is not a valid boolean (try true/false)", s)
		}
		v.SetBool(b)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(strings.TrimSpace(s), 10, v.Type().Bits())
		if err != nil {
			return fmt.Errorf("%q is not a valid integer", s)
		}
		v.SetInt(n)
	case reflect.Float32, reflect.Float64:
		f, err := strconv.ParseFloat(strings.TrimSpace(s), v.Type().Bits())
		if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
			return fmt.Errorf("%q is not a valid number", s)
		}
		v.SetFloat(f)
	case reflect.Struct, reflect.Slice:
		return fmt.Errorf("is a section, not a value; address one of its keys")
	default:
		return fmt.Errorf("unsupported field kind %s", v.Kind())
	}
	return nil
}

// Paths lists every overridable dotted path for the current config, in struct
// order, expanding existing array elements by index. Arrays that are empty
// contribute nothing; any array can be extended by overriding index len().
func (c *Config) Paths() []string {
	var out []string
	collectPaths(reflect.ValueOf(c).Elem(), "", &out)
	return out
}

func collectPaths(v reflect.Value, prefix string, out *[]string) {
	switch v.Kind() {
	case reflect.Struct:
		t := v.Type()
		for i := 0; i < t.NumField(); i++ {
			k := tomlKey(t.Field(i))
			if k == "" {
				continue
			}
			collectPaths(v.Field(i), joinPath(prefix, k), out)
		}
	case reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			collectPaths(v.Index(i), joinPath(prefix, strconv.Itoa(i)), out)
		}
	default:
		*out = append(*out, prefix)
	}
}

func joinPath(prefix, seg string) string {
	if prefix == "" {
		return seg
	}
	return prefix + "." + seg
}

// resolveEnv maps env-name segments (already lowercased and split on "_") to
// the dotted config path for type t. Keys themselves contain underscores, so
// it tries the longest key match first and backtracks.
func resolveEnv(t reflect.Type, segs []string) ([]string, bool) {
	switch t.Kind() {
	case reflect.Struct:
		type cand struct {
			key  []string
			name string
			typ  reflect.Type
		}
		var cands []cand
		for i := 0; i < t.NumField(); i++ {
			k := tomlKey(t.Field(i))
			if k == "" {
				continue
			}
			cands = append(cands, cand{strings.Split(k, "_"), k, t.Field(i).Type})
		}
		sort.SliceStable(cands, func(i, j int) bool { return len(cands[i].key) > len(cands[j].key) })
		for _, cd := range cands {
			n := len(cd.key)
			if len(segs) < n || !equalSegs(segs[:n], cd.key) {
				continue
			}
			if rest, ok := resolveEnv(cd.typ, segs[n:]); ok {
				return append([]string{cd.name}, rest...), true
			}
		}
		return nil, false
	case reflect.Slice:
		if len(segs) == 0 {
			return nil, false
		}
		n, err := strconv.Atoi(segs[0])
		if err != nil || n < 0 || strconv.Itoa(n) != segs[0] {
			return nil, false
		}
		rest, ok := resolveEnv(t.Elem(), segs[1:])
		if !ok {
			return nil, false
		}
		return append([]string{segs[0]}, rest...), true
	default:
		return nil, len(segs) == 0
	}
}

func equalSegs(a, b []string) bool {
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// pathLess orders dotted paths segment by segment, comparing numeric
// segments numerically so array elements are created in index order.
func pathLess(a, b string) bool {
	as, bs := strings.Split(a, "."), strings.Split(b, ".")
	for i := 0; i < len(as) && i < len(bs); i++ {
		if as[i] == bs[i] {
			continue
		}
		an, aerr := strconv.Atoi(as[i])
		bn, berr := strconv.Atoi(bs[i])
		if aerr == nil && berr == nil {
			return an < bn
		}
		return as[i] < bs[i]
	}
	return len(as) < len(bs)
}

// FromEnv applies PROKTOR_* variables from environ (KEY=VALUE strings, as
// os.Environ returns). PROKTOR_CONFIG is reserved for the CLI and ignored.
// Unknown PROKTOR_* variables produce an error listing them; recognised ones
// are still applied.
func (c *Config) FromEnv(environ []string) error {
	type setting struct{ path, value, name string }
	var settings []setting
	unknown := map[string]struct{}{}
	for _, kv := range environ {
		name, value, _ := strings.Cut(kv, "=")
		if !strings.HasPrefix(name, envPrefix) || name == envPrefix+"CONFIG" {
			continue
		}
		segs := strings.Split(strings.ToLower(strings.TrimPrefix(name, envPrefix)), "_")
		parts, ok := resolveEnv(reflect.TypeOf(*c), segs)
		if !ok || len(parts) == 0 {
			unknown[name] = struct{}{}
			continue
		}
		settings = append(settings, setting{strings.Join(parts, "."), value, name})
	}
	sort.SliceStable(settings, func(i, j int) bool { return pathLess(settings[i].path, settings[j].path) })

	var errs []error
	for _, s := range settings {
		if err := c.Override(s.path, s.value); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", s.name, err))
		}
	}
	if len(unknown) > 0 {
		errs = append(errs, fmt.Errorf("unknown environment variable(s): %s", strings.Join(sortedKeys(unknown), ", ")))
	}
	return errors.Join(errs...)
}

// EnvName returns the environment variable name for a dotted config path.
func EnvName(path string) string {
	return envPrefix + strings.ToUpper(strings.ReplaceAll(path, ".", "_"))
}
