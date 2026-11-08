// Package reactive provides a dependency-tracked graph of atoms whose values live in a Registry.
//
// An atom is a definition: a source holding a value, or a derived function of other atoms. A Registry stores one value per atom, records which atoms each derived atom read, and recomputes a derived atom only when a dependency changed. Definitions are shareable across registries, so one actor definition serves many threads.
package reactive

import "fmt"

// Getter reads atom values. During a derived computation it records each read as a dependency.
type Getter interface {
	read(n node) any
	current() *entry
}

// node is the type-erased form of an atom definition; implementations must be comparable.
type node interface {
	compute(g Getter) any
	initial() (any, bool)
}

// Atom is a typed atom definition.
type Atom[T any] interface {
	node
	cast(v any) T
}

type derived[T any] struct {
	read func(Getter) T
}

func (d *derived[T]) compute(g Getter) any { return d.read(g) }
func (d *derived[T]) initial() (any, bool) { return nil, false }
func (d *derived[T]) cast(v any) T         { return castValue[T](v) }

// Source is a writable atom holding a value set through Set.
type Source[T any] struct {
	value T
}

func (s *Source[T]) compute(Getter) any   { return s.value }
func (s *Source[T]) initial() (any, bool) { return s.value, true }
func (s *Source[T]) cast(v any) T         { return castValue[T](v) }

func castValue[T any](v any) T {
	if v == nil {
		var zero T
		return zero
	}
	return v.(T)
}

// New defines a derived atom computed from the atoms its read function gets.
func New[T any](read func(Getter) T) Atom[T] {
	return &derived[T]{read: read}
}

// Value defines a source atom whose value starts at initial in every registry.
func Value[T any](initial T) *Source[T] {
	return &Source[T]{value: initial}
}

type member[K comparable, T any] struct {
	family *family[K, T]
	key    K
}

type family[K comparable, T any] struct {
	read func(K, Getter) T
}

func (m member[K, T]) compute(g Getter) any { return m.family.read(m.key, g) }
func (m member[K, T]) initial() (any, bool) { return nil, false }
func (m member[K, T]) cast(v any) T         { return castValue[T](v) }

// Family defines a keyed set of derived atoms. Members are compared by key, so a registry holds one value per key and the definition retains no per-key state.
func Family[K comparable, T any](read func(key K, g Getter) T) func(K) Atom[T] {
	f := &family[K, T]{read: read}
	return func(key K) Atom[T] { return member[K, T]{family: f, key: key} }
}

// Get reads an atom through a getter.
func Get[T any](g Getter, a Atom[T]) T {
	return a.cast(g.read(a))
}

// Previous returns the value the atom being computed held before this computation.
func Previous[T any](g Getter) (T, bool) {
	e := g.current()
	if e == nil || !e.computed {
		var zero T
		return zero, false
	}
	return castValue[T](e.value), true
}

type entry struct {
	value     any
	version   uint64
	computed  bool
	checked   uint64
	deps      []dependency
	computing bool
}

type dependency struct {
	node    node
	version uint64
}

// Registry stores atom values and dependencies. A Registry is not safe for concurrent use; callers serialize access.
type Registry struct {
	entries map[node]*entry
	epoch   uint64
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{entries: map[node]*entry{}, epoch: 1}
}

// Read returns an atom's current value without recording a dependency.
func Read[T any](r *Registry, a Atom[T]) T {
	return a.cast(r.read(a))
}

// Set replaces a source atom's value in this registry.
func Set[T any](r *Registry, s *Source[T], value T) {
	e := r.entry(s)
	e.value = value
	e.computed = true
	e.version++
	r.epoch++
}

func (r *Registry) read(n node) any { return r.refresh(n).value }
func (r *Registry) current() *entry { return nil }
func (r *Registry) entry(n node) *entry {
	e, ok := r.entries[n]
	if !ok {
		e = &entry{}
		r.entries[n] = e
	}
	return e
}

func (r *Registry) refresh(n node) *entry {
	e := r.entry(n)
	if value, ok := n.initial(); ok {
		if !e.computed {
			e.value = value
			e.computed = true
			e.version = 1
		}
		return e
	}
	if e.computed && e.checked == r.epoch {
		return e
	}
	if e.computing {
		panic(fmt.Errorf("reactive: atom depends on itself"))
	}
	if e.computed && !r.stale(e) {
		e.checked = r.epoch
		return e
	}
	r.compute(n, e)
	return e
}

func (r *Registry) stale(e *entry) bool {
	for _, d := range e.deps {
		if r.refresh(d.node).version != d.version {
			return true
		}
	}
	return false
}

func (r *Registry) compute(n node, e *entry) {
	e.computing = true
	defer func() { e.computing = false }()
	t := &tracker{registry: r, self: e, seen: map[node]struct{}{}}
	value := n.compute(t)
	e.value = value
	e.version++
	e.deps = t.deps
	e.computed = true
	e.checked = r.epoch
}

type tracker struct {
	registry *Registry
	self     *entry
	deps     []dependency
	seen     map[node]struct{}
}

func (t *tracker) read(n node) any {
	e := t.registry.refresh(n)
	if _, ok := t.seen[n]; !ok {
		t.seen[n] = struct{}{}
		t.deps = append(t.deps, dependency{node: n, version: e.version})
	}
	return e.value
}

func (t *tracker) current() *entry { return t.self }

// Snapshot captures every entry so Restore can discard later computations and writes.
type Snapshot struct {
	entries map[node]entry
}

// Snapshot records the registry state. Values are retained by reference, so atom values must not be mutated after they are returned.
func (r *Registry) Snapshot() Snapshot {
	s := Snapshot{entries: make(map[node]entry, len(r.entries))}
	for n, e := range r.entries {
		s.entries[n] = *e
	}
	return s
}

// Restore returns the registry to a snapshot taken from it.
func (r *Registry) Restore(s Snapshot) {
	next := make(map[node]*entry, len(s.entries))
	for n, e := range s.entries {
		copied := e
		copied.computing = false
		next[n] = &copied
	}
	r.entries = next
	r.epoch++
}

// Each calls visit for every atom computed in the registry, in no particular order.
func (r *Registry) Each(visit func(atom any, value any)) {
	for n, e := range r.entries {
		if e.computed {
			visit(n, e.value)
		}
	}
}
