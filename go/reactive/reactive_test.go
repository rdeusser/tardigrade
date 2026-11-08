package reactive

import "testing"

func TestDerivedRecomputesOnlyWhenDependencyChanges(t *testing.T) {
	count := Value(1)
	other := Value("x")
	calls := 0
	double := New(func(g Getter) int {
		calls++
		return Get(g, count) * 2
	})
	r := NewRegistry()
	if got := Read(r, double); got != 2 {
		t.Fatalf("double = %d, want 2", got)
	}
	Set(r, other, "y")
	Read(r, double)
	if calls != 1 {
		t.Fatalf("unrelated write recomputed: calls = %d", calls)
	}
	Set(r, count, 5)
	if got := Read(r, double); got != 10 || calls != 2 {
		t.Fatalf("double = %d calls = %d, want 10 and 2", got, calls)
	}
}

func TestRegistriesHoldSeparateValues(t *testing.T) {
	count := Value(0)
	a, b := NewRegistry(), NewRegistry()
	Set(a, count, 3)
	if Read(a, count) != 3 || Read(b, count) != 0 {
		t.Fatal("registries share a source value")
	}
}

func TestPreviousExposesPriorValue(t *testing.T) {
	step := Value(1)
	sum := New(func(g Getter) int {
		prior, _ := Previous[int](g)
		return prior + Get(g, step)
	})
	r := NewRegistry()
	Read(r, sum)
	Set(r, step, 2)
	if got := Read(r, sum); got != 3 {
		t.Fatalf("sum = %d, want 3", got)
	}
}

func TestFamilyMembersShareKeyIdentity(t *testing.T) {
	base := Value(10)
	plus := Family(func(k int, g Getter) int { return Get(g, base) + k })
	r := NewRegistry()
	if Read(r, plus(1)) != 11 || Read(r, plus(2)) != 12 {
		t.Fatal("family member value mismatch")
	}
	if plus(1) != plus(1) {
		t.Fatal("family members with the same key differ")
	}
}

func TestRestoreDiscardsLaterWrites(t *testing.T) {
	count := Value(1)
	double := New(func(g Getter) int { return Get(g, count) * 2 })
	r := NewRegistry()
	Read(r, double)
	snap := r.Snapshot()
	Set(r, count, 7)
	if Read(r, double) != 14 {
		t.Fatal("write not visible")
	}
	r.Restore(snap)
	if got := Read(r, double); got != 2 {
		t.Fatalf("double after restore = %d, want 2", got)
	}
	Set(r, count, 4)
	if got := Read(r, double); got != 8 {
		t.Fatalf("double after restore and write = %d, want 8", got)
	}
}

func TestCycleIsReported(t *testing.T) {
	var a Atom[int]
	a = New(func(g Getter) int { return Get(g, a) })
	defer func() {
		if recover() == nil {
			t.Fatal("cycle did not panic")
		}
	}()
	Read(NewRegistry(), a)
}
