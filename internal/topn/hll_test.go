package topn

import (
	"fmt"
	"testing"
)

func TestHLLCounter_Basic(t *testing.T) {
	counter := newHLLCounter()
	counter.Add("user-1")
	counter.Add("user-2")
	counter.Add("user-1")

	estimate := counter.Estimate()
	if estimate < 1 || estimate > 4 {
		t.Errorf("HLL estimate %d is unreasonable for 2 unique sources", estimate)
	}
}

func TestHLLCounter_Merge(t *testing.T) {
	leftCounter := newHLLCounter()
	rightCounter := newHLLCounter()
	for i := 0; i < 50; i++ {
		leftCounter.Add(fmt.Sprintf("ua-%d", i))
	}
	for i := 50; i < 100; i++ {
		rightCounter.Add(fmt.Sprintf("ub-%d", i))
	}

	leftCounter.Merge(rightCounter)
	estimate := leftCounter.Estimate()
	if estimate < 80 || estimate > 120 {
		t.Errorf("after merge: estimate %d, expected ~100", estimate)
	}
}

func TestHLLCounter_Clone(t *testing.T) {
	orig := newHLLCounter()
	orig.Add("u1")
	orig.Add("u2")

	cloned := orig.Clone()

	cloned.Add("u3")

	origEst := orig.Estimate()
	clonedEst := cloned.Estimate()

	if origEst >= clonedEst {
		t.Errorf("clone(%d) should have higher estimate than original(%d) after adding u3", clonedEst, origEst)
	}
}

func TestHLLCounter_MergeTypeMismatch(t *testing.T) {
	hll := newHLLCounter()
	exact := newExactCounter()
	hll.Merge(exact)
}

func TestExactCounter_Merge(t *testing.T) {
	leftCounter := newExactCounter()
	rightCounter := newExactCounter()
	leftCounter.Add("u1")
	rightCounter.Add("u2")
	rightCounter.Add("u1")

	leftCounter.Merge(rightCounter)
	if leftCounter.Estimate() != 2 {
		t.Errorf("expected 2 unique after merge, got %d", leftCounter.Estimate())
	}
}
