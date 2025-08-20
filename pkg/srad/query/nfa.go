package query

import (
	"bytes"
	"regexp"
	"regexp/syntax"
)

// Engine provides a regex matching engine with prefix-viability checks.
// It uses a minimal Thompson NFA for a supported subset of regex features,
// and falls back to Go's regexp for full matches and prefix pruning when
// unsupported constructs are detected.
type Engine struct {
	re       *regexp.Regexp
	supports bool

	// Fallback helpers
	literal    []byte
	hasLiteral bool

	// Simple NFA (only when supports == true)
	start *nfaState
	acc   *nfaState

	// Simple caches
	prefixCache map[string]bool
}

// Supports reports whether this engine compiled a supported NFA and can use the fast path.
func (e *Engine) Supports() bool {
	return e != nil && e.supports
}

// Compile builds an Engine. For unsupported patterns, supports=false and
// the engine will fall back to regexp and literal-prefix heuristics.
func Compile(re *regexp.Regexp) *Engine {
	eng := &Engine{re: re, prefixCache: make(map[string]bool)}
	if re == nil {
		return eng
	}

	if lit, complete := re.LiteralPrefix(); lit != "" {
		eng.literal = []byte(lit)
		eng.hasLiteral = true && complete // complete literal speeds up pruning
	}

	// Parse syntax tree
	// Note: use POSIX=false to allow typical Go regex features
	r, err := syntax.Parse(re.String(), syntax.ClassNL|syntax.PerlX|syntax.UnicodeGroups)
	if err != nil {
		return eng
	}
	r = r.Simplify()

	// Attempt to build a tiny NFA for a supported subset.
	start, acc, ok := buildNFA(r)
	if ok {
		eng.supports = true
		eng.start = start
		eng.acc = acc
	}
	return eng
}

// Match returns whether the engine accepts the full key.
func (e *Engine) Match(key []byte) bool {
	if e == nil || e.re == nil {
		return false
	}
	if e.supports {
		return nfaMatch(e.start, e.acc, key)
	}
	return e.re.Match(key)
}

// CanMatchPrefix returns true if some extension of the prefix could match.
func (e *Engine) CanMatchPrefix(prefix []byte) bool {
	if e == nil || e.re == nil {
		return true
	}
	if e.supports {
		const minCacheLen = 6
		const cacheStride = 2
		const maxCacheSize = 4096
		useCache := len(prefix) >= minCacheLen && (len(prefix)%cacheStride == 0)
		if useCache {
			if v, ok := e.prefixCache[string(prefix)]; ok {
				return v
			}
		}
		v := nfaViable(e.start, prefix)
		// Only store negatives and respect size cap
		if useCache && !v {
			if len(e.prefixCache) >= maxCacheSize {
				// naive cap: reset map to avoid unbounded growth
				e.prefixCache = make(map[string]bool)
			}
			e.prefixCache[string(prefix)] = false
		}
		return v
	}
	// Fallback: use literal prefix heuristic
	if e.hasLiteral {
		lit := e.literal
		if len(prefix) <= len(lit) {
			return bytes.HasPrefix(lit, prefix)
		}
		return bytes.HasPrefix(prefix, lit)
	}
	return true
}

// StateSet is an opaque handle representing a set of NFA states used during
// product traversal with a trie. Obtain it via StartState and advance with Step.
type StateSet interface{ isStateSet() }

type nfaSet struct{ states map[*nfaState]struct{} }

func (*nfaSet) isStateSet() {}

type fallbackSet struct{ prefix []byte }

func (*fallbackSet) isStateSet() {}

// StartState returns the initial state set for automaton product traversal.
func (e *Engine) StartState() StateSet {
	if e == nil || e.re == nil {
		return &fallbackSet{prefix: nil}
	}
	if e.supports {
		return &nfaSet{states: nfaStart(e.start)}
	}
	return &fallbackSet{prefix: nil}
}

// Step advances the state with a single byte and reports if the new set is viable.
func (e *Engine) Step(s StateSet, b byte) (StateSet, bool) {
	if e == nil || e.re == nil {
		return &fallbackSet{prefix: nil}, true
	}
	switch cur := s.(type) {
	case *nfaSet:
		if !e.supports || cur == nil {
			np := []byte{b}
			fs := &fallbackSet{prefix: np}
			return fs, e.CanMatchPrefix(np)
		}
		next := step(cur.states, rune(b))
		if len(next) == 0 {
			return nil, false
		}
		return &nfaSet{states: next}, true
	case *fallbackSet:
		np := make([]byte, len(cur.prefix)+1)
		copy(np, cur.prefix)
		np[len(cur.prefix)] = b
		return &fallbackSet{prefix: np}, e.CanMatchPrefix(np)
	default:
		np := []byte{b}
		return &fallbackSet{prefix: np}, e.CanMatchPrefix(np)
	}
}

// --- Minimal NFA implementation for a subset of regex ---

type nfaState struct {
	eps   []*nfaState
	trans []edge
	acc   bool
}

type edge struct {
	any    bool
	ranges []rune // pairs [lo, hi, lo, hi, ...] from syntax.Regexp.Rune
	ch     rune
	next   *nfaState
	useCh  bool
}

// buildNFA attempts to build an NFA for a limited subset:
// literals, concatenation, '.', character classes, '?', '*', '+'.
// Alternation '|' and complex constructs will disable support (return ok=false).
func buildNFA(re *syntax.Regexp) (*nfaState, *nfaState, bool) {
	switch re.Op {
	case syntax.OpLiteral:
		start := &nfaState{}
		cur := start
		for _, r := range re.Rune {
			nxt := &nfaState{}
			cur.trans = append(cur.trans, edge{useCh: true, ch: r, next: nxt})
			cur = nxt
		}
		cur.acc = true
		return start, cur, true

	case syntax.OpCharClass:
		start := &nfaState{}
		acc := &nfaState{acc: true}
		start.trans = append(start.trans, edge{ranges: append([]rune(nil), re.Rune...), next: acc})
		return start, acc, true

	case syntax.OpAnyCharNotNL, syntax.OpAnyChar:
		start := &nfaState{}
		acc := &nfaState{acc: true}
		start.trans = append(start.trans, edge{any: true, next: acc})
		return start, acc, true

	case syntax.OpQuest: // X?
		subS, subA, ok := buildNFA(re.Sub[0])
		if !ok {
			return nil, nil, false
		}
		start := &nfaState{}
		acc := &nfaState{acc: true}
		start.eps = append(start.eps, subS) // take
		start.eps = append(start.eps, acc)  // or skip
		subA.eps = append(subA.eps, acc)
		return start, acc, true

	case syntax.OpStar: // X*
		subS, subA, ok := buildNFA(re.Sub[0])
		if !ok {
			return nil, nil, false
		}
		start := &nfaState{}
		acc := &nfaState{acc: true}
		start.eps = append(start.eps, acc)  // zero
		start.eps = append(start.eps, subS) // one or more
		subA.eps = append(subA.eps, subS)   // loop
		subA.eps = append(subA.eps, acc)    // exit
		return start, acc, true

	case syntax.OpPlus: // X+
		subS, subA, ok := buildNFA(re.Sub[0])
		if !ok {
			return nil, nil, false
		}
		acc := &nfaState{acc: true}
		subA.eps = append(subA.eps, subS) // loop
		subA.eps = append(subA.eps, acc)  // exit
		return subS, acc, true

	case syntax.OpConcat:
		if len(re.Sub) == 0 {
			s := &nfaState{acc: true}
			return s, s, true
		}
		s0, a0, ok := buildNFA(re.Sub[0])
		if !ok {
			return nil, nil, false
		}
		start := s0
		lastAcc := a0
		for i := 1; i < len(re.Sub); i++ {
			si, ai, ok := buildNFA(re.Sub[i])
			if !ok {
				return nil, nil, false
			}
			lastAcc.eps = append(lastAcc.eps, si)
			lastAcc = ai
		}
		return start, lastAcc, true

	case syntax.OpAlternate: // X|Y|Z
		if len(re.Sub) == 0 {
			s := &nfaState{acc: true}
			return s, s, true
		}
		start := &nfaState{}
		acc := &nfaState{acc: true}
		for _, sub := range re.Sub {
			si, ai, ok := buildNFA(sub)
			if !ok {
				return nil, nil, false
			}
			start.eps = append(start.eps, si)
			ai.eps = append(ai.eps, acc)
		}
		return start, acc, true

	case syntax.OpRepeat: // X{m,n}
		if len(re.Sub) != 1 {
			return nil, nil, false
		}
		min := re.Min
		max := re.Max
		if max >= 0 && min > max {
			return nil, nil, false
		}
		if max > 64 { // avoid state explosion
			return nil, nil, false
		}
		s0, a0, ok := buildNFA(re.Sub[0])
		if !ok {
			return nil, nil, false
		}
		start := s0
		lastAcc := a0
		for i := 1; i < min; i++ { // already have one copy
			si, ai, ok := buildNFA(re.Sub[0])
			if !ok {
				return nil, nil, false
			}
			lastAcc.eps = append(lastAcc.eps, si)
			lastAcc = ai
		}
		if max == min {
			lastAcc.acc = true
			return start, lastAcc, true
		}
		if max < 0 { // unbounded: X{min,} == X^min (X*)
			finalAcc := &nfaState{acc: true}
			// Option to stop after reaching min
			lastAcc.eps = append(lastAcc.eps, finalAcc)
			// Build X* loop
			ls, la, ok := buildNFA(re.Sub[0])
			if !ok {
				return nil, nil, false
			}
			lastAcc.eps = append(lastAcc.eps, ls) // take one more
			la.eps = append(la.eps, ls)           // loop
			la.eps = append(la.eps, finalAcc)     // or exit
			return start, finalAcc, true
		}
		finalAcc := &nfaState{acc: true}
		lastAcc.eps = append(lastAcc.eps, finalAcc) // option to stop at min
		for i := min; i < max; i++ {
			si, ai, ok := buildNFA(re.Sub[0])
			if !ok {
				return nil, nil, false
			}
			lastAcc.eps = append(lastAcc.eps, si) // take another copy
			ai.eps = append(ai.eps, finalAcc)     // or stop here
			lastAcc = ai
		}
		return start, finalAcc, true

	case syntax.OpBeginText, syntax.OpBeginLine:
		// Anchors are epsilon; since we match whole string, begin anchors are satisfied at start
		s := &nfaState{}
		acc := &nfaState{acc: true}
		s.eps = append(s.eps, acc)
		return s, acc, true

	case syntax.OpEndText, syntax.OpEndLine:
		// End anchors: epsilon; accept must be reached at the end of input
		s := &nfaState{}
		acc := &nfaState{acc: true}
		s.eps = append(s.eps, acc)
		return s, acc, true

	default:
		// Unsupported: alternation, capture, repeat ranges, etc.
		return nil, nil, false
	}
}

func inRanges(rng []rune, r rune) bool {
	for i := 0; i+1 < len(rng); i += 2 {
		lo := rng[i]
		hi := rng[i+1]
		if r >= lo && r <= hi {
			return true
		}
	}
	return false
}

func step(states map[*nfaState]struct{}, r rune) map[*nfaState]struct{} {
	next := make(map[*nfaState]struct{}, 8)
	for st := range states {
		for _, e := range st.trans {
			if e.any {
				next[e.next] = struct{}{}
				continue
			}
			if e.useCh {
				if e.ch == r {
					next[e.next] = struct{}{}
				}
				continue
			}
			if len(e.ranges) > 0 {
				if inRanges(e.ranges, r) {
					next[e.next] = struct{}{}
				}
			}
		}
	}
	return epsilonClosure(next)
}

func epsilonClosure(states map[*nfaState]struct{}) map[*nfaState]struct{} {
	stack := make([]*nfaState, 0, len(states))
	for st := range states {
		stack = append(stack, st)
	}
	for len(stack) > 0 {
		st := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		for _, e := range st.eps {
			if _, ok := states[e]; !ok {
				states[e] = struct{}{}
				stack = append(stack, e)
			}
		}
	}
	return states
}

func nfaStart(start *nfaState) map[*nfaState]struct{} {
	s := map[*nfaState]struct{}{start: {}}
	return epsilonClosure(s)
}

func nfaMatch(start, acc *nfaState, input []byte) bool {
	states := nfaStart(start)
	for _, b := range input {
		// Treat bytes as runes in 0..255
		states = step(states, rune(b))
		if len(states) == 0 {
			return false
		}
	}
	for st := range states {
		if st.acc || st == acc {
			return true
		}
	}
	return false
}

func nfaViable(start *nfaState, prefix []byte) bool {
	states := nfaStart(start)
	for _, b := range prefix {
		states = step(states, rune(b))
		if len(states) == 0 {
			return false
		}
	}
	// If any state reachable, some extension may still accept
	return len(states) > 0
}
