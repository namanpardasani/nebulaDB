package server

// globMatch implements Redis-style glob matching (SCAN MATCH):
//
//   - any run of bytes (including empty)
//     ?      exactly one byte
//     [abc]  one of the listed bytes;  [a-z] a range;  [^abc] / [!abc] negation
//     \x     literal x
//
// It works on bytes, like Redis. Matching is iterative with a single
// backtrack point for the most recent '*', so it runs in O(len(pat)*len(s))
// worst case instead of exponential time on patterns like "*a*a*a*b".
func globMatch(pat, s []byte) bool {
	p, i := 0, 0
	starP, starI := -1, 0
	for i < len(s) {
		if p < len(pat) {
			switch pat[p] {
			case '*':
				starP, starI = p, i
				p++
				continue
			case '?':
				p++
				i++
				continue
			case '[':
				if n, ok := matchClass(pat[p:], s[i]); ok {
					p += n
					i++
					continue
				}
			case '\\':
				if p+1 < len(pat) {
					if pat[p+1] == s[i] {
						p += 2
						i++
						continue
					}
				} else if pat[p] == s[i] { // trailing backslash is literal
					p++
					i++
					continue
				}
			default:
				if pat[p] == s[i] {
					p++
					i++
					continue
				}
			}
		}
		// Mismatch: retry from the last '*', letting it swallow one more byte.
		if starP < 0 {
			return false
		}
		starI++
		i = starI
		p = starP + 1
	}
	for p < len(pat) && pat[p] == '*' {
		p++
	}
	return p == len(pat)
}

// matchClass matches byte c against the class at the start of pat ("[...]").
// It returns the number of pattern bytes the class occupies and whether c matches.
// An unterminated class consumes the rest of the pattern (as Redis does).
func matchClass(pat []byte, c byte) (int, bool) {
	i := 1
	negate := false
	if i < len(pat) && (pat[i] == '^' || pat[i] == '!') {
		negate = true
		i++
	}
	matched := false
	for i < len(pat) && pat[i] != ']' {
		switch {
		case pat[i] == '\\' && i+1 < len(pat):
			if pat[i+1] == c {
				matched = true
			}
			i += 2
		case i+2 < len(pat) && pat[i+1] == '-' && pat[i+2] != ']':
			lo, hi := pat[i], pat[i+2]
			if lo > hi {
				lo, hi = hi, lo
			}
			if c >= lo && c <= hi {
				matched = true
			}
			i += 3
		default:
			if pat[i] == c {
				matched = true
			}
			i++
		}
	}
	if i < len(pat) {
		i++ // consume ']'
	}
	return i, matched != negate
}
