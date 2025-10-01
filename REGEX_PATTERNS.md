# Regex Pattern Guide for go-live-srad

## Basic Rule

**Regex in Go (and go-live-srad) matches the ENTIRE key value, not substrings!**

Therefore, you must use `.*` to match any characters before/after the fragment you're interested in.

---

## Basic Patterns

### 1. Exact Match

```go
// Find exactly "user:alice"
pattern := "^user:alice$"
re := regexp.MustCompile(pattern)
iter, _ := store.RegexSearch(ctx, re, nil)

// Or without anchors (automatically matches the whole string):
pattern := "user:alice"
```

**Example:**
- ✅ Will find: `user:alice`
- ❌ Will NOT find: `user:alice:admin`, `prefix:user:alice`

---

### 2. Prefix Match (starts with...)

```go
// Find everything starting with "user:"
pattern := "^user:.*"
re := regexp.MustCompile(pattern)
```

**Example:**
- ✅ Will find: `user:alice`, `user:bob`, `user:123:admin`
- ❌ Will NOT find: `myuser:alice`, `admin:user:bob`

**⚠️ Common mistake:**
```go
// ❌ WRONG - won't work!
pattern := "^user:"  // Missing .* at the end

// ✅ CORRECT
pattern := "^user:.*"
```

---

### 3. Suffix Match (ends with...)

```go
// Find everything ending with ":admin"
pattern := ".*:admin$"
re := regexp.MustCompile(pattern)
```

**Example:**
- ✅ Will find: `user:alice:admin`, `group:123:admin`
- ❌ Will NOT find: `admin`, `admin:user`

**⚠️ Common mistake:**
```go
// ❌ WRONG
pattern := ":admin$"  // Missing .* at the beginning

// ✅ CORRECT
pattern := ".*:admin$"
```

---

### 4. Substring Match (contains...)

```go
// Find everything containing "admin"
pattern := ".*admin.*"
re := regexp.MustCompile(pattern)
```

**Example:**
- ✅ Will find: `admin`, `user:admin`, `admin:root`, `myadmin:data`
- ❌ Will NOT find: `admn`, `user:root`

**⚠️ Common mistake:**
```go
// ❌ WRONG
pattern := "admin"  // Only exact match

// ✅ CORRECT
pattern := ".*admin.*"
```

---

## Advanced Patterns

### 5. Prefix with Specific Format

```go
// user:ID where ID is numbers
pattern := "^user:[0-9]+$"

// user:name where name is letters
pattern := "^user:[a-z]+$"

// user:anything:admin
pattern := "^user:.*:admin$"
```

**Example:**
```go
pattern := "^user:[0-9]+$"
// ✅ Will find: user:123, user:456
// ❌ Will NOT find: user:abc, user:123:admin
```

---

### 6. Alternation (OR)

```go
// Find keys starting with "user:" OR "admin:"
pattern := "^(user|admin):.*"

// Find keys ending with ":active" OR ":pending"
pattern := ".*(active|pending)$"
```

**Example:**
```go
pattern := "^(user|admin|guest):.*"
// ✅ Will find: user:alice, admin:root, guest:bob
// ❌ Will NOT find: moderator:alice
```

---

### 7. Character Classes

```go
// Starts with letter u, p or a
pattern := "^[upa].*"

// Email-like pattern
pattern := "^email:.*@.*\\..*$"

// UUID-like pattern
pattern := "^[0-9a-f]{8}-[0-9a-f]{4}-.*"
```

---

### 8. Quantifiers

```go
// Exactly 3 digits
pattern := "^user:[0-9]{3}$"

// 3 to 10 characters
pattern := "^user:[a-z]{3,10}$"

// At least one digit
pattern := "^user:[0-9]+$"

// Zero or more letters
pattern := "^user:[a-z]*$"

// Zero or one letter
pattern := "^user:[a-z]?$"
```

---

## Practical Examples for Different Use Cases

### Example 1: User System with Roles

```go
// Key structure: "user:ID:role" e.g. "user:alice:admin"

// All users
pattern := "^user:.*"

// Specific user with any role
pattern := "^user:alice:.*"

// All admins (any ID)
pattern := "^user:.*:admin$"

// Specific user with specific role
pattern := "^user:alice:admin$"
```

### Example 2: Logs with Timestamps

```go
// Structure: "log:2024:01:15:12:30:ERROR:message"

// All logs from 2024
pattern := "^log:2024:.*"

// Logs from January 2024
pattern := "^log:2024:01:.*"

// All errors (any date)
pattern := "^log:.*:ERROR:.*"

// Errors from specific day
pattern := "^log:2024:01:15:.*:ERROR:.*"
```

### Example 3: E-commerce

```go
// Structure: "order:YYYY:MM:ID" + "product:category:ID" + "user:ID"

// All orders
pattern := "^order:.*"

// Orders from January 2024
pattern := "^order:2024:01:.*"

// Products from electronics category
pattern := "^product:electronics:.*"

// Products from electronics OR books category
pattern := "^product:(electronics|books):.*"
```

### Example 4: Cache Keys

```go
// Structure: "cache:domain:key"

// All cache keys
pattern := "^cache:.*"

// Cache for specific domain
pattern := "^cache:users:.*"

// Expired sessions (if key contains "expired")
pattern := "^cache:session:.*:expired$"
```

---

## Complete Code Example

```go
package main

import (
    "context"
    "fmt"
    "regexp"
    "github.com/CVDpl/go-live-srad/pkg/srad"
)

func main() {
    store, _ := srad.Open("./data", nil)
    defer store.Close()
    
    ctx := context.Background()
    
    // Insert data
    store.Insert([]byte("user:alice:admin"))
    store.Insert([]byte("user:bob:user"))
    store.Insert([]byte("product:laptop:electronics"))
    store.Insert([]byte("order:2024:01:001"))
    
    // === Example 1: Prefix match ===
    re1 := regexp.MustCompile("^user:.*")
    iter1, _ := store.RegexSearch(ctx, re1, nil)
    fmt.Println("Users:")
    for iter1.Next(ctx) {
        fmt.Printf("  - %s\n", iter1.String())
    }
    iter1.Close()
    // Result: user:alice:admin, user:bob:user
    
    // === Example 2: Suffix match ===
    re2 := regexp.MustCompile(".*:admin$")
    iter2, _ := store.RegexSearch(ctx, re2, nil)
    fmt.Println("Admins:")
    for iter2.Next(ctx) {
        fmt.Printf("  - %s\n", iter2.String())
    }
    iter2.Close()
    // Result: user:alice:admin
    
    // === Example 3: Alternation ===
    re3 := regexp.MustCompile("^(user|product):.*")
    iter3, _ := store.RegexSearch(ctx, re3, nil)
    fmt.Println("Users and Products:")
    for iter3.Next(ctx) {
        fmt.Printf("  - %s\n", iter3.String())
    }
    iter3.Close()
    // Result: user:alice:admin, user:bob:user, product:laptop:electronics
    
    // === Example 4: Complex pattern ===
    re4 := regexp.MustCompile("^order:2024:0[12]:.*")
    iter4, _ := store.RegexSearch(ctx, re4, nil)
    fmt.Println("Orders from Jan-Feb 2024:")
    for iter4.Next(ctx) {
        fmt.Printf("  - %s\n", iter4.String())
    }
    iter4.Close()
    // Result: order:2024:01:001
}
```

---

## Quick Pattern Reference (Cheat Sheet)

| Goal | ❌ Wrong | ✅ Correct |
|------|----------|-----------|
| Prefix "user:" | `^user:` | `^user:.*` |
| Suffix ":admin" | `:admin$` | `.*:admin$` |
| Contains "admin" | `admin` | `.*admin.*` |
| Exact "user:alice" | `^user:alice` | `^user:alice$` or `user:alice` |
| Starts with "user:" or "admin:" | `^user:\|admin:` | `^(user\|admin):.*` |
| Contains digits | `[0-9]+` | `.*[0-9]+.*` |

---

## Debugging Patterns

If your pattern doesn't work as expected:

1. **Check if it matches the whole string:**
   ```go
   // Local test
   re := regexp.MustCompile("your-pattern")
   fmt.Println(re.MatchString("test-key"))  // true/false
   ```

2. **Add .* at the beginning/end:**
   - If searching for prefix: `^prefix.*`
   - If searching for suffix: `.*suffix$`
   - If searching for substring: `.*substring.*`

3. **Use an online regex tester:**
   - Paste your pattern
   - Test on sample keys
   - Remember: select "Go" mode (not PCRE)

---

## Optimizations

### Fast Path for Exact Match
```go
// This is optimized:
pattern := "^exact-key-name$"
// go-live-srad will use fast lookup path
```

### Literal Prefix
```go
// This allows faster filtering:
pattern := "^user:alice.*"
// Engine can use literal prefix "user:alice" for speedup
```

---

## Summary

✅ **Always use:**
- `^` at the beginning when you want to control the start of the key
- `$` at the end when you want to control the end of the key  
- `.*` to match "anything" before/after/between patterns

❌ **Avoid:**
- Patterns without `.*` when you don't want exact match
- Too general patterns like `.*` (slow, scan everything)
- Very complicated regex (may be slow)

💡 **Pro tip:** If you know the exact key, use `^exact-key$` - this triggers the fast path optimization!
