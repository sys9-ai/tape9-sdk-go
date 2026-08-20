package tape9

import "regexp"

var idRe = regexp.MustCompile(`^[0-9A-Za-z_][0-9A-Za-z_\-/.]{2,511}$`)

// IsValidID reports whether s matches the public tape9 identifier contract.
func IsValidID(s string) bool {
	return idRe.MatchString(s)
}
