package tape9

import "testing"

func TestIsValidID(t *testing.T) {
	if !IsValidID("acme/dev") {
		t.Fatalf("expected slash-bearing id to be valid")
	}
	if IsValidID("ab") {
		t.Fatalf("expected undersized identifier to be invalid")
	}
}
