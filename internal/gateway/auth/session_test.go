package auth

import "testing"

func TestSignParseSession_RoundTrip(t *testing.T) {
	const secret = "s3cr3t"
	tok, err := SignSession(secret, "usr_1", "a@b.com", true)
	if err != nil {
		t.Fatal(err)
	}
	c, err := ParseSession(secret, tok)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if c.UserID != "usr_1" || c.Email != "a@b.com" || !c.IsAdmin {
		t.Fatalf("claims round-trip mismatch: %+v", c)
	}
}

func TestParseSession_RejectsWrongSecretAndTamper(t *testing.T) {
	tok, _ := SignSession("right", "usr_1", "a@b.com", false)

	if _, err := ParseSession("wrong", tok); err == nil {
		t.Error("a token signed with a different secret must not verify")
	}
	if _, err := ParseSession("right", tok+"x"); err == nil {
		t.Error("a tampered signature must not verify")
	}
	if _, err := ParseSession("right", "not.a.jwt"); err == nil {
		t.Error("a malformed token must not verify")
	}
}

func TestSessionAuthContext_AdminScope(t *testing.T) {
	user := SessionAuthContext(SessionClaims{UserID: "u", IsAdmin: false})
	if user.HasScope("admin:all") {
		t.Error("standard user must not have admin:all")
	}
	if !user.HasScope("trade:write") {
		t.Error("standard user should have trade:write")
	}
	admin := SessionAuthContext(SessionClaims{UserID: "a", IsAdmin: true})
	if !admin.HasScope("admin:all") {
		t.Error("admin must have admin:all")
	}
}

func TestPassword_HashAndCheck(t *testing.T) {
	hash, err := HashPassword("hunter2pw")
	if err != nil {
		t.Fatal(err)
	}
	if !CheckPassword(hash, "hunter2pw") {
		t.Error("correct password should verify")
	}
	if CheckPassword(hash, "wrong") {
		t.Error("wrong password must not verify")
	}
}
