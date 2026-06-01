package client

import "testing"

func TestParseDepthDecimal_RoundTrip(t *testing.T) {
	cases := []string{
		"100.00",
		"0.00000001",
		"100",
		"151.25",
		"-5.5",
		"0.00",
		"999999.99999999",
	}
	for _, c := range cases {
		d := parseDepthDecimal(c)
		if got := d.String(); got != c {
			t.Errorf("parseDepthDecimal(%q).String() = %q, want exact round-trip", c, got)
		}
	}
}

func TestParseDepthDecimal_Empty(t *testing.T) {
	d := parseDepthDecimal("")
	if d.Value() != 0 {
		t.Errorf("empty string should yield zero decimal, got value %d", d.Value())
	}
}

func TestParseDepthDecimal_PrecisionDerived(t *testing.T) {
	// "100.00" → precision 2; raw value should be 10000.
	d := parseDepthDecimal("100.00")
	if d.Precision() != 2 {
		t.Errorf("precision = %d, want 2", d.Precision())
	}
	if d.Value() != 10000 {
		t.Errorf("value = %d, want 10000", d.Value())
	}
}
