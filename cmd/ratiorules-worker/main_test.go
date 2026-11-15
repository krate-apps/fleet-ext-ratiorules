package main

import "testing"

func TestFirstMatchPrivatePublic(t *testing.T) {
	rules := []rule{
		{Name: "priv", Match: matchPrivate, TargetLabel: "seed", Enabled: true},
		{Name: "pub", Match: matchPublic, TargetLabel: "free", Enabled: true},
	}
	r, _, ok := firstMatch(rules, torrentRow{Hash: "A", Private: true})
	if !ok || r.Name != "priv" {
		t.Fatalf("private: %#v ok=%v", r, ok)
	}
	r, _, ok = firstMatch(rules, torrentRow{Hash: "B", Private: false})
	if !ok || r.Name != "pub" {
		t.Fatalf("public: %#v ok=%v", r, ok)
	}
}

func TestParseAndValidateRules(t *testing.T) {
	rules, err := parseRulesText("priv|private||seed|on\ntrk|tracker_contain|example.org|tv|off")
	if err != nil {
		t.Fatal(err)
	}
	if len(rules) != 2 || rules[0].Match != "private" || rules[1].TargetLabel != "tv" {
		t.Fatalf("%#v", rules)
	}
	cfg := &config{Enabled: true, Rules: rules}
	if err := validateConfig(cfg); err != nil {
		t.Fatal(err)
	}
	if err := validateConfig(&config{Rules: []rule{{Name: "x", Match: "nope", TargetLabel: "a"}}}); err == nil {
		t.Fatal("expected match error")
	}
}
