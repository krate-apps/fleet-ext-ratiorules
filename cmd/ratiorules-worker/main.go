// Command ratiorules-worker assigns labels from match rules (ruTorrent extratio parity).
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

type request struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	ID      any             `json:"id"`
}

// match kinds mirror RR_* in ruTorrent extratio/rules.php.
const (
	matchLabelContain   = "label_contain"
	matchTrackerContain = "tracker_contain"
	matchPrivate        = "private"
	matchPublic         = "public"
)

type rule struct {
	Name        string `json:"name"`
	Match       string `json:"match"`
	Pattern     string `json:"pattern"`
	TargetLabel string `json:"targetLabel"`
	Enabled     bool   `json:"enabled"`
}

type config struct {
	Enabled bool   `json:"enabled"`
	Rules   []rule `json:"rules"`
}

type torrentRow struct {
	Hash    string `json:"hash"`
	Name    string `json:"name"`
	Label   string `json:"label"`
	Private bool   `json:"private"`
}

type trackerRow struct {
	URL string `json:"url"`
}

var (
	mu       sync.Mutex
	nextHost = int64(1)
	pending  = map[int64]chan hostReply{}
	outMu    sync.Mutex
)

type hostReply struct {
	result json.RawMessage
	err    string
}

func main() {
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 0, 64*1024), 8<<20)
	for in.Scan() {
		line := in.Bytes()
		if len(line) == 0 {
			continue
		}
		var probe map[string]json.RawMessage
		if err := json.Unmarshal(line, &probe); err != nil {
			continue
		}
		if _, hasMethod := probe["method"]; !hasMethod {
			var resp struct {
				ID     any             `json:"id"`
				Result json.RawMessage `json:"result"`
				Error  *struct {
					Message string `json:"message"`
				} `json:"error"`
			}
			if err := json.Unmarshal(line, &resp); err != nil {
				continue
			}
			idNum, ok := asInt64(resp.ID)
			if !ok {
				continue
			}
			mu.Lock()
			ch := pending[idNum]
			delete(pending, idNum)
			mu.Unlock()
			if ch == nil {
				continue
			}
			msg := ""
			if resp.Error != nil {
				msg = resp.Error.Message
			}
			ch <- hostReply{result: resp.Result, err: msg}
			continue
		}

		var req request
		if err := json.Unmarshal(line, &req); err != nil {
			continue
		}
		go handleRequest(req)
	}
}

func handleRequest(req request) {
	switch req.Method {
	case "fleet.handshake", "fleet.ping", "ext.activate", "ext.deactivate", "ext.health", "ext.upgrade":
		reply(req.ID, map[string]any{"ok": true})
	case "fleet.shutdown":
		reply(req.ID, map[string]any{"ok": true})
		os.Exit(0)
	case "ext.ratiorules.get":
		cfg, err := loadConfig()
		if err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		reply(req.ID, map[string]any{
			"enabled":   cfg.Enabled,
			"rules":     cfg.Rules,
			"rulesText": rulesText(cfg.Rules),
		})
	case "ext.ratiorules.save":
		var p struct {
			Enabled   *bool  `json:"enabled"`
			RulesText string `json:"rulesText"`
			Rules     []rule `json:"rules"`
		}
		if err := json.Unmarshal(req.Params, &p); err != nil {
			replyErr(req.ID, "invalid config")
			return
		}
		cfg, err := loadConfig()
		if err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		if p.Enabled != nil {
			cfg.Enabled = *p.Enabled
		}
		if strings.TrimSpace(p.RulesText) != "" {
			parsed, err := parseRulesText(p.RulesText)
			if err != nil {
				replyErr(req.ID, err.Error())
				return
			}
			cfg.Rules = parsed
		} else if p.Rules != nil {
			cfg.Rules = p.Rules
		}
		if err := validateConfig(cfg); err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		if err := saveConfig(cfg); err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		reply(req.ID, map[string]any{
			"ok":        true,
			"enabled":   cfg.Enabled,
			"rules":     cfg.Rules,
			"rulesText": rulesText(cfg.Rules),
		})
	case "ext.ratiorules.apply":
		res, err := applyRules(false)
		if err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		reply(req.ID, res)
	case "ext.ratiorules.preview":
		res, err := applyRules(true)
		if err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		reply(req.ID, res)
	default:
		replyErr(req.ID, fmt.Sprintf("method not found: %s", req.Method))
	}
}

func validateConfig(cfg *config) error {
	if cfg == nil {
		return fmt.Errorf("config required")
	}
	if len(cfg.Rules) > 100 {
		return fmt.Errorf("too many rules (max 100)")
	}
	seen := map[string]struct{}{}
	for i, r := range cfg.Rules {
		name := strings.TrimSpace(r.Name)
		if name == "" {
			return fmt.Errorf("rules[%d]: name required", i)
		}
		key := strings.ToLower(name)
		if _, dup := seen[key]; dup {
			return fmt.Errorf("duplicate rule name %q", name)
		}
		seen[key] = struct{}{}
		match := strings.ToLower(strings.TrimSpace(r.Match))
		switch match {
		case matchLabelContain, matchTrackerContain, matchPrivate, matchPublic:
		default:
			return fmt.Errorf("rules[%d]: match must be label_contain|tracker_contain|private|public", i)
		}
		if (match == matchLabelContain || match == matchTrackerContain) && strings.TrimSpace(r.Pattern) == "" && match == matchTrackerContain {
			return fmt.Errorf("rules[%d]: pattern required for tracker_contain", i)
		}
		if strings.TrimSpace(r.TargetLabel) == "" {
			return fmt.Errorf("rules[%d]: targetLabel required", i)
		}
	}
	return nil
}

func applyRules(dryRun bool) (map[string]any, error) {
	cfg, err := loadConfig()
	if err != nil {
		return nil, err
	}
	if !cfg.Enabled {
		return map[string]any{"ok": true, "applied": 0, "matches": []map[string]any{}, "reason": "disabled", "dryRun": dryRun}, nil
	}
	raw, err := hostCall("fleet.torrents.list", map[string]any{})
	if err != nil {
		return nil, err
	}
	var wrap struct {
		Torrents []torrentRow `json:"torrents"`
	}
	if err := json.Unmarshal(raw, &wrap); err != nil {
		return nil, err
	}
	applied := 0
	matches := make([]map[string]any, 0)
	details := []map[string]any{}
	for _, t := range wrap.Torrents {
		rule, trackers, ok := firstMatch(cfg.Rules, t)
		if !ok {
			continue
		}
		target := strings.TrimSpace(rule.TargetLabel)
		if strings.EqualFold(strings.TrimSpace(t.Label), target) {
			continue
		}
		hit := map[string]any{
			"hash":        t.Hash,
			"name":        t.Name,
			"rule":        rule.Name,
			"targetLabel": target,
			"label":       t.Label,
		}
		matches = append(matches, hit)
		if dryRun {
			continue
		}
		if _, err := hostCall("fleet.torrents.setLabel", map[string]any{
			"hash":  t.Hash,
			"label": target,
		}); err != nil {
			details = append(details, map[string]any{"hash": t.Hash, "ok": false, "error": err.Error()})
			continue
		}
		applied++
		details = append(details, map[string]any{
			"hash":     t.Hash,
			"ok":       true,
			"rule":     rule.Name,
			"label":    target,
			"trackers": len(trackers),
		})
	}
	out := map[string]any{
		"ok":      true,
		"dryRun":  dryRun,
		"matches": matches,
		"matched": len(matches),
		"checked": len(wrap.Torrents),
	}
	if dryRun {
		out["applied"] = 0
	} else {
		out["applied"] = applied
		out["details"] = details
	}
	return out, nil
}

func firstMatch(rules []rule, t torrentRow) (rule, []string, bool) {
	var trackers []string
	trackersLoaded := false
	for _, r := range rules {
		if !r.Enabled {
			continue
		}
		match := strings.ToLower(strings.TrimSpace(r.Match))
		switch match {
		case matchLabelContain:
			pat := r.Pattern
			if strings.Contains(strings.ToLower(t.Label), strings.ToLower(pat)) || (t.Label == "" && pat == "") {
				return r, nil, true
			}
		case matchPrivate:
			if t.Private {
				return r, nil, true
			}
		case matchPublic:
			if !t.Private {
				return r, nil, true
			}
		case matchTrackerContain:
			if !trackersLoaded {
				trackers = listTrackerURLs(t.Hash)
				trackersLoaded = true
			}
			joined := strings.Join(trackers, "#")
			if strings.Contains(strings.ToLower(joined), strings.ToLower(r.Pattern)) {
				return r, trackers, true
			}
		}
	}
	return rule{}, nil, false
}

func listTrackerURLs(hash string) []string {
	raw, err := hostCall("fleet.trackers.list", map[string]any{"hash": hash})
	if err != nil {
		return nil
	}
	var wrap struct {
		Trackers []trackerRow `json:"trackers"`
	}
	if err := json.Unmarshal(raw, &wrap); err != nil {
		return nil
	}
	out := make([]string, 0, len(wrap.Trackers))
	for _, tr := range wrap.Trackers {
		if u := strings.TrimSpace(tr.URL); u != "" {
			out = append(out, u)
		}
	}
	return out
}

func rulesText(rules []rule) string {
	lines := make([]string, 0, len(rules))
	for _, r := range rules {
		flag := "off"
		if r.Enabled {
			flag = "on"
		}
		lines = append(lines, r.Name+"|"+r.Match+"|"+r.Pattern+"|"+r.TargetLabel+"|"+flag)
	}
	return strings.Join(lines, "\n")
}

func parseRulesText(text string) ([]rule, error) {
	out := make([]rule, 0)
	seen := map[string]struct{}{}
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "|", 5)
		if len(parts) < 4 {
			return nil, fmt.Errorf("expected Name|match|pattern|targetLabel[|on] on line %q", line)
		}
		name := strings.TrimSpace(parts[0])
		match := strings.ToLower(strings.TrimSpace(parts[1]))
		pattern := parts[2]
		target := strings.TrimSpace(parts[3])
		enabled := true
		if len(parts) == 5 {
			switch strings.ToLower(strings.TrimSpace(parts[4])) {
			case "", "on", "1", "true", "yes":
				enabled = true
			case "off", "0", "false", "no":
				enabled = false
			default:
				return nil, fmt.Errorf("enabled flag must be on/off on line %q", line)
			}
		}
		if name == "" || target == "" {
			return nil, fmt.Errorf("incomplete rule on line %q", line)
		}
		key := strings.ToLower(name)
		if _, dup := seen[key]; dup {
			return nil, fmt.Errorf("duplicate rule name %q", name)
		}
		seen[key] = struct{}{}
		out = append(out, rule{
			Name:        name,
			Match:       match,
			Pattern:     pattern,
			TargetLabel: target,
			Enabled:     enabled,
		})
	}
	return out, nil
}

func loadConfig() (*config, error) {
	raw, err := hostCall("fleet.storage.get", map[string]any{"key": "rules"})
	if err != nil {
		return nil, err
	}
	if string(raw) == "null" || len(raw) == 0 {
		return &config{Enabled: true, Rules: []rule{}}, nil
	}
	var cfg config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return &config{Enabled: true, Rules: []rule{}}, nil
	}
	if cfg.Rules == nil {
		cfg.Rules = []rule{}
	}
	return &cfg, nil
}

func saveConfig(cfg *config) error {
	_, err := hostCall("fleet.storage.put", map[string]any{"key": "rules", "value": cfg})
	return err
}

func hostCall(method string, params any) (json.RawMessage, error) {
	ch := make(chan hostReply, 1)
	mu.Lock()
	id := nextHost
	nextHost++
	pending[id] = ch
	mu.Unlock()
	b, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "method": method, "params": params, "id": id})
	if err != nil {
		mu.Lock()
		delete(pending, id)
		mu.Unlock()
		return nil, err
	}
	outMu.Lock()
	fmt.Fprintf(os.Stdout, "%s\n", b)
	outMu.Unlock()
	select {
	case reply := <-ch:
		if reply.err != "" {
			return nil, fmt.Errorf("%s", reply.err)
		}
		if len(reply.result) == 0 {
			return json.RawMessage("null"), nil
		}
		return reply.result, nil
	case <-time.After(45 * time.Second):
		mu.Lock()
		delete(pending, id)
		mu.Unlock()
		return nil, fmt.Errorf("host call timeout")
	}
}

func reply(id any, result any) {
	b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "result": result, "id": id})
	outMu.Lock()
	fmt.Fprintf(os.Stdout, "%s\n", b)
	outMu.Unlock()
}

func replyErr(id any, msg string) {
	b, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"error":   map[string]any{"code": -32000, "message": msg},
		"id":      id,
	})
	outMu.Lock()
	fmt.Fprintf(os.Stdout, "%s\n", b)
	outMu.Unlock()
}

func asInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case float64:
		return int64(n), true
	case int64:
		return n, true
	case int:
		return int64(n), true
	case json.Number:
		i, err := n.Int64()
		return i, err == nil
	default:
		return 0, false
	}
}
