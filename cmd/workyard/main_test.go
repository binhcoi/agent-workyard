package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestShippedBundleFromAnotherRepository(t *testing.T) {
	bundle, err := filepath.Abs("../../configs/default")
	if err != nil {
		t.Fatal(err)
	}
	project := t.TempDir()
	if err := os.WriteFile(filepath.Join(project, "AGENTS.md"), []byte("Keep existing project instructions.\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, profile := range []string{"supervisor", "general-worker", "implementer", "reviewer"} {
		t.Run(profile, func(t *testing.T) {
			var out, stderr bytes.Buffer
			err := run([]string{"config", "render", "--bundle", bundle, "--project", project, "--profile", profile, "--skill", "workyard-plan", "--skill", "workyard-handoff", "--skill", "workyard-verify"}, &out, &stderr)
			if err != nil || out.Len() == 0 || stderr.Len() != 0 {
				t.Fatalf("render: %v, stdout=%q, stderr=%q", err, out.String(), stderr.String())
			}
		})
	}
	var out, stderr bytes.Buffer
	if err := run([]string{"config", "check", "--bundle", bundle, "--project", project}, &out, &stderr); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "4 profiles, 3 skills") {
		t.Fatal(out.String())
	}
	entries, err := os.ReadDir(project)
	if err != nil || len(entries) != 1 || entries[0].Name() != "AGENTS.md" {
		t.Fatalf("project modified: %v, %v", entries, err)
	}
	data, err := os.ReadFile(filepath.Join(project, "AGENTS.md"))
	if err != nil || string(data) != "Keep existing project instructions.\n" {
		t.Fatalf("project instructions changed: %s, %v", data, err)
	}
}

func TestInvalidCommandsDoNotEmitPrompt(t *testing.T) {
	for _, args := range [][]string{
		nil,
		{"start"},
		{"config", "check"},
		{"config", "render", "--bundle", "../../configs/default"},
		{"config", "render", "--bundle", "../../configs/default", "--profile", "missing"},
		{"config", "render", "--bundle", "../../configs/default", "--profile", "supervisor", "--skill", "missing"},
		{"config", "check", "--bundle", "../../configs/default", "extra"},
		{"config", "check", "--bundle", "../../configs/default", "--profile", "supervisor"},
	} {
		var out, stderr bytes.Buffer
		if err := run(args, &out, &stderr); err == nil || out.Len() != 0 {
			t.Fatalf("args %v: expected failure without stdout, got %v, %q", args, err, out.String())
		}
	}
}
