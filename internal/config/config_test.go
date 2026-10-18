package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func write(t *testing.T, root, path, content string) {
	t.Helper()
	name := filepath.Join(root, path)
	if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(name, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func fixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	write(t, dir, "config.json", `{
		"version":1,"instructions":["common.md"],
		"profiles":{"general":{"kind":"worker","instructions":["worker.md"]}},
		"skills":{"check":"skills/check/SKILL.md"}
	}`)
	write(t, dir, "common.md", "shared guidance")
	write(t, dir, "worker.md", "worker guidance")
	write(t, dir, "skills/check/SKILL.md", "---\nname: check\ndescription: Check the work.\n---\nselected skill")
	return dir
}

func snapshot(t *testing.T, root string) map[string]string {
	t.Helper()
	files := map[string]string{}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			files[rel] = string(data)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func TestLoadAcrossRepositoriesAndRenderAdditionsWithoutWrites(t *testing.T) {
	bundle := fixture(t)
	project := t.TempDir()
	write(t, project, "AGENTS.md", "existing project guidance")
	write(t, project, ".workyard/config.json", `{"version":1,"instructions":["AGENTS.md"],"profiles":{"general":["local.md"]}}`)
	write(t, project, "local.md", "local worker guidance")
	beforeBundle, beforeProject := snapshot(t, bundle), snapshot(t, project)
	loaded, err := Load(bundle, project)
	if err != nil {
		t.Fatal(err)
	}
	text, err := loaded.Render("general", []string{"check", "check"})
	if err != nil {
		t.Fatal(err)
	}
	previous := -1
	for _, expected := range []string{"shared guidance", "worker guidance", "existing project guidance", "local worker guidance", "selected skill"} {
		pos := strings.Index(text, expected)
		if pos <= previous {
			t.Fatalf("missing or misordered %q in %s", expected, text)
		}
		previous = pos
	}
	if strings.Count(text, "selected skill") != 1 {
		t.Fatal("repeated selection duplicated skill context")
	}
	withoutSkills, err := loaded.Render("general", nil)
	if err != nil || strings.Contains(withoutSkills, "selected skill") {
		t.Fatalf("unselected skill loaded: %q, %v", withoutSkills, err)
	}
	if !reflect.DeepEqual(beforeBundle, snapshot(t, bundle)) || !reflect.DeepEqual(beforeProject, snapshot(t, project)) {
		t.Fatal("loading or rendering modified configuration or project files")
	}
}

func TestProjectConfigurationIsOptionalButProjectMustExist(t *testing.T) {
	bundle := fixture(t)
	for _, dir := range []string{"", t.TempDir()} {
		if _, err := Load(bundle, dir); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := Load(bundle, filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("accepted a nonexistent target project")
	}
}

func TestInvalidConfiguration(t *testing.T) {
	cases := []struct {
		name string
		file string
		text string
		want string
	}{
		{"unknown field", "config.json", `{"version":1,"profiels":{}}`, "unknown field"},
		{"trailing object", "config.json", `{"version":1} {}`, "exactly one"},
		{"unsupported version", "config.json", `{"version":2}`, "version"},
		{"missing profiles", "config.json", `{"version":1}`, "define profiles"},
		{"missing instruction", "config.json", `{"version":1,"profiles":{"general":{"kind":"worker","instructions":["missing.md"]}}}`, "missing.md"},
		{"escape", "config.json", `{"version":1,"profiles":{"general":{"kind":"worker","instructions":["../private.md"]}}}`, "within its root"},
		{"absolute path", "config.json", `{"version":1,"profiles":{"general":{"kind":"worker","instructions":["/private.md"]}}}`, "within its root"},
		{"invalid profile kind", "config.json", `{"version":1,"profiles":{"general":{"kind":"daemon","instructions":["worker.md"]}}}`, "invalid profile"},
		{"invalid profile ID", "config.json", `{"version":1,"profiles":{"../general":{"kind":"worker","instructions":["worker.md"]}}}`, "invalid profile"},
		{"empty profile", "config.json", `{"version":1,"profiles":{"general":{"kind":"worker"}}}`, "invalid profile"},
		{"empty document", "worker.md", " \n", "empty"},
		{"wrong skill path", "config.json", `{"version":1,"profiles":{"general":{"kind":"worker","instructions":["worker.md"]}},"skills":{"check":"worker.md"}}`, "SKILL.md"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := fixture(t)
			write(t, dir, tc.file, tc.text)
			_, err := Load(dir, "")
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want error containing %q", err, tc.want)
			}
		})
	}
}

func TestRejectProjectErrorsRatherThanSilentlyIgnoringThem(t *testing.T) {
	for _, text := range []string{
		`{"version":2}`,
		`{"version":1,"unknown":true}`,
		`{"version":1,"instructions":["missing.md"]}`,
		`{"version":1,"profiles":{"typo":["AGENTS.md"]}}`,
		`{"version":1,"instructions":["../private.md"]}`,
	} {
		t.Run(text, func(t *testing.T) {
			project := t.TempDir()
			write(t, project, ".workyard/config.json", text)
			if _, err := Load(fixture(t), project); err == nil {
				t.Fatal("invalid project configuration was ignored")
			}
		})
	}
}

func TestSymlinkCannotReadOutsideBundle(t *testing.T) {
	bundle := fixture(t)
	outside := t.TempDir()
	write(t, outside, "private.md", "must remain outside")
	link := filepath.Join(bundle, "escape.md")
	if err := os.Symlink(filepath.Join(outside, "private.md"), link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	m := Manifest{Version: 1, Profiles: map[string]Profile{"general": {Kind: "worker", Instructions: []string{"escape.md"}}}}
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	write(t, bundle, "config.json", string(data))
	if _, err := Load(bundle, ""); err == nil {
		t.Fatal("read a symlink outside the bundle")
	}
}

func TestUnknownSelectionsReturnNoPartialPrompt(t *testing.T) {
	l, err := Load(fixture(t), "")
	if err != nil {
		t.Fatal(err)
	}
	for _, selection := range []struct {
		profile string
		skills  []string
	}{{"missing", nil}, {"general", []string{"check", "missing"}}} {
		text, err := l.Render(selection.profile, selection.skills)
		if err == nil || text != "" {
			t.Fatalf("got partial prompt %q, error %v", text, err)
		}
	}
}
