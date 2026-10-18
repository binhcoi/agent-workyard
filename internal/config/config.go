// Package config loads reusable instructions without installing or modifying files.
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

type Profile struct {
	Kind         string   `json:"kind"`
	Instructions []string `json:"instructions"`
}

type Manifest struct {
	Version      int                `json:"version"`
	Instructions []string           `json:"instructions"`
	Profiles     map[string]Profile `json:"profiles"`
	Skills       map[string]string  `json:"skills"`
}

// Project additions never replace the shared bundle or a project's AGENTS.md.
type Project struct {
	Version      int                 `json:"version"`
	Instructions []string            `json:"instructions"`
	Profiles     map[string][]string `json:"profiles"`
}

type Document struct {
	Path string
	Text string
}

type Loaded struct {
	Manifest  Manifest
	Common    []Document
	Profiles  map[string][]Document
	Skills    map[string]Document
	Project   []Document
	Additions map[string][]Document
}

// Load reads a bundle and, optionally, .workyard/config.json in projectDir.
// References are local to their manifest's directory tree, including symlinks.
func Load(bundleDir, projectDir string) (*Loaded, error) {
	root, err := os.OpenRoot(bundleDir)
	if err != nil {
		return nil, fmt.Errorf("open bundle: %w", err)
	}
	defer root.Close()
	var m Manifest
	if err := decode(root, "config.json", &m); err != nil {
		return nil, err
	}
	if m.Version != 1 {
		return nil, fmt.Errorf("bundle version %d: supported version is 1", m.Version)
	}
	if len(m.Profiles) == 0 {
		return nil, fmt.Errorf("bundle must define profiles")
	}
	l := &Loaded{Manifest: m, Profiles: map[string][]Document{}, Skills: map[string]Document{}, Additions: map[string][]Document{}}
	if l.Common, err = documents(root, m.Instructions); err != nil {
		return nil, err
	}
	for _, id := range keys(m.Profiles) {
		p := m.Profiles[id]
		if !validID(id) || (p.Kind != "supervisor" && p.Kind != "worker") || len(p.Instructions) == 0 {
			return nil, fmt.Errorf("invalid profile %q: require an ID, supervisor/worker kind and instructions", id)
		}
		if l.Profiles[id], err = documents(root, p.Instructions); err != nil {
			return nil, fmt.Errorf("profile %q: %w", id, err)
		}
	}
	for _, id := range keys(m.Skills) {
		path := m.Skills[id]
		if !validID(id) || filepath.Base(path) != "SKILL.md" || filepath.Base(filepath.Dir(path)) != id {
			return nil, fmt.Errorf("skill %q must reference its own <id>/SKILL.md", id)
		}
		docs, err := documents(root, []string{path})
		if err != nil {
			return nil, fmt.Errorf("skill %q: %w", id, err)
		}
		l.Skills[id] = docs[0]
	}
	if projectDir != "" {
		if err := l.loadProject(projectDir); err != nil {
			return nil, err
		}
	}
	return l, nil
}

func (l *Loaded) loadProject(dir string) error {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return fmt.Errorf("open project: %w", err)
	}
	defer root.Close()
	var p Project
	if err := decode(root, ".workyard/config.json", &p); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if p.Version != 1 {
		return fmt.Errorf("project version %d: supported version is 1", p.Version)
	}
	if l.Project, err = documents(root, p.Instructions); err != nil {
		return err
	}
	for _, id := range keys(p.Profiles) {
		if _, ok := l.Profiles[id]; !ok {
			return fmt.Errorf("project refers to unknown profile %q", id)
		}
		if l.Additions[id], err = documents(root, p.Profiles[id]); err != nil {
			return err
		}
	}
	return nil
}

// Render includes only explicitly requested skills. Order is bundle common,
// profile, project common, project profile additions, then selected skills.
// Order is composition, not security precedence or permission enforcement.
func (l *Loaded) Render(profile string, skills []string) (string, error) {
	profileDocs, ok := l.Profiles[profile]
	if !ok {
		return "", fmt.Errorf("unknown profile %q", profile)
	}
	docs := append([]Document{}, l.Common...)
	docs = append(docs, profileDocs...)
	docs = append(docs, l.Project...)
	docs = append(docs, l.Additions[profile]...)
	seen := map[string]bool{}
	for _, id := range skills {
		doc, ok := l.Skills[id]
		if !ok {
			return "", fmt.Errorf("unknown skill %q", id)
		}
		if !seen[id] {
			docs = append(docs, doc)
			seen[id] = true
		}
	}
	var out strings.Builder
	for _, doc := range docs {
		fmt.Fprintf(&out, "<!-- workyard source: %s -->\n%s\n\n", doc.Path, strings.TrimSpace(doc.Text))
	}
	return out.String(), nil
}

func decode(root *os.Root, path string, v any) error {
	f, err := root.Open(path)
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	defer f.Close()
	d := json.NewDecoder(f)
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("decode %s: expected exactly one JSON object", path)
	}
	return nil
}

func documents(root *os.Root, paths []string) ([]Document, error) {
	docs := make([]Document, 0, len(paths))
	for _, path := range paths {
		if !filepath.IsLocal(path) {
			return nil, fmt.Errorf("instruction path %q must stay within its root", path)
		}
		f, err := root.Open(path)
		if err != nil {
			return nil, fmt.Errorf("read instruction %s: %w", path, err)
		}
		data, err := io.ReadAll(f)
		f.Close()
		if err != nil {
			return nil, fmt.Errorf("read instruction %s: %w", path, err)
		}
		if strings.TrimSpace(string(data)) == "" {
			return nil, fmt.Errorf("instruction %s is empty", path)
		}
		docs = append(docs, Document{Path: filepath.Join(root.Name(), path), Text: string(data)})
	}
	return docs, nil
}

func validID(id string) bool {
	if id == "" {
		return false
	}
	for _, r := range id {
		if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-') {
			return false
		}
	}
	return true
}

func keys[V any](m map[string]V) []string {
	ids := make([]string, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}
