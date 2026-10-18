package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"agentworkyard/internal/config"
)

const usage = `Usage:
  workyard config check --bundle DIR [--project DIR]
  workyard config render --bundle DIR --profile ID [--project DIR] [--skill ID ...]

  workyard coordinator serve --state DIR --model MODEL [--capacity N] [--codex PATH]
  workyard coordinator resources|runtime --state DIR
  workyard assignment put --state DIR --id ID --project DIR --supervisor ID --objective TEXT
  workyard assignment list --state DIR
  workyard task submit --state DIR --id ID --assignment ID --project DIR --brief FILE --bundle DIR [--profile ID] [--skill ID ...] [--effort LEVEL]
  workyard task status|result|conversation|events|cancel --state DIR --id ID
  workyard task attempts --state DIR --id ID
  workyard task resolve --state DIR --id ID --attempt ID [--action reconcile|cancel|retry] [--reason TEXT]
  workyard task list --state DIR

Run the coordinator separately from supervisor sessions. M2 workers use read-only execution.
`

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "workyard:", err)
		os.Exit(1)
	}
}

type skillFlags []string

func (s *skillFlags) String() string { return fmt.Sprint([]string(*s)) }
func (s *skillFlags) Set(value string) error {
	*s = append(*s, value)
	return nil
}

func run(args []string, out, stderr io.Writer) error {
	if len(args) == 1 && (args[0] == "--help" || args[0] == "-h") {
		_, err := io.WriteString(out, usage)
		return err
	}
	if len(args) > 0 && args[0] != "config" {
		return execution(args, out, stderr)
	}
	if len(args) < 2 || args[0] != "config" || (args[1] != "check" && args[1] != "render") {
		return errors.New(usage)
	}
	f := flag.NewFlagSet("config "+args[1], flag.ContinueOnError)
	f.SetOutput(stderr)
	bundle := f.String("bundle", "", "shared configuration bundle directory (required)")
	project := f.String("project", "", "target project directory")
	var profile string
	var skills skillFlags
	if args[1] == "render" {
		f.StringVar(&profile, "profile", "", "profile ID (required)")
		f.Var(&skills, "skill", "task skill ID (repeatable)")
	}
	if err := f.Parse(args[2:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if *bundle == "" || f.NArg() != 0 {
		return errors.New("--bundle is required; positional arguments are not accepted")
	}
	if args[1] == "render" && profile == "" {
		return errors.New("--profile is required for render")
	}
	l, err := config.Load(*bundle, *project)
	if err != nil {
		return err
	}
	if args[1] == "check" {
		_, err = fmt.Fprintf(out, "Configuration valid: %d profiles, %d skills. No files changed.\n", len(l.Profiles), len(l.Skills))
		return err
	}
	text, err := l.Render(profile, skills)
	if err != nil {
		return err
	}
	_, err = io.WriteString(out, text)
	return err
}
