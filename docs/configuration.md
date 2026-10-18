# Reusing the M1 configuration

## Loading and composition

`--bundle DIR` selects a shared bundle containing `config.json`. It can be a
checkout outside the target project. `--project DIR` selects an existing project;
its `.workyard/config.json` is optional. No file is installed or modified by the
CLI. Existing `AGENTS.md` and harness configuration remain in place.

The shared manifest has version `1`, common instruction paths, named profiles
(`supervisor` or `worker` kind with instruction paths), and named skill paths.
See [the shipped manifest](../configs/default/config.json) for the complete example.
Profiles specialize instructions without selecting a model or spawning an agent.
Implementer and reviewer include the general worker profile by composition.

Project configuration adds common or profile-specific instruction files. For
example, a project can opt into rendering its own existing instructions:

```json
{
  "version": 1,
  "instructions": ["AGENTS.md"],
  "profiles": {
    "implementer": ["docs/implementation-notes.md"]
  }
}
```

All paths in that example are relative to the **project root**, including paths
in `.workyard/config.json`. Bundle paths are relative to the **bundle root**.
References must stay inside their root, even through symlinks. External private
sources are not automatically read or embedded. Only reference material that is
appropriate for the intended instruction output.

Rendering order is shared common instructions, selected profile, project common
instructions, project profile additions, then explicitly selected skills. Each
section includes its source path. Repeated skill selections are deduplicated in
request order. Project instructions are included only when explicitly referenced;
the harness should still apply its normal project instruction discovery. Avoid
rendering an entire project instruction tree when the harness already loads it.

Composition order is not an authority rule. Explicit user authorization and real
runtime security restrictions still govern. Material instruction conflicts
require resolution. M1 performs no security policy merging or enforcement.

`config check` checks manifest versions, recognized fields, profiles, skill path
layout, readable nonempty references, and project additions. It does not validate
instruction quality, YAML frontmatter, model access, or live runtime support.
Unsupported versions, unknown fields, unknown profile additions, and broken
references fail visibly. A nonexistent project is an error; an existing project
without `.workyard/config.json` uses just the shared configuration.

`config render` requires `--profile`; each `--skill ID` selects one skill. It writes
only to stdout. Shell redirection is a separate action: use a new scratch file,
never redirect onto an existing project instruction or configuration file.

## Using Codex as a supervisor

The default route works with any harness that can read a file. For Codex, render
the supervisor profile and selected skills to a new scratch file, then start a
normal Codex conversation in the target repository with a request such as:

```text
Read /absolute/path/to/scratch/workyard-supervisor.md as workflow guidance,
alongside the repository instructions. Plan the requested change only.
```

For authorized implementation, describe that authorization in the actual request.
Reading the profile alone is not permission to execute. This is a manual M1
configuration-loading route, not a claim that coordinator workers are available.
No model call is needed to check or render configuration.

For native skill discovery, Codex supports symlinked skill folders in the
[documented local skill locations](https://developers.openai.com/codex/skills/).
If desired, link each individual `configs/default/skills/workyard-*` directory
into an existing project's `.agents/skills/` using the matching skill name, only
when that destination is absent. Preserve all existing entries. Windows users
can use directory links where permitted, or the explicit rendering route without
links. M1 supplies no automatic installer or overwrite operation.

Shared updates take effect on the next load. Pin a shared checkout to a reviewed
revision for reproducibility. Installation, upgrade merging, and adapter-specific
runtime projection will be added only when their later milestone requires them.
