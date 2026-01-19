# Environment Configuration

Environments isolate job instances from each other. Each environment has its own runtime space and persistence, allowing you to separate jobs by project, purpose, or any other criteria.

## Configuration Files

Configuration files match the pattern `*env*.toml` (e.g., `env.toml`, `myenv.toml`).

### Search Locations

Files are searched in the following locations, listed from **lowest to highest priority**:

| Priority | Location | Description |
|----------|----------|-------------|
| 1 (lowest) | [Packaged default](https://github.com/runtoolsio/runcore/blob/main/src/runtools/runcore/config/env.toml) | Built-in default shipped with runcore |
| 2 | `/etc/runtools/` | System-wide configuration |
| 3 | `/etc/xdg/runtools/` | XDG system configuration |
| 4 | `~/.config/runtools/` | User configuration |
| 5 (highest) | `./.runtools/config/` or `./` | Project/current directory |

**Higher priority files override lower priority files.** This means:
- Project config overrides user config
- User config overrides system config
- System config overrides packaged defaults

### How Merging Works

When the same environment ID exists in multiple files, settings are **deep-merged**:

```toml
# Packaged default (priority 1)
[[environment]]
id = "local"
[environment.persistence]
type = "sqlite"
enabled = true

# User config ~/.config/runtools/env.toml (priority 4)
[[environment]]
id = "local"
[environment.persistence]
database = "~/my-custom-path.db"
```

**Merged result:**
```toml
[[environment]]
id = "local"
[environment.persistence]
type = "sqlite"           # from packaged default
enabled = true            # from packaged default
database = "~/my-custom-path.db"  # from user config (overrides)
```

## Configuration Schema

```toml
[[environment]]
id = "local"              # Required: unique identifier
type = "local"            # Required: "local" or "isolated"
default = true            # Optional: mark as default environment

[environment.layout]
root_dir = "/custom/path" # Optional: root directory for runtime files

[environment.persistence]
type = "sqlite"           # Required: persistence type
enabled = true            # Optional: enable/disable (default: true)
database = "path/to/db"   # Optional: database path

[environment.persistence.params]
timeout = 5.0             # Optional: database-specific parameters
```

## Environment Types

### Local

A local environment is restricted to a single machine. Communication between job instances and connectors happens via Unix domain sockets, which only work on the same host. This is the standard environment type for running and monitoring jobs.

The default `local` environment is defined in the [packaged configuration](https://github.com/runtoolsio/runcore/blob/main/src/runtools/runcore/config/env.toml) and can be customized by adding your own `env.toml` file in any of the higher-priority locations.

### Isolated

An isolated environment exists only within a single process. It uses in-memory SQLite and memory-based locks with no external communication (no sockets). This makes it useful for testing and scenarios where jobs should be completely isolated from other processes.

## Default Environment

When no environment ID is specified, runcore selects the default:

1. Looks for an environment with `default = true`
2. Falls back to `id = "local"`
3. Raises `EnvironmentNotFoundError` if neither exists

## Usage in Code

```python
from runtools.runcore.env import get_env_config

# Load default environment
env_config = get_env_config()

# Load specific environment by ID
env_config = get_env_config("myproject")
```
