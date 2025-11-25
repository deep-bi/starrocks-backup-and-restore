# Installation Guide

Choose the installation method that best fits your needs.

## Option 1: Install from PyPI

```bash
# Create a virtual environment
python3 -m venv .venv

# Activate it
source .venv/bin/activate  # Linux/Mac
# .venv\Scripts\activate    # Windows

# Install
pip install starrocks-br

# Verify
starrocks-br --help
```

**Note:** Always activate the virtual environment before using the tool.

## Option 2: Standalone Executable

**No Python installation required.**

Download the executable for your platform from the [latest release](https://github.com/deep-bi/starrocks-backup-and-restore/releases/latest):

- **Linux**: `starrocks-br-linux-x86_64`
- **Windows**: `starrocks-br-windows-x86_64.exe`
- **macOS (Apple Silicon)**: `starrocks-br-macos-arm64`
- **macOS (Intel)**: `starrocks-br-macos-x86_64`

**Linux/macOS:**
```bash
chmod +x starrocks-br-*
./starrocks-br-linux-x86_64 --help
```

**Windows (PowerShell):**
```powershell
.\starrocks-br-windows-x86_64.exe --help
```

## Option 3: Micromamba

[Micromamba](https://mamba.readthedocs.io/en/latest/user_guide/micromamba.html) is a lightweight package manager that creates isolated environments. It's useful when you want environment isolation without a full Anaconda/Miniconda installation.

```bash
# Install micromamba (if not already installed)
"${SHELL}" <(curl -L micro.mamba.pm/install.sh)

# Create environment and install
micromamba create -n starrocks-br python=3.11 -c conda-forge
micromamba activate starrocks-br
pip install starrocks-br

# Verify
starrocks-br --help
```

**Running without activation:**

You can run the tool without activating the environment:

```bash
# Option 1: Using micromamba run
micromamba run -n starrocks-br starrocks-br --help

# Option 2: Using direct path to binary
~/.local/share/micromamba/envs/starrocks-br/bin/starrocks-br --help
```

**Optional:** Add the binary to your PATH for easier access:
```bash
export PATH="$HOME/.local/share/micromamba/envs/starrocks-br/bin:$PATH"
```

## Option 4: Devbox (Development)

**Recommended for contributors.**

```bash
# Clone the repository
git clone https://github.com/deep-bi/starrocks-backup-and-restore
cd starrocks-br

# Install devbox (if not already installed)
curl -fsSL https://get.jetpack.io/devbox | bash

# Start devbox shell (auto-installs everything)
devbox shell

# Ready to go
starrocks-br --help
pytest
```

## Option 5: Manual Development Setup

```bash
# Clone the repository
git clone https://github.com/deep-bi/starrocks-backup-and-restore
cd starrocks-br

# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate  # Linux/Mac

# Install in editable mode
pip install -e ".[dev]"

# Verify
starrocks-br --help
```

## Next Steps

- **New users**: See [Getting Started](getting-started.md)
- **Configuration**: Check [Configuration Reference](configuration.md)