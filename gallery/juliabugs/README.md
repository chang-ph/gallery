## How to set up and run this workflow

1. Ensure your current working directory is the workflow directory (where this README.md is located).
1. Install `uv` if you haven't installed it: `curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR="/usr/bin" sh`
1. Install Python if you haven't installed it: `uv python install 3.11`
1. Install Julia if you haven't installed it: `curl -fsSL https://install.julialang.org | sh`
1. Install Python dependencies: `uv sync && source .venv/bin/activate`
1. Run sample: `inv sample`
1. Run analyzer: `inv analyze`
