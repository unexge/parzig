#!/bin/bash

# Only run in remote environments
if [ "$CLAUDE_CODE_REMOTE" != "true" ]; then
  exit 0
fi

echo "Installing jj using Cargo..."
cargo install --locked --bin jj jj-cli
jj --version

echo "Installing uv using pipx..."
pipx install uv
uv --version

echo "Installing anyzig..."
arch=$(uname -m)
if [[ $arch == x86_64* ]]; then
    curl -L https://github.com/marler8997/anyzig/releases/latest/download/anyzig-x86_64-linux.tar.gz | tar xz
elif [[ $arch == aarch64* ]]; then
    curl -L https://github.com/marler8997/anyzig/releases/latest/download/anyzig-aarch64-linux.tar.gz | tar xz
fi
mv ./zig /usr/local/bin
zig any version
zig version

exit 0
