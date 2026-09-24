#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="/mnt/c/Users/melis/OneDrive/Desktop/IA decision plateform"

printf '\n[1/6] Mise a jour des paquets Ubuntu\n'
sudo apt-get update

printf '\n[2/6] Installation des outils systeme utiles\n'
sudo apt-get install -y curl ca-certificates unzip git build-essential openjdk-17-jdk python3 python3-venv python3-pip

printf '\n[3/6] Verification Java et Python\n'
java -version
python3 --version

printf '\n[4/6] Installation de uv dans Ubuntu si absent\n'
if ! command -v uv >/dev/null 2>&1; then
  curl -LsSf https://astral.sh/uv/install.sh | sh
fi
export PATH="$HOME/.local/bin:$PATH"
uv --version

printf '\n[5/6] Synchronisation des dependances Python du projet avec Spark/Delta\n'
cd "$PROJECT_DIR"
export UV_LINK_MODE=copy
export UV_PROJECT_ENVIRONMENT="$HOME/.venvs/ia-decision-platform"
uv sync --frozen --extra dev --extra spark

printf '\n[6/6] Verification Spark/Delta depuis Ubuntu\n'
uv run --frozen --extra spark python spark/check_installation.py

printf '\nOK - Ubuntu est pret pour la suite Spark/Delta.\n'


