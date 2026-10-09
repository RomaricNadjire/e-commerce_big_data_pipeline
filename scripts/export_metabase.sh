#!/usr/bin/env bash
set -euo pipefail

METABASE_URL="${METABASE_URL:-http://localhost:3000}"
METABASE_USER="${METABASE_USER:?définis METABASE_USER}"
METABASE_PASSWORD="${METABASE_PASSWORD:?définis METABASE_PASSWORD}"
OUT_DIR="${OUT_DIR:-metabase/dashboards}"

mkdir -p "$OUT_DIR"

echo "→ Authentification..."
SESSION=$(curl -s -X POST "$METABASE_URL/api/session" \
  -H 'Content-Type: application/json' \
  -d "{\"username\":\"$METABASE_USER\",\"password\":\"$METABASE_PASSWORD\"}" \
  | jq -r '.id')

if [ "$SESSION" = "null" ] || [ -z "$SESSION" ]; then
  echo "❌ Échec de l'authentification" >&2
  exit 1
fi

echo "→ Liste des dashboards..."
curl -s "$METABASE_URL/api/dashboard" \
  -H "X-Metabase-Session: $SESSION" \
  | jq -c '.[] | {id, name}' \
  | while read -r row; do
      ID=$(echo "$row" | jq -r '.id')
      NAME=$(echo "$row" | jq -r '.name' | tr ' ' '_' | tr '[:upper:]' '[:lower:]')
      echo "  → $NAME (id=$ID)"
      curl -s "$METABASE_URL/api/dashboard/$ID" \
        -H "X-Metabase-Session: $SESSION" \
        | jq '.' > "$OUT_DIR/${NAME}.json"
    done

echo "✅ Export terminé dans $OUT_DIR/"