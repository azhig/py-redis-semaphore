#!/usr/bin/env bash
# Prints colored ASCII logo for py-redis-semaphore.

RED=$'\033[31m'
YELLOW=$'\033[33m'
GREEN=$'\033[32m'
RESET=$'\033[0m'

cat <<'EOF'
+------------------------------+
|          TRAFFIC LIGHT       |
|      +------------------+    |
|      |      ${RED}(@)${RESET}         |    |
|      |      ${YELLOW}(@)${RESET}         |    |
|      |      ${GREEN}(@)${RESET}         |    |
|      +------------------+    |
|                              |
+------ py-redis-semaphore -----+
EOF
