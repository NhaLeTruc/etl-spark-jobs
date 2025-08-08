#!/usr/bin/env bash

echo "Adding Git hooks"
echo "Adding commit-msg hook"

chmod u+x hooks/commit-msg
ln -s -f ../../hooks/commit-msg .git/hooks/commit-msg

echo "Git hooks added"