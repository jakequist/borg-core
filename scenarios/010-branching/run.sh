#!/usr/bin/env bash
# Forking, divergence, and merge — the thing that makes Borg not an ORM.
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/../lib.sh"
setup

borg set 'Company#1.name' Acme
fork_point="$(borg layer head)"

borg branch fork main --at "$fork_point" --name feature

assert_eq "$(borg get 'Company#1.name' --value --branch feature)" "Acme" \
    "a fork inherits its parent through ancestry, not by copying"

borg --branch feature set 'Company#1.name' 'Acme Corp'
assert_eq "$(borg get 'Company#1.name' --value --branch feature)" "Acme Corp" \
    "the child sees its own write"
assert_eq "$(borg get 'Company#1.name' --value --branch main)" "Acme" "the parent is untouched"

# A PID records where an object was *allocated*, not where it lives, so the fork inherits the
# object under the same name rather than a renamed copy — visible in the canonical address.
assert_eq "$(borg get 'Company#1.name' --branch feature | head -1)" \
    "$(borg get 'Company#1.name' --branch main | head -1)" \
    "the same object has one canonical address on both branches"

# The parent moves on somewhere the child did not touch.
borg --branch main set 'Company#1.founded' 1999

borg branch merge feature --into main
assert_eq "$(borg get 'Company#1.name' --value --branch main)" "Acme Corp" \
    "merge replays the child's writes onto the parent"
assert_eq "$(borg get 'Company#1.founded' --value --branch main)" "1999" \
    "and leaves the parent's own work alone"

assert_contains "$(borg branch list)" "feature" "branches are first-class and listable"
