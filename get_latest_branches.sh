git for-each-ref --sort=committerdate refs/heads/ --format '%(committerdate:short) %(refname:short)' | grep -v resync/
