#!/bin/bash

# Get the hash of the last tag
tag_hash_latest=$(git rev-list --tags --max-count=1)
# Get the tag name associated with that hash
tag_latest=$(git describe --tags "$tag_hash_latest")
# Get the commit messages from that tag to the current commit
CHANGELOG=$(git log "$tag_latest"..HEAD --pretty=format:"- %h %s")
# Print the changelog
echo "$CHANGELOG"
