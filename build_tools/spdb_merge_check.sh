#!/bin/sh

# Copyright (C) 2022 Speedb Ltd. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -eo pipefail

bail() {
	echo 1>&2 "$1"
	exit 1
}

if [ $# -lt 1 ] || [ $# -gt 2 ]; then
	bail "usage: $0 <merge-to-ref> [<merge-from-ref>]"
fi

find_suitable_remote() {
	(
		root=$1
		branch=$2

		if echo "$branch" | grep -q '^[^/]\+/.\+$'; then
			possible_remote=$(echo "$branch" | sed 's#\([^/]\+\)/#\1#')
			if [ -n "$possible_remote" ]; then
				if git remote get-url "$possible_remote" 2> /dev/null; then
					echo "$possible_remote" && return
				fi
			fi
		fi

		cd "$root"

		git remote show 2> /dev/null | {
			while read -r remote; do
				remote_url=$(git remote get-url "$remote")
				case $remote_url in
					*[/:]speedb-io/speedb*)
						echo "$remote" && return
						;;
					*)
						;;
				esac
			done

			if git remote get-url origin > /dev/null; then
				echo origin && return
			fi
		}
	)
}

get_branch_name_for_ref() {
	(
		remote=$1
		remote_url=$(git remote get-url "$remote")
		root=$2
		ref=$3

		cd "$root"

		branch=$(git rev-parse --abbrev-ref "$ref")
		if [ -n "$branch" ]; then
			echo "${branch#"${remote}/"}"
			return
		fi

		git ls-remote --heads "$remote_url" | grep "$ref" | while read -r line; do
			ref_hash=$(echo "$line" | sed 's/^\([0-9a-f]\+\).*/\1/')
			ref_branch=$(echo "$line" | sed 's#^[0-9a-f]\+\s\+refs/heads/##')
			if [ "$ref_hash" = "$ref" ] || [ "$ref_branch" = "$ref" ]; then
				echo "$ref_branch" && return
			fi
		done ||:
	)
}

# Check for the existence of git
command -v git >/dev/null || bail "error: git not found"

SCRIPT_ROOT=$(dirname "$0")

# Get the repo's root dir
# Use `cd` and a subshell instead of `git -C`, in order to support old git versions
REPO_ROOT=$(cd "$SCRIPT_ROOT" && git rev-parse --show-toplevel)

TARGET_REF=$1
REMOTE=$(find_suitable_remote "$REPO_ROOT" "$TARGET_REF")
if [ -z "$REMOTE" ]; then
	bail "failed to find suitable remote for target ref $TARGET_REF"
fi

# Target must be a branch name
TARGET_BRANCH=$(get_branch_name_for_ref "$REMOTE" "$REPO_ROOT" "$TARGET_REF")
if [ -z "$TARGET_BRANCH" ]; then
	bail "error: no target branch found for target ref $TARGET_REF on remote $REMOTE"
fi

# Assume HEAD if no merge-from ref was provided
if [ $# -lt 2 ]; then
	FROM_REF=$(cd "$REPO_ROOT" && git rev-parse --abbrev-ref HEAD)
else
	FROM_REF=$2
fi

FETCH_MAX_DEPTH=${FETCH_MAX_DEPTH:-50}

# Make sure we have access to target and merge-from refs
(cd "$REPO_ROOT" && { 
	git rev-parse -q --verify "$TARGET_REF" > /dev/null 2>&1 ||
	git fetch -p --depth="$FETCH_MAX_DEPTH" "$REMOTE" "$TARGET_BRANCH"
})
(cd "$REPO_ROOT" && { 
	git rev-parse -q --verify "$FROM_REF" > /dev/null 2>&1 ||
	git fetch -p --depth="$FETCH_MAX_DEPTH" "$REMOTE" "$FROM_REF"
})

# Check that the branch being merged is indeed based on the target branch
if ! (cd "$REPO_ROOT" && git merge-base --is-ancestor "$REMOTE/$TARGET_BRANCH" "$FROM_REF"); then
	bail "error: $TARGET_REF is not an ancestor of $FROM_REF"
fi

# Ensure that the version number wasn't touched
version_commits=$(cd "$REPO_ROOT" && git log --format="%h %s" -G'SPEEDB_(MAJOR|MINOR|PATCH)' "$REMOTE/$TARGET_BRANCH..$FROM_REF" -- speedb/version.h)
if [ -n "$version_commits" ]; then
	printf >&2 "%s\n" "$version_commits"
	bail "error: the above commits made changes to the Speedb version definition"
fi

# Verify that we pass code style checks for the changes that were made
if ! (cd "$REPO_ROOT" && VERBOSE_CHECK=1 FORMAT_UPSTREAM="$REMOTE/$TARGET_BRANCH" ./build_tools/format-diff.sh -c 1>&2); then
	bail "error: some files require formatting. Please check the output above"
fi

if ! (cd "$REPO_ROOT" && tools/check_all_python.py 1>&2); then
	bail "error: Python checks failed. Please check the output above"
fi

# Verify that added files have a Speedb copyright header
(cd "$REPO_ROOT" && git diff "$REMOTE/$TARGET_BRANCH..$FROM_REF" --name-only --diff-filter=A) | {
	copyright_verification_failed=0
	while read -r new_f; do
		if [ -x "$REPO_ROOT/$new_f" ] || (echo "$new_f" |
			grep -q  "^\(.*\.\(S\|c\|cc\|h\|java\|in\|proto\|py\|sh\|ps1\|mk\|cmake\)\|Makefile\|CMakeLists.txt\)$")
		then
			if ! head -n10 "$REPO_ROOT/$new_f" | grep -q "Copyright (C) 20[0-9]\{2\} Speedb Ltd\. All rights reserved"; then
				copyright_verification_failed=1
				echo >&2 "* $new_f: missing copyright header"
			fi
		elif case "$new_f" in *.md) ;; *) false ;; esac; then
			if ! head -n10 "$REPO_ROOT/$new_f" | grep -q "Part of the Speedb project, under the Apache License v2\.0\."; then
				copyright_verification_failed=1
				echo >&2 "* $new_f: missing licence header"
			fi
		fi
	done
	[ "$copyright_verification_failed" = "0" ] || bail "error: some files are missing the Speedb copyright or licence header"
}

# The following checks are only relevant when merging to main or to a release branch
if [ "$TARGET_BRANCH" = "main" ] || [ "${TARGET_BRANCH#release/}" != "$TARGET_BRANCH" ]; then
	FROM_BRANCH=$(get_branch_name_for_ref "$REMOTE" "$REPO_ROOT" "$FROM_REF")
	if [ -n "$FROM_BRANCH" ]; then
		# Extract the issue number from the branch name
		ISSUE_NUM=$(echo "$FROM_BRANCH" | { grep -o '^[0-9]\+' ||:; } | head -1)
		if [ -z "$ISSUE_NUM" ]; then
			bail "error: branch name $FROM_BRANCH does not begin with an issue number"
		fi

		# Verify that all of the commits contain the right issue number
		(cd "$REPO_ROOT" && git log --format="%h %s" "$REMOTE/$TARGET_BRANCH..$FROM_REF") | while read -r line; do
			m=$(echo "$line" | sed 's/[0-9a-f]\+ //')
			if ! echo "$m" | grep -q "(#${ISSUE_NUM})\$"; then
				h=$(echo "$line" | sed 's/\([0-9a-f]\+\) .*$/\1/')
				bail "error: commit message for $h doesn't end with issue number $ISSUE_NUM"
			fi
		done
	fi
fi
