#!/usr/bin/env python

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

from __future__ import print_function
import argparse
import os
import re
import subprocess
import sys


SPEEDB_URL_PATTERN = re.compile(r".*[/:]speedb-io/speedb.*")
TAG_VERSION_PATTERN = re.compile(r"^speedb/v(\d+)\.(\d+)\.(\d+)$")


def split_nonempty_lines(s):
    for line in s.splitlines():
        line = line.rstrip()
        if line:
            yield line


def check_output(call, with_stderr=True):
    stderr = None if with_stderr else subprocess.DEVNULL
    return subprocess.check_output(call, stderr=stderr).rstrip(b"\n").decode("utf-8")


def get_suitable_remote():
    for remote in split_nonempty_lines(check_output(["git", "remote", "show"])):
        remote = remote.strip()
        url = check_output(["git", "remote", "get-url", remote])
        if SPEEDB_URL_PATTERN.match(url):
            return remote


def get_branch_name(remote, ref, hint=None):
    remote_candidates = []
    results = split_nonempty_lines(
        check_output(
            [
                "git",
                "branch",
                "-r",
                "--contains",
                ref,
                "--format=%(refname:lstrip=3)",
                "{}/*".format(remote),
            ]
        )
    )
    for result in results:
        if result == "main":
            return (False, result)

        remote_candidates.append(result)

    local_candidates = []
    results = split_nonempty_lines(
        check_output(
            ["git", "branch", "--contains", ref, "--format=%(refname:lstrip=2)"]
        )
    )
    for result in results:
        if result == "main":
            return (True, result)

        local_candidates.append(result)

    # Find the most fitting branch by giving more weight to branches that are
    # ancestors to the most branches
    #
    # This will choose A by lexigoraphic order in the following case (the ref
    # that we are checking is bracketed):
    # BASE * - * - (*) - * - * A
    #               \
    #                * - * B
    # This is not a wrong choice, even if originally A was branched from B,
    # because without looking at the reflog (which we can't do on build machines)
    # there is no way to tell which branch was the "original". Moreover, if B
    # is later rebased, A indeed will be the sole branch containing the checked
    # commit.
    #
    # `hint` is used to guide the choice in that case to the branch that we've
    # chosen in a previous commit.
    all_candidates = []
    for target in remote_candidates:
        boost = -0.5 if hint == (False, target) else 0.0
        all_candidates.append(
            (
                boost
                + sum(
                    -1.0
                    for c in remote_candidates
                    if is_ancestor_of(
                        "{}/{}".format(remote, c), "{}/{}".format(remote, target)
                    )
                ),
                (False, target),
            )
        )
    for target in local_candidates:
        boost = -0.5 if hint == (True, target) else 0.0
        all_candidates.append(
            (
                boost
                + sum(-1.0 for c in local_candidates if is_ancestor_of(c, target)),
                (True, target),
            )
        )
    all_candidates.sort()

    if all_candidates:
        return all_candidates[0][1]

    # Not on any branch (detached on a commit that isn't referenced by a branch)
    return (True, "?")


def is_ancestor_of(ancestor, ref):
    try:
        subprocess.check_output(["git", "merge-base", "--is-ancestor", ancestor, ref])
    except subprocess.CalledProcessError:
        return False
    else:
        return True


def get_remote_tags_for_ref(remote, from_ref):
    tag_ref_prefix = "refs/tags/"
    tags = {}
    for line in split_nonempty_lines(
        check_output(["git", "ls-remote", "--tags", "--refs", remote])
    ):
        h, tag = line.split(None, 1)
        if not tag.startswith(tag_ref_prefix) or not is_ancestor_of(h, from_ref):
            continue
        tags[h] = tag[len(tag_ref_prefix):]
    return tags


def get_local_tags_for_ref(from_ref):
    tags = {}
    for line in split_nonempty_lines(
        check_output(
            [
                "git",
                "tag",
                "--merged",
                from_ref,
                "--format=%(objectname) %(refname:lstrip=2)",
            ]
        )
    ):
        h, tag = line.split(None, 1)
        tags[h] = tag
    return tags


def get_speedb_version_tags(remote, head_ref):
    try:
        tags = get_remote_tags_for_ref(remote, head_ref)
    except subprocess.CalledProcessError:
        warning("failed to fetch remote tags, falling back on local tags")
        tags = get_local_tags_for_ref(head_ref)

    version_tags = {
        h: n for h, n in tags.items() if TAG_VERSION_PATTERN.match(n)
    }

    return version_tags


def get_branches_for_revlist(remote, base_ref, head_ref):
    refs_since = tuple(
        split_nonempty_lines(
            check_output(["git", "rev-list", "{}..{}".format(base_ref, head_ref)])
        )
    )

    branches = []
    last_branch, last_count = None, 0
    for i, cur_ref in enumerate(refs_since):
        cur_branch = get_branch_name(remote, cur_ref, last_branch)

        if cur_branch != last_branch:
            if last_count > 0:
                branches.append((last_branch, last_count))

            # All versions are rooted in main, so there's no point to continue
            # iterating after hitting it
            if cur_branch == ("", "main"):
                last_branch, last_count = cur_branch, len(refs_since) - i
                break
            else:
                last_branch, last_count = cur_branch, 1
        else:
            last_count += 1

    if last_count > 0:
        branches.append((last_branch, last_count))

    return branches


def is_dirty_worktree():
    try:
        subprocess.check_call(["git", "diff-index", "--quiet", "HEAD", "--"])
    except subprocess.CalledProcessError:
        return True
    else:
        return False


def get_latest_release_ref(ref, tags):
    for line in split_nonempty_lines(
        check_output(
            ["git", "rev-list", "--no-walk", "--topo-order"] + list(tags.keys())
        )
    ):
        line = line.strip()
        return (line, tags[line])


def get_current_speedb_version():
    base_path = check_output(["git", "rev-parse", "--show-toplevel"])
    with open(os.path.join(base_path, "speedb", "version.h"), "rb") as f:
        data = f.read()

    components = []
    for component in (b"MAJOR", b"MINOR", b"PATCH"):
        v = re.search(rb"\s*#\s*define\s+SPEEDB_%b\s+(\d+)" % component, data).group(1)
        components.append(int(v.decode("utf-8")))

    return tuple(components)


def which(cmd):
    exts = os.environ.get("PATHEXT", "").split(os.pathsep)
    for p in os.environ["PATH"].split(os.pathsep):
        if not p:
            continue

        full_path = os.path.join(p, cmd)
        if os.access(full_path, os.X_OK):
            return full_path

        for ext in exts:
            if not ext:
                continue

            check_path = "{}.{}".format(full_path, ext)
            if os.access(check_path, os.X_OK):
                return check_path

    return None


output_level = 1 if os.isatty(sys.stderr.fileno()) else 0


def warning(s):
    if output_level and s:
        print("warning: {}".format(s), file=sys.stderr)


def info(s):
    if output_level > 1 and s:
        print("info: {}".format(s), file=sys.stderr)


def exit_unknown(s, additional_components=[]):
    print("-".join(["?"] + additional_components))
    warning(s)
    raise SystemExit(2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="print information to stderr"
    )
    args = parser.parse_args()

    if args.verbose:
        global output_level
        output_level = 2

    if not which("git"):
        exit_unknown("git wasn't found on your system")

    try:
        git_dir = check_output(["git", "rev-parse", "--git-dir"], False)
    except subprocess.CalledProcessError:
        exit_unknown("not a git repository")

    components = []
    if is_dirty_worktree():
        components.append("*")

    if os.path.isfile(os.path.join(git_dir, "shallow")):
        exit_unknown("can't calculate build tag in a shallow repository", components)

    remote = get_suitable_remote()
    if not remote:
        exit_unknown("no suitable remote found", components)

    head_ref = check_output(["git", "rev-parse", "HEAD"]).strip()
    version_tags = get_speedb_version_tags(remote, head_ref)

    if not version_tags:
        exit_unknown("no version tags found for current HEAD")

    base_ref, release_name = get_latest_release_ref(head_ref, version_tags)
    current_ver = ".".join(str(v) for v in get_current_speedb_version())
    tag_ver = ".".join(TAG_VERSION_PATTERN.match(release_name).groups())
    if current_ver != tag_ver:
        warning(
            "current version doesn't match latest release tag (current={}, tag={})".format(
                current_ver, tag_ver
            )
        )
        components.append("(tag:{})".format(tag_ver))
    else:
        info("latest release is {} ({})".format(release_name, base_ref))
        info("current Speedb version is {}".format(current_ver))

    branches = get_branches_for_revlist(remote, base_ref, head_ref)

    for (is_local, name), commits in reversed(branches):
        components.append(
            "({}{}+{})".format(
                "#" if is_local else "",
                re.sub(r"([#()+\"])", r"\\\1", name.replace("\\", "\\\\")),
                commits,
            )
        )

    print("-".join(components))


if __name__ == "__main__":
    main()
