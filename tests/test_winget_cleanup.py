import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
from strictyaml.ruamel import YAML

WORKFLOW = (
    Path(__file__).resolve().parents[1] / ".github/workflows/release.yml"
)
REPO = "repos/microsoft/winget-pkgs"


@pytest.fixture(scope="module")
def winget_steps():
    workflow = YAML(typ="safe").load(WORKFLOW.read_text(encoding="utf-8"))
    return workflow["jobs"]["winget"]["steps"]


@pytest.fixture
def cleanup(monkeypatch, winget_steps):
    step = next(
        s
        for s in winget_steps
        if s.get("name", "").startswith("Close superseded")
    )
    script = compile(step["run"], str(WORKFLOW), "exec")

    def run(version="1.10.0"):
        monkeypatch.setenv("VERSION", version)
        exec(script, {})

    return run


def submission(number=1, version="1.9.0", **overrides):
    pr = {
        "number": number,
        "title": f"New version: ptweezy.cronstable version {version}",
        "state": "open",
        "merged": False,
        "user": {"id": 42, "login": "release-account"},
        "pull_request": {},
    }
    pr.update(overrides)
    return pr


@pytest.fixture
def github(monkeypatch):
    data = SimpleNamespace(
        pages=[[submission()]],
        pulls={},
        reviews={},
        closed=[],
        fail=None,
        calls=[],
    )

    def run(command, **kwargs):
        assert command[:4] == ["gh", "api", "--hostname", "github.com"]
        assert kwargs["check"] is True
        endpoint, args = command[4], command[5:]
        method = (
            args[args.index("--method") + 1] if "--method" in args else "GET"
        )
        data.calls.append((method, endpoint))
        if data.fail == (method, endpoint):
            raise subprocess.CalledProcessError(
                1, command, stderr="API failed"
            )
        if endpoint == "user":
            result = {"id": 42, "login": "release-account"}
        elif endpoint == f"{REPO}/issues":
            assert method == "GET"
            assert "creator=release-account" in args
            assert "state=open" in args
            assert "--paginate" in args and "--slurp" in args
            result = data.pages
        elif endpoint.endswith("/reviews?per_page=100"):
            assert "--paginate" in args and "--slurp" in args
            number = int(endpoint.split("/")[-2])
            result = data.reviews.get(number, [[]])
        else:
            assert endpoint.startswith(f"{REPO}/pulls/")
            number = int(endpoint.split("/")[-1])
            result = data.pulls.get(number)
            if result is None:
                result = next(
                    pr
                    for page in data.pages
                    for pr in page
                    if pr["number"] == number
                )
            if method == "PATCH":
                assert "state=closed" in args
                data.closed.append(number)
                result["state"] = "closed"
        return subprocess.CompletedProcess(command, 0, json.dumps(result))

    monkeypatch.setattr(subprocess, "run", run)
    return data


def test_cleanup_requires_successful_submission(winget_steps):
    submit = next(
        s for s in winget_steps if s.get("name") == "Submit winget manifest"
    )
    cleanup = next(
        s
        for s in winget_steps
        if s.get("name", "").startswith("Close superseded")
    )
    assert winget_steps.index(submit) < winget_steps.index(cleanup)
    assert "submit winget-manifests" in submit["run"]
    assert not submit.get("continue-on-error", False)
    assert cleanup.get("if", "success()") == "success()"
    assert cleanup["env"]["GH_TOKEN"] == submit["env"]["WINGET_TOKEN"]
    assert cleanup["env"]["VERSION"] == submit["env"]["VERSION"]
    assert cleanup["shell"] == "python {0}"


@pytest.mark.parametrize(
    "old,current,closed",
    [
        ("1.9.0", "1.10.0", True),
        ("1.10.9", "1.10.10", True),
        ("9.99.99", "10.0.0", True),
        ("1.10.0", "1.10.0", False),
        ("1.11.0", "1.10.0", False),
        ("2.0.0", "1.10.0", False),
    ],
)
def test_only_older_versions_close(cleanup, github, old, current, closed):
    github.pages = [[submission(version=old)]]
    cleanup(current)
    assert github.closed == ([1] if closed else [])


@pytest.mark.parametrize(
    "title",
    [
        "New version: someone.other version 1.0.0",
        "New version: ptweezy.cronstable.Other version 1.0.0",
        "New version: ptweezy.cronstable version 1.0.0-beta",
        "New version: ptweezy.cronstable version 1.0.0 extra",
        "Revert New version: ptweezy.cronstable version 1.0.0",
    ],
)
def test_unrelated_or_unrecognized_titles_stay_open(cleanup, github, title):
    github.pages = [[submission(title=title)]]
    cleanup()
    assert github.closed == []


def test_other_authors_and_issues_stay_open(cleanup, github):
    issue = submission(2)
    del issue["pull_request"]
    github.pages = [
        [submission(user={"id": 99, "login": "someone-else"}), issue]
    ]
    cleanup()
    assert github.closed == []
    assert all("/pulls/" not in endpoint for _, endpoint in github.calls)


@pytest.mark.parametrize(
    "updates",
    [
        {"state": "closed"},
        {"merged": True},
        {"title": "New version: ptweezy.cronstable version 2.0.0"},
        {"title": "Unrelated change"},
    ],
)
def test_pr_is_checked_again_before_closing(cleanup, github, updates):
    github.pulls[1] = submission(**updates)
    cleanup()
    assert github.closed == []


@pytest.mark.parametrize(
    "reviews,closed",
    [
        ([[]], True),
        ([[{"state": "COMMENTED"}]], True),
        ([[{"state": "CHANGES_REQUESTED"}]], True),
        ([[{"state": "DISMISSED"}]], True),
        ([[{"state": "PENDING"}]], True),
        ([[{"state": "APPROVED", "commit_id": "older-commit"}]], False),
        ([[{"state": "COMMENTED"}], [{"state": "APPROVED"}]], False),
        ([[{"state": "APPROVED"}, {"state": "COMMENTED"}]], False),
    ],
)
def test_review_history_protects_approved_prs(
    cleanup, github, reviews, closed
):
    github.reviews[1] = reviews
    cleanup()
    assert github.closed == ([1] if closed else [])


def test_all_candidate_pages_are_processed_and_reruns_are_safe(
    cleanup, github
):
    github.pages = [[submission(1)], [submission(2), submission(3, "1.10.0")]]
    cleanup()
    cleanup()
    assert github.closed == [1, 2]


@pytest.mark.parametrize(
    "failure",
    [
        ("GET", "user"),
        ("GET", f"{REPO}/issues"),
        ("GET", f"{REPO}/pulls/1"),
        ("GET", f"{REPO}/pulls/1/reviews?per_page=100"),
        ("PATCH", f"{REPO}/pulls/1"),
    ],
)
def test_api_failures_abort_cleanup(cleanup, github, failure):
    github.fail = failure
    with pytest.raises(subprocess.CalledProcessError):
        cleanup()
    assert github.closed == []


def test_invalid_release_version_aborts_before_api_calls(cleanup, github):
    with pytest.raises(ValueError, match="Invalid release version"):
        cleanup("1.10.0-beta")
    assert github.calls == []
