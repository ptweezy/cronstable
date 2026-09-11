"""Read-only release configuration checks, before expensive builders start."""

import json
import os
import re
import urllib.request

REQUIRED = (
    "HOMEBREW_TAP_TOKEN",
    "WINGET_TOKEN",
    "AZURE_TENANT_ID",
    "AZURE_CLIENT_ID",
    "AZURE_SUBSCRIPTION_ID",
    "AZURE_SIGNING_ENDPOINT",
    "AZURE_SIGNING_ACCOUNT",
    "AZURE_SIGNING_PROFILE",
)
MACOS = (
    "MACOS_CERT_P12_BASE64",
    "MACOS_CERT_PASSWORD",
    "MACOS_SIGN_IDENTITY",
    "MACOS_NOTARY_KEY_BASE64",
    "MACOS_NOTARY_KEY_ID",
    "MACOS_NOTARY_ISSUER_ID",
)


def validate(version, env):
    if not re.fullmatch(
        r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)", version
    ):
        raise ValueError("Release version must be canonical X.Y.Z")
    major, minor, patch = map(int, version.split("."))
    # The MSI upgrade smoke builds patch+1 as well.
    if major > 255 or minor > 255 or patch >= 65535:
        raise ValueError(
            "Version exceeds MSI limits, including the upgrade smoke's patch+1"
        )
    missing = [key for key in REQUIRED if not env.get(key)]
    if missing:
        raise ValueError("Missing release secrets: " + ", ".join(missing))
    if any(env.get(key) for key in MACOS) and not all(
        env.get(key) for key in MACOS
    ):
        raise ValueError(
            "macOS signing requires all six MACOS_* secrets or none"
        )
    if bool(env.get("DOCKERHUB_USERNAME")) != bool(env.get("DOCKERHUB_TOKEN")):
        raise ValueError(
            "Docker Hub requires both username and token or neither"
        )


def github(path, token):
    request = urllib.request.Request(
        "https://api.github.com/" + path,
        headers={
            "Authorization": "Bearer " + token,
            "Accept": "application/vnd.github+json",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response), response.headers


def authenticate(env):
    repositories = [("HOMEBREW_TAP_TOKEN", "ptweezy/homebrew-tap")]
    if env.get("RELEASE_TOKEN"):
        repositories.append(("RELEASE_TOKEN", env["GITHUB_REPOSITORY"]))
    else:
        # The publishing job has its own contents:write GITHUB_TOKEN. Its
        # established fallback does not require a separately stored PAT.
        print(
            "RELEASE_TOKEN not configured; tag publication uses GITHUB_TOKEN"
        )
    for secret, repo in repositories:
        result, headers = github("repos/" + repo, env[secret])
        if not result.get("permissions", {}).get("push", False):
            raise ValueError(f"{secret} does not report push access to {repo}")
        scopes = {
            s.strip()
            for s in headers.get("X-OAuth-Scopes", "").split(",")
            if s.strip()
        }
        if secret == "RELEASE_TOKEN" and scopes and "workflow" not in scopes:
            raise ValueError("RELEASE_TOKEN classic PAT needs workflow scope")
    _, headers = github("user", env["WINGET_TOKEN"])
    scopes = {s.strip() for s in headers.get("X-OAuth-Scopes", "").split(",")}
    if not scopes.intersection({"repo", "public_repo"}):
        raise ValueError(
            "WINGET_TOKEN must be a classic PAT with public_repo or repo scope"
        )


def authenticate_pypi(env):
    """Check Trusted Publishing; discard its token without uploading."""
    request = urllib.request.Request(
        env["ACTIONS_ID_TOKEN_REQUEST_URL"] + "&audience=pypi",
        headers={
            "Authorization": "Bearer " + env["ACTIONS_ID_TOKEN_REQUEST_TOKEN"]
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        oidc = json.load(response)["value"]
    request = urllib.request.Request(
        "https://pypi.org/_/oidc/mint-token",
        data=json.dumps({"token": oidc}).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        if not json.load(response).get("token"):
            raise ValueError("PyPI Trusted Publishing did not return a token")


if __name__ == "__main__":
    validate(os.environ["VERSION"], os.environ)
    authenticate(os.environ)
    authenticate_pypi(os.environ)
    print(
        "Release configuration, GitHub authentication and "
        "PyPI Trusted Publishing verified"
    )
