"""git_provider: GitLab's and GitHub's merge-request APIs, mocked at the httpx layer.

What is pinned is the request each provider sends (path, auth header, body)
and the classification of every answer: find-before-create, 401/403 →
GitProviderAuthError, 400/404/422 → GitProviderRequestRejectedError, 409/5xx/
unreachable → the retryable GitProviderError, bodies bounded in the message.
"""

from __future__ import annotations

import json

import httpx
import pytest
import respx

from activities.cluster_lifecycle import git_provider
from shared.exceptions import GitProviderAuthError, GitProviderError, GitProviderRequestRejectedError

TOKEN = "glpat-not-real"
GITLAB_REPO = "https://gitlab.example.internal/platform/gitops-day1/platform-config.git"
GITLAB_API = "https://gitlab.example.internal/api/v4/projects/platform%2Fgitops-day1%2Fplatform-config"
GITHUB_REPO = "https://github.com/team-redbull/gitops-day1-platform-config.git"
GITHUB_API = "https://api.github.com/repos/team-redbull/gitops-day1-platform-config"
BRANCH = "provision-cluster/ocp4-prep-e2e-site2-a"


class TestRepoPath:
    @pytest.mark.parametrize("url", [GITLAB_REPO, GITLAB_REPO.removesuffix(".git"), GITLAB_REPO + "/"])
    def test_a_gitlab_project_path_is_url_encoded(self, url):
        assert git_provider.GitLabProvider(url, TOKEN)._base == GITLAB_API

    def test_a_github_enterprise_host_gets_its_api_v3(self):
        provider = git_provider.GitHubProvider("https://ghe.example/org/repo.git", TOKEN)
        assert provider._base == "https://ghe.example/api/v3/repos/org/repo"

    @pytest.mark.parametrize("url", ["git@github.com:org/repo.git", "https://github.com/", "https://github.com/only-owner"])
    def test_a_url_that_is_not_a_clone_url_is_refused(self, url):
        with pytest.raises(GitProviderRequestRejectedError):
            git_provider.GitHubProvider(url, TOKEN)

    def test_an_unknown_provider_name_is_refused(self):
        with pytest.raises(GitProviderRequestRejectedError, match="unknown git provider"):
            git_provider.provider_for("bitbucket", GITHUB_REPO, TOKEN)


@respx.mock
class TestGitLab:
    def setup_method(self):
        self.provider = git_provider.GitLabProvider(GITLAB_REPO, TOKEN)

    async def test_find_then_create_when_none_is_open(self):
        find = respx.get(f"{GITLAB_API}/merge_requests").mock(return_value=httpx.Response(200, json=[]))
        create = respx.post(f"{GITLAB_API}/merge_requests").mock(
            return_value=httpx.Response(201, json={"iid": 42, "web_url": "https://gitlab.example.internal/p/-/merge_requests/42"})
        )
        ref = await git_provider.ensure_merge_request(self.provider, BRANCH, "Provision x", "desc")
        assert (ref.identifier, ref.created, ref.url) == ("42", True, "https://gitlab.example.internal/p/-/merge_requests/42")
        assert find.calls[0].request.headers["PRIVATE-TOKEN"] == TOKEN
        assert dict(find.calls[0].request.url.params) == {"source_branch": BRANCH, "target_branch": "main", "state": "opened"}
        body = json.loads(create.calls[0].request.content)
        assert body == {"source_branch": BRANCH, "target_branch": "main", "title": "Provision x", "description": "desc",
                        "remove_source_branch": True}

    async def test_an_open_merge_request_is_found_not_created(self):
        respx.get(f"{GITLAB_API}/merge_requests").mock(
            return_value=httpx.Response(200, json=[{"iid": 7, "web_url": "https://g/mr/7"}, {"iid": 8, "web_url": "https://g/mr/8"}])
        )
        create = respx.post(f"{GITLAB_API}/merge_requests")
        ref = await git_provider.ensure_merge_request(self.provider, BRANCH, "t", "d")
        assert (ref.identifier, ref.created, ref.url) == ("7", False, "https://g/mr/7")
        assert not create.called

    @pytest.mark.parametrize(("state", "expected", "sha"), [
        ("opened", "open", None), ("locked", "open", None), ("closed", "closed", None), ("merged", "merged", "abc123"),
    ])
    async def test_state_is_read_back(self, state, expected, sha):
        respx.get(f"{GITLAB_API}/merge_requests/42").mock(
            return_value=httpx.Response(200, json={"state": state, "merge_commit_sha": sha})
        )
        result = await self.provider.state("42")
        assert (result.state, result.merged_sha) == (expected, sha)

    async def test_a_squash_merge_reports_its_commit(self):
        respx.get(f"{GITLAB_API}/merge_requests/42").mock(
            return_value=httpx.Response(200, json={"state": "merged", "merge_commit_sha": None, "squash_commit_sha": "sq1"})
        )
        assert (await self.provider.state("42")).merged_sha == "sq1"

    async def test_an_unknown_state_is_retried_not_guessed(self):
        respx.get(f"{GITLAB_API}/merge_requests/42").mock(return_value=httpx.Response(200, json={"state": "weird"}))
        with pytest.raises(GitProviderError, match="unknown state"):
            await self.provider.state("42")

    @pytest.mark.parametrize("identifier", ["42/../1", "abc", "", "4 2"])
    async def test_an_identifier_that_is_not_a_number_never_reaches_the_api(self, identifier):
        route = respx.get(url__regex=r".*")
        with pytest.raises(GitProviderRequestRejectedError):
            await self.provider.state(identifier)
        assert not route.called

    @pytest.mark.parametrize(("status", "error"), [
        (401, GitProviderAuthError), (403, GitProviderAuthError),
        (400, GitProviderRequestRejectedError), (404, GitProviderRequestRejectedError), (422, GitProviderRequestRejectedError),
        (409, GitProviderError), (500, GitProviderError), (502, GitProviderError),
    ])
    async def test_every_status_is_classified(self, status, error):
        respx.get(f"{GITLAB_API}/merge_requests").mock(return_value=httpx.Response(status, json={"message": "why"}))
        with pytest.raises(error, match="why"):
            await self.provider.find_open(BRANCH)

    async def test_an_unreachable_provider_is_retryable(self):
        respx.get(f"{GITLAB_API}/merge_requests").mock(side_effect=httpx.ConnectError("refused"))
        with pytest.raises(GitProviderError, match="ConnectError"):
            await self.provider.find_open(BRANCH)

    async def test_a_body_that_is_not_json_is_retryable_and_bounded(self):
        respx.get(f"{GITLAB_API}/merge_requests").mock(return_value=httpx.Response(200, text="<html>" * 100_000))
        with pytest.raises(GitProviderError) as caught:
            await self.provider.find_open(BRANCH)
        assert len(str(caught.value)) < 800 and "more characters cut" in str(caught.value)

    async def test_an_error_body_is_bounded_too(self):
        respx.post(f"{GITLAB_API}/merge_requests").mock(return_value=httpx.Response(500, text="x" * 10_000))
        with pytest.raises(GitProviderError) as caught:
            await self.provider.create(BRANCH, "t", "d")
        assert len(str(caught.value)) < 800

    async def test_a_list_that_is_not_a_list_of_objects_is_retryable(self):
        respx.get(f"{GITLAB_API}/merge_requests").mock(return_value=httpx.Response(200, json={"iid": 1}))
        with pytest.raises(GitProviderError, match="not a list"):
            await self.provider.find_open(BRANCH)


@respx.mock
class TestGitHub:
    def setup_method(self):
        self.provider = git_provider.GitHubProvider(GITHUB_REPO, TOKEN)

    async def test_find_then_create_as_a_pull_request(self):
        find = respx.get(f"{GITHUB_API}/pulls").mock(return_value=httpx.Response(200, json=[]))
        create = respx.post(f"{GITHUB_API}/pulls").mock(
            return_value=httpx.Response(201, json={"number": 5, "html_url": "https://github.com/t/r/pull/5"})
        )
        ref = await git_provider.ensure_merge_request(self.provider, BRANCH, "Provision x", "desc")
        assert (ref.identifier, ref.created, ref.url) == ("5", True, "https://github.com/t/r/pull/5")
        request = find.calls[0].request
        assert request.headers["Authorization"] == f"Bearer {TOKEN}"
        assert request.headers["Accept"] == "application/vnd.github+json"
        assert dict(request.url.params) == {"head": f"team-redbull:{BRANCH}", "base": "main", "state": "open"}
        assert json.loads(create.calls[0].request.content) == {"title": "Provision x", "head": BRANCH, "base": "main", "body": "desc"}

    async def test_an_open_pull_request_is_found_not_created(self):
        respx.get(f"{GITHUB_API}/pulls").mock(return_value=httpx.Response(200, json=[{"number": 9, "html_url": "https://g/pull/9"}]))
        create = respx.post(f"{GITHUB_API}/pulls")
        ref = await git_provider.ensure_merge_request(self.provider, BRANCH, "t", "d")
        assert (ref.identifier, ref.created) == ("9", False)
        assert not create.called

    @pytest.mark.parametrize(("body", "expected", "sha"), [
        ({"state": "open", "merged": False}, "open", None),
        ({"state": "closed", "merged": False}, "closed", None),
        ({"state": "closed", "merged": True, "merge_commit_sha": "m1"}, "merged", "m1"),
    ])
    async def test_state_is_read_back_with_merged_before_closed(self, body, expected, sha):
        respx.get(f"{GITHUB_API}/pulls/5").mock(return_value=httpx.Response(200, json=body))
        result = await self.provider.state("5")
        assert (result.state, result.merged_sha) == (expected, sha)

    async def test_no_token_sends_no_authorization_header(self):
        provider = git_provider.GitHubProvider(GITHUB_REPO, "")
        route = respx.get(f"{GITHUB_API}/pulls").mock(return_value=httpx.Response(200, json=[]))
        assert await provider.find_open(BRANCH) is None
        assert "Authorization" not in route.calls[0].request.headers

    async def test_a_422_for_an_existing_pull_request_is_rejected_with_githubs_words(self):
        respx.post(f"{GITHUB_API}/pulls").mock(
            return_value=httpx.Response(422, json={"message": "Validation Failed", "errors": [{"message": "A pull request already exists"}]})
        )
        with pytest.raises(GitProviderRequestRejectedError, match="A pull request already exists"):
            await self.provider.create(BRANCH, "t", "d")
