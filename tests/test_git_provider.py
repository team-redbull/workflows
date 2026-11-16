"""git_provider: GitLab's merge-request API, mocked at the httpx layer.

GitLab is the ONLY provider (CLAUDE.md §4): nothing here may name another.
What is pinned is the request it sends (path, auth header, body)
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
# The air-gap's layout: the project sits two subgroups deep, and the whole path is ONE encoded segment.
GITLAB_REPO = "https://gitlab.example.internal/redbull/gitops-day1/platform-config.git"
GITLAB_API = "https://gitlab.example.internal/api/v4/projects/redbull%2Fgitops-day1%2Fplatform-config"
BRANCH = "provision-cluster/ocp4-prep-e2e-site2-a"


class TestRepoPath:
    @pytest.mark.parametrize("url", [GITLAB_REPO, GITLAB_REPO.removesuffix(".git"), GITLAB_REPO + "/"])
    def test_a_gitlab_project_path_is_url_encoded(self, url):
        assert git_provider.GitLabProvider(url, TOKEN)._base == GITLAB_API

    @pytest.mark.parametrize("url", ["git@gitlab.example.internal:redbull/repo.git", "https://gitlab.example.internal/", "https://gitlab.example.internal/only-group"])
    def test_a_url_that_is_not_a_clone_url_is_refused(self, url):
        with pytest.raises(GitProviderRequestRejectedError):
            git_provider.GitLabProvider(url, TOKEN)

    def test_gitlab_is_the_only_provider(self):
        """One convention (the operator, 2026-11-14): no second provider class, no selector."""
        import inspect

        source = inspect.getsource(git_provider).lower()
        assert "github" not in source and "pull request" not in source
        assert not hasattr(git_provider, "provider_for")


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

    async def test_a_fast_forward_merge_reports_the_merge_requests_head(self):
        # GitLab's answer for a fast-forward merge, as the 2qsc5 instance gave it (19.5.3).
        respx.get(f"{GITLAB_API}/merge_requests/42").mock(return_value=httpx.Response(
            200, json={"state": "merged", "merge_commit_sha": None, "squash_commit_sha": None, "sha": "head1"}
        ))
        assert (await self.provider.state("42")).merged_sha == "head1"

    async def test_the_merge_commit_wins_over_the_head(self):
        respx.get(f"{GITLAB_API}/merge_requests/42").mock(return_value=httpx.Response(
            200, json={"state": "merged", "merge_commit_sha": "m1", "squash_commit_sha": None, "sha": "head1"}
        ))
        assert (await self.provider.state("42")).merged_sha == "m1"

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

    async def test_no_token_sends_no_private_token_header(self):
        provider = git_provider.GitLabProvider(GITLAB_REPO, "")
        route = respx.get(f"{GITLAB_API}/merge_requests").mock(return_value=httpx.Response(200, json=[]))
        assert await provider.find_open(BRANCH) is None
        assert "PRIVATE-TOKEN" not in route.calls[0].request.headers

    async def test_gitlabs_field_errors_are_carried_in_the_message(self):
        # GitLab's 409 for a second open merge request from one branch: `message` is a LIST.
        respx.post(f"{GITLAB_API}/merge_requests").mock(return_value=httpx.Response(
            409, json={"message": ["Another open merge request already exists for this source branch: !1"]}
        ))
        with pytest.raises(GitProviderError, match="Another open merge request already exists"):
            await self.provider.create(BRANCH, "t", "d")
