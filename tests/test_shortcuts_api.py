"""shortcuts_api: the shortcuts service (go-links), mocked at the httpx layer, and the
ensure_cluster_shortcut activity over it.

Pinned: the requests sent (path, bearer token, body), find-before-create, the
read-back after the create, "taken by us" = success with nothing posted, "taken
by another owner" = ShortcutTakenError (the ONE deterministic answer), and every
other failure — 401/403/400/422 included — the RETRYABLE ShortcutsApiError (the
run is past its merge and cannot be re-run), bodies bounded, the token never in
an error.
"""

from __future__ import annotations

import json

import httpx
import pytest
import respx
from temporalio.testing import ActivityEnvironment

from activities.cluster_lifecycle import shortcuts_api
from shared.exceptions import ShortcutsApiError, ShortcutTakenError
from shared.models.cluster_lifecycle import ClusterShortcut, ClusterShortcutRequest

BASE = "https://shorts.example.internal"
TOKEN = "shorts-token-not-real"
OWNER = "redbull-workflows"
NAME = "prod-herz1-site1-a"
URL = "https://console-openshift-console.apps.ocp4-prod-herz1-site1-a.example.internal"
CHECK = f"{BASE}/check/{NAME}"
CREATE = f"{BASE}/create"
CREATED_BODY = [{"hits": 0, "name": NAME, "url": URL, "id": 140930, "username": OWNER}]


async def _ensure() -> ClusterShortcut:
    return await shortcuts_api.ensure(base_url=BASE, token=TOKEN, owner=OWNER, shortcut=NAME, url=URL)


def _available() -> httpx.Response:
    return httpx.Response(200, json={"available": True, "owner": None})


def _taken(owner: str = OWNER) -> httpx.Response:
    return httpx.Response(200, json={"available": False, "owner": owner})


@pytest.fixture
def mocked():
    # Not `@respx.mock` on the class: a decorated class is no longer collected by pytest.
    with respx.mock:
        yield


@pytest.mark.usefixtures("mocked")
class TestEnsure:
    async def test_an_available_name_is_created_and_read_back(self):
        check = respx.get(CHECK).mock(side_effect=[_available(), _taken()])
        create = respx.post(CREATE).mock(return_value=httpx.Response(200, json=CREATED_BODY))
        result = await _ensure()
        assert result == ClusterShortcut(shortcut=NAME, url=URL, owner=OWNER, created=True)
        assert check.call_count == 2  # the find, then the read-back
        sent = create.calls[0].request
        assert json.loads(sent.content) == {"shortcut": NAME, "username": OWNER, "url": URL}
        for call in (*check.calls, *create.calls):
            assert call.request.headers["Authorization"] == f"Bearer {TOKEN}"

    async def test_a_name_we_own_already_is_success_and_nothing_is_posted(self):
        respx.get(CHECK).mock(return_value=_taken())
        create = respx.post(CREATE)
        result = await _ensure()
        assert result == ClusterShortcut(shortcut=NAME, url=URL, owner=OWNER, created=False)
        assert not create.called

    async def test_a_name_another_owner_holds_is_taken_and_nothing_is_posted(self):
        respx.get(CHECK).mock(return_value=_taken("someone-else"))
        create = respx.post(CREATE)
        with pytest.raises(ShortcutTakenError) as excinfo:
            await _ensure()
        assert "someone-else" in str(excinfo.value) and OWNER in str(excinfo.value)
        assert not create.called

    async def test_owner_compare_is_exact(self):
        respx.get(CHECK).mock(return_value=_taken(OWNER.upper()))
        with pytest.raises(ShortcutTakenError):
            await _ensure()

    async def test_a_create_the_check_does_not_confirm_is_retried(self):
        respx.get(CHECK).mock(side_effect=[_available(), _available()])
        respx.post(CREATE).mock(return_value=httpx.Response(200, json=CREATED_BODY))
        with pytest.raises(ShortcutsApiError, match="still reports it available"):
            await _ensure()

    async def test_a_create_read_back_under_another_owner_is_taken(self):
        respx.get(CHECK).mock(side_effect=[_available(), _taken("raced-in")])
        respx.post(CREATE).mock(return_value=httpx.Response(200, json=CREATED_BODY))
        with pytest.raises(ShortcutTakenError, match="raced-in"):
            await _ensure()

    @pytest.mark.parametrize("status", [400, 401, 403, 404, 409, 422, 500, 503])
    async def test_every_refusal_of_the_check_is_retryable(self, status):
        respx.get(CHECK).mock(return_value=httpx.Response(status, text="nope"))
        with pytest.raises(ShortcutsApiError) as excinfo:
            await _ensure()
        assert str(status) in str(excinfo.value)
        assert TOKEN not in str(excinfo.value)

    @pytest.mark.parametrize("status", [400, 401, 409, 422, 500])
    async def test_every_refusal_of_the_create_is_retryable(self, status):
        respx.get(CHECK).mock(return_value=_available())
        respx.post(CREATE).mock(return_value=httpx.Response(status, json={"detail": "refused"}))
        with pytest.raises(ShortcutsApiError, match=str(status)):
            await _ensure()

    async def test_a_rejected_token_names_the_setting(self):
        respx.get(CHECK).mock(return_value=httpx.Response(401, text="bad token"))
        with pytest.raises(ShortcutsApiError, match="SHORTCUTS_API_TOKEN"):
            await _ensure()

    async def test_an_unreachable_service_is_retryable(self):
        respx.get(CHECK).mock(side_effect=httpx.ConnectError("refused"))
        with pytest.raises(ShortcutsApiError, match="ConnectError"):
            await _ensure()

    @pytest.mark.parametrize(
        "body",
        [[], {"owner": OWNER}, {"available": "no", "owner": OWNER}, {"available": False}, {"available": False, "owner": ""}],
    )
    async def test_an_undocumented_check_answer_is_retryable(self, body):
        respx.get(CHECK).mock(return_value=httpx.Response(200, json=body))
        with pytest.raises(ShortcutsApiError):
            await _ensure()

    async def test_a_body_that_is_not_json_is_retryable(self):
        respx.get(CHECK).mock(return_value=httpx.Response(200, text="<html>proxy</html>"))
        with pytest.raises(ShortcutsApiError, match="not JSON"):
            await _ensure()

    async def test_a_huge_error_body_is_bounded(self):
        respx.get(CHECK).mock(return_value=httpx.Response(502, text="x" * 3_000_000))
        with pytest.raises(ShortcutsApiError) as excinfo:
            await _ensure()
        assert len(str(excinfo.value)) < 1_000 and "characters cut" in str(excinfo.value)

    async def test_the_name_is_one_path_segment(self):
        route = respx.get(f"{BASE}/check/a%2Fb").mock(return_value=_taken())
        await shortcuts_api.ensure(base_url=BASE, token=TOKEN, owner=OWNER, shortcut="a/b", url=URL)
        assert route.called


@respx.mock
async def test_the_activity_builds_the_url_from_the_stem_and_sends_the_owner():
    """conftest: SHORTCUTS_API_URL=https://shorts.test, SHORTCUT_OWNER=redbull-workflows,
    CLUSTER_CONSOLE_URL_TEMPLATE=https://console-openshift-console.apps.{cluster}.test."""
    from activities.cluster_lifecycle.activities import ensure_cluster_shortcut

    respx.get("https://shorts.test/check/prod-herz1-site1-a").mock(
        side_effect=[_available(), _taken("redbull-workflows")]
    )
    create = respx.post("https://shorts.test/create").mock(return_value=httpx.Response(200, json=CREATED_BODY))
    result = await ActivityEnvironment().run(
        ensure_cluster_shortcut, ClusterShortcutRequest(shortcut="prod-herz1-site1-a", cluster="ocp4-prod-herz1-site1-a")
    )
    expected_url = "https://console-openshift-console.apps.ocp4-prod-herz1-site1-a.test"
    assert result == ClusterShortcut(shortcut="prod-herz1-site1-a", url=expected_url, owner="redbull-workflows", created=True)
    assert json.loads(create.calls[0].request.content) == {
        "shortcut": "prod-herz1-site1-a", "username": "redbull-workflows", "url": expected_url,
    }
    assert create.calls[0].request.headers["Authorization"] == "Bearer test-shorts-token"


class TestSettings:
    @pytest.mark.parametrize(
        "template",
        [
            "console.apps.{cluster}.test",  # no scheme
            "https://console.apps.example.test",  # no {cluster}
            "https://console.apps.{clustr}.test",  # a typo
            "https://console.apps.{cluster}.{base}",  # another placeholder
            "https://console.apps.{cluster.test",  # not a format string
        ],
    )
    def test_a_bad_console_template_fails_at_startup(self, template, monkeypatch):
        from pydantic import ValidationError

        from shared.settings.cluster_lifecycle import ClusterLifecycleActivitySettings

        monkeypatch.setenv("CLUSTER_CONSOLE_URL_TEMPLATE", template)
        with pytest.raises(ValidationError, match="cluster_console_url_template"):
            ClusterLifecycleActivitySettings()

    def test_an_empty_owner_fails_at_startup(self, monkeypatch):
        from pydantic import ValidationError

        from shared.settings.cluster_lifecycle import ClusterLifecycleActivitySettings

        monkeypatch.setenv("SHORTCUT_OWNER", "")
        with pytest.raises(ValidationError, match="shortcut_owner"):
            ClusterLifecycleActivitySettings()
