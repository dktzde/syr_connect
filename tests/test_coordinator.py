"""Test the SYR Connect coordinator."""
import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from freezegun.api import FrozenDateTimeFactory
from homeassistant.components.recorder import Recorder, get_instance
from homeassistant.components.recorder.models import StatisticMeanType
from homeassistant.components.recorder.statistics import statistics_during_period
from homeassistant.config_entries import ConfigEntryState
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed, ConfigEntryNotReady, HomeAssistantError
from homeassistant.helpers.update_coordinator import UpdateFailed
from homeassistant.setup import async_setup_component
from homeassistant.util import dt as dt_util
from pytest_homeassistant_custom_component.common import MockConfigEntry
from pytest_homeassistant_custom_component.components.recorder.common import (
    async_recorder_block_till_done,
    async_wait_recording_done,
    do_adhoc_statistics,
)

from custom_components.syr_connect.api_json import SyrConnectJsonAPI
from custom_components.syr_connect.const import (
    _SYR_CONNECT_API_SERVICES,
    _SYR_CONNECT_DEFAULT_CF_BUNDLE_IDENTIFIER,
    API_TYPE_JSON,
    CONF_API_TYPE,
    CONF_HOST,
    CONF_MODEL,
    CONF_SERVICE,
    DOMAIN,
)
from custom_components.syr_connect.coordinator import (
    SyrConnectDataUpdateCoordinator,
    async_import_history,
    hourly_statistics,
)
from custom_components.syr_connect.exceptions import (
    SyrConnectAuthError,
    SyrConnectConnectionError,
)
from custom_components.syr_connect.response_parser import ResponseParser


def _consume_coro_return_task(coro):
    """Safely consume a coroutine passed to `async_create_task` in tests.

    This closes the coroutine (if possible) to avoid "was never awaited"
    warnings and returns a dummy task-like object with a `cancel` and
    `done` method so callers that inspect the returned value behave.
    """
    try:
        close = getattr(coro, "close", None)
        if callable(close):
            try:
                close()
            except Exception:
                pass
    except Exception:
        pass

    class _DummyTask:
        def cancel(self):
            return None

        def done(self):
            return True

    return _DummyTask()


async def test_coordinator_update_success(hass: HomeAssistant, setup_in_progress_config_entry) -> None:
    """Test successful coordinator update."""
    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI") as mock_api_class:
        mock_api = MagicMock()
        mock_api.session_data = "test_session"
        mock_api.projects = [{"id": "project1", "name": "Test Project"}]
        mock_api.login = AsyncMock(return_value=True)
        mock_api.get_devices = AsyncMock(return_value=[
            {
                "id": "device1",
                "name": "Test Device",
                "project_id": "project1",
                "dclg": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
            }
        ])
        mock_api.get_device_status = AsyncMock(return_value={
            "getPRS": "50",
            "getFLO": "10",
        })
        mock_api_class.return_value = mock_api

        config_data = {
            CONF_USERNAME: "test@example.com",
            CONF_PASSWORD: "password",
        }
        coordinator = SyrConnectDataUpdateCoordinator(
            hass,
            MagicMock(),
            config_data,
            60,
        )
        coordinator.config_entry = setup_in_progress_config_entry
        await coordinator.async_config_entry_first_refresh()

        assert coordinator.data is not None
        assert "devices" in coordinator.data
        assert len(coordinator.data["devices"]) == 1
        assert coordinator.data["devices"][0]["id"] == "device1"


async def test_coordinator_update_no_session(hass: HomeAssistant, setup_in_progress_config_entry) -> None:
    """Test coordinator update without session triggers login."""
    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI") as mock_api_class:
        mock_api = MagicMock()
        mock_api.session_data = None  # No session initially
        mock_api.projects = [{"id": "project1", "name": "Test Project"}]
        mock_api.login = AsyncMock(return_value=True)
        mock_api.get_devices = AsyncMock(return_value=[])
        mock_api.is_session_valid = MagicMock(return_value=False)
        mock_api_class.return_value = mock_api

        config_data = {
            CONF_USERNAME: "test@example.com",
            CONF_PASSWORD: "password",
        }
        coordinator = SyrConnectDataUpdateCoordinator(
            hass,
            MagicMock(),
            config_data,
            60,
        )
        coordinator.config_entry = setup_in_progress_config_entry
        # Simulate no session
        mock_api.session_data = None
        await coordinator.async_config_entry_first_refresh()

        # Verify login was called
        mock_api.login.assert_called_once()


async def test_coordinator_set_device_value(hass: HomeAssistant, setup_in_progress_config_entry) -> None:
    """Test setting device value through coordinator."""
    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI") as mock_api_class:
        mock_api = MagicMock()
        mock_api.session_data = "test_session"
        mock_api.projects = [{"id": "project1", "name": "Test Project"}]
        mock_api.get_devices = AsyncMock(return_value=[
            {
                "id": "device1",
                "name": "Test Device",
                "project_id": "project1",
                "dclg": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
            }
        ])
        mock_api.get_device_status = AsyncMock(return_value={})
        mock_api.set_device_status = AsyncMock(return_value=True)
        mock_api.is_session_valid = MagicMock(return_value=True)
        mock_api_class.return_value = mock_api

        config_data = {
            CONF_USERNAME: "test@example.com",
            CONF_PASSWORD: "password",
        }
        coordinator = SyrConnectDataUpdateCoordinator(
            hass,
            MagicMock(),
            config_data,
            60,
        )
        coordinator.config_entry = setup_in_progress_config_entry
        await coordinator.async_config_entry_first_refresh()

        # Set device value
        with patch.object(coordinator, "async_set_updated_data"):
            with patch.object(hass, "async_create_task", side_effect=_consume_coro_return_task):
                await coordinator.async_set_device_value("device1", "setSIR", 0)

        # Verify API call
        mock_api.set_device_status.assert_called_once_with("f47ac10b-58cc-4372-a567-0e02b2c3d479", [("setSIR", 0)])


async def test_set_device_status_api_raises_propagates(hass: HomeAssistant, setup_in_progress_config_entry) -> None:
    """If API.set_device_status raises, the exception should propagate to caller."""
    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI") as mock_api_class:
        mock_api = MagicMock()
        mock_api.session_data = "test_session"
        mock_api.projects = [{"id": "project1", "name": "Test Project"}]
        mock_api.is_session_valid = MagicMock(return_value=True)
        mock_api.get_devices = AsyncMock(return_value=[{"id": "device1", "dclg": "f47ac10b-58cc-4372-a567-0e02b2c3d479"}])
        mock_api.get_device_status = AsyncMock(return_value={})
        mock_api.set_device_status = AsyncMock(side_effect=Exception("set failed"))
        mock_api_class.return_value = mock_api

        config_data = {CONF_USERNAME: "test@example.com", CONF_PASSWORD: "password"}
        coordinator = SyrConnectDataUpdateCoordinator(hass, MagicMock(), config_data, 60)
        coordinator.config_entry = setup_in_progress_config_entry

        # Populate initial data
        await coordinator.async_config_entry_first_refresh()

        # Ensure exception from API call is propagated
        with patch.object(coordinator, "async_set_updated_data"):
            with pytest.raises(Exception, match="set failed"):
                await coordinator.async_set_device_value("device1", "setSIR", 10)


async def test_invalid_command_no_optimistic_update(hass: HomeAssistant, setup_in_progress_config_entry) -> None:
    """Commands not starting with 'set' should not perform optimistic updates."""
    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI") as mock_api_class:
        mock_api = MagicMock()
        mock_api.session_data = "test_session"
        mock_api.projects = [{"id": "project1", "name": "Test Project"}]
        mock_api.get_devices = AsyncMock(return_value=[{"id": "device1", "dclg": "f47ac10b-58cc-4372-a567-0e02b2c3d479", "status": {}}])
        mock_api.get_device_status = AsyncMock(return_value={})
        mock_api.set_device_status = AsyncMock(return_value=True)
        mock_api.is_session_valid = MagicMock(return_value=True)
        mock_api_class.return_value = mock_api

        config_data = {CONF_USERNAME: "test@example.com", CONF_PASSWORD: "password"}
        coordinator = SyrConnectDataUpdateCoordinator(hass, MagicMock(), config_data, 60)
        coordinator.config_entry = setup_in_progress_config_entry
        await coordinator.async_config_entry_first_refresh()

        # Use a non-conforming command
        with patch.object(coordinator, "async_set_updated_data") as mock_async_set_updated_data:
            await coordinator.async_set_device_value("device1", "badCommand", 1)

        # optimistic update should NOT have occurred
        mock_async_set_updated_data.assert_not_called()
        # API call should still be attempted
        mock_api.set_device_status.assert_called_once()


async def test_coordinator_optimistic_update(hass: HomeAssistant, setup_in_progress_config_entry) -> None:
    """Test coordinator optimistic update of in-memory data."""
    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI") as mock_api_class:
        mock_api = MagicMock()
        mock_api.session_data = "test_session"
        mock_api.projects = [{"id": "project1", "name": "Test Project"}]
        mock_api.get_devices = AsyncMock(return_value=[
            {
                "id": "device1",
                "name": "Test Device",
                "project_id": "project1",
                "dclg": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
                "status": {"getSIR": "1"},
            }
        ])
        mock_api.get_device_status = AsyncMock(return_value={"getSIR": "1"})
        mock_api.set_device_status = AsyncMock(return_value=True)
        mock_api.is_session_valid = MagicMock(return_value=True)
        mock_api_class.return_value = mock_api

        config_data = {
            CONF_USERNAME: "test@example.com",
            CONF_PASSWORD: "password",
        }
        coordinator = SyrConnectDataUpdateCoordinator(
            hass,
            MagicMock(),
            config_data,
            60,
        )
        coordinator.config_entry = setup_in_progress_config_entry
        await coordinator.async_config_entry_first_refresh()

        await coordinator.async_set_device_value("device1", "setSIR", 0)

        assert coordinator.data is not None
        device = coordinator.data["devices"][0]
        assert device["status"]["getSIR"] == "0"
        assert device["available"] is True


async def test_coordinator_device_not_found_error(hass: HomeAssistant, setup_in_progress_config_entry) -> None:
    """Test coordinator raises HomeAssistantError when device not found."""
    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI") as mock_api_class:
        mock_api = MagicMock()
        mock_api.session_data = "test_session"
        mock_api.projects = [{"id": "project1", "name": "Test Project"}]
        mock_api.get_devices = AsyncMock(return_value=[])
        mock_api.get_device_status = AsyncMock(return_value={})
        mock_api.is_session_valid = MagicMock(return_value=True)
        mock_api_class.return_value = mock_api

        config_data = {
            CONF_USERNAME: "test@example.com",
            CONF_PASSWORD: "password",
        }
        coordinator = SyrConnectDataUpdateCoordinator(
            hass,
            MagicMock(),
            config_data,
            60,
        )
        coordinator.config_entry = setup_in_progress_config_entry
        await coordinator.async_config_entry_first_refresh()

        # Try to set value for non-existent device
        with patch.object(coordinator, "async_set_updated_data"):
            with patch.object(hass, "async_create_task", side_effect=_consume_coro_return_task):
                with pytest.raises(HomeAssistantError, match="Device unknown_device not found"):
                    await coordinator.async_set_device_value("unknown_device", "setSIR", 0)


async def test_coordinator_no_data_error(hass: HomeAssistant) -> None:
    """Test coordinator raises HomeAssistantError when no data available."""
    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI") as mock_api_class:
        mock_api = MagicMock()
        mock_api_class.return_value = mock_api

        config_data = {
            CONF_USERNAME: "test@example.com",
            CONF_PASSWORD: "password",
        }
        coordinator = SyrConnectDataUpdateCoordinator(
            hass,
            MagicMock(),
            config_data,
            60,
        )

        # Try to set value when coordinator has no data
        with patch.object(coordinator, "async_set_updated_data"):
            with patch.object(hass, "async_create_task", side_effect=_consume_coro_return_task):
                with pytest.raises(HomeAssistantError, match="Coordinator data not available"):
                    await coordinator.async_set_device_value("device1", "setSIR", 0)


async def test_coordinator_device_fetch_exception(hass: HomeAssistant, setup_in_progress_config_entry) -> None:
    """Test coordinator handles exceptions when fetching devices."""
    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI") as mock_api_class:
        mock_api = MagicMock()
        mock_api.session_data = "test_session"
        mock_api.projects = [
            {"id": "project1", "name": "Test Project 1"},
            {"id": "project2", "name": "Test Project 2"},
        ]
        mock_api.is_session_valid = MagicMock(return_value=True)

        # First project raises exception, second succeeds
        async def get_devices_side_effect(project_id):
            if project_id == "project1":
                raise Exception("Failed to fetch devices")
            return [{"id": "device2", "dclg": "c4d5e6f7-a8b9-0123-cdef-123456789012"}]

        mock_api.get_devices = AsyncMock(side_effect=get_devices_side_effect)
        mock_api.get_device_status = AsyncMock(return_value={"getPRS": "50"})
        mock_api_class.return_value = mock_api

        config_data = {
            CONF_USERNAME: "test@example.com",
            CONF_PASSWORD: "password",
        }
        coordinator = SyrConnectDataUpdateCoordinator(
            hass,
            MagicMock(),
            config_data,
            60,
        )
        coordinator.config_entry = setup_in_progress_config_entry
        await coordinator.async_config_entry_first_refresh()

        # Should still have device from second project
        assert coordinator.data is not None
        assert len(coordinator.data["devices"]) == 1
        assert coordinator.data["devices"][0]["id"] == "device2"


async def test_coordinator_non_list_device_result(hass: HomeAssistant, setup_in_progress_config_entry) -> None:
    """Test coordinator handles non-list device results."""
    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI") as mock_api_class:
        mock_api = MagicMock()
        mock_api.session_data = "test_session"
        mock_api.projects = [{"id": "project1", "name": "Test Project"}]
        mock_api.is_session_valid = MagicMock(return_value=True)
        # Return non-list result
        mock_api.get_devices = AsyncMock(return_value="not a list")
        mock_api_class.return_value = mock_api

        config_data = {
            CONF_USERNAME: "test@example.com",
            CONF_PASSWORD: "password",
        }
        coordinator = SyrConnectDataUpdateCoordinator(
            hass,
            MagicMock(),
            config_data,
            60,
        )
        coordinator.config_entry = setup_in_progress_config_entry
        await coordinator.async_config_entry_first_refresh()

        # Should handle gracefully with no devices
        assert coordinator.data is not None
        assert len(coordinator.data["devices"]) == 0


async def test_coordinator_device_status_none(hass: HomeAssistant, setup_in_progress_config_entry) -> None:
    """Test coordinator handles None status (unexpected structure)."""
    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI") as mock_api_class:
        mock_api = MagicMock()
        mock_api.session_data = "test_session"
        mock_api.projects = [{"id": "project1", "name": "Test Project"}]
        mock_api.is_session_valid = MagicMock(return_value=True)
        mock_api.get_devices = AsyncMock(return_value=[
            {"id": "device1", "dclg": "f47ac10b-58cc-4372-a567-0e02b2c3d479", "cna": "Device 1"}
        ])
        # Return None to simulate unexpected structure
        mock_api.get_device_status = AsyncMock(return_value=None)
        mock_api_class.return_value = mock_api

        config_data = {
            CONF_USERNAME: "test@example.com",
            CONF_PASSWORD: "password",
        }
        coordinator = SyrConnectDataUpdateCoordinator(
            hass,
            MagicMock(),
            config_data,
            60,
        )
        coordinator.config_entry = setup_in_progress_config_entry
        await coordinator.async_config_entry_first_refresh()

        # Device should be present with empty status but available
        assert coordinator.data is not None
        assert len(coordinator.data["devices"]) == 1
        device = coordinator.data["devices"][0]
        assert device["id"] == "device1"
        assert device["status"] == {}
        assert device["available"] is True


async def test_coordinator_device_status_none_reuses_previous(hass: HomeAssistant, setup_in_progress_config_entry) -> None:
    """Test coordinator reuses previous status when new status is None."""
    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI") as mock_api_class:
        mock_api = MagicMock()
        mock_api.session_data = "test_session"
        mock_api.projects = [{"id": "project1", "name": "Test Project"}]
        mock_api.is_session_valid = MagicMock(return_value=True)
        mock_api.get_devices = AsyncMock(return_value=[
            {"id": "device1", "dclg": "f47ac10b-58cc-4372-a567-0e02b2c3d479", "cna": "Device 1"}
        ])
        # First call returns valid status, second returns None
        mock_api.get_device_status = AsyncMock(side_effect=[
            {"getPRS": "50", "getFLO": "10"},
            None
        ])
        mock_api_class.return_value = mock_api

        config_data = {
            CONF_USERNAME: "test@example.com",
            CONF_PASSWORD: "password",
        }
        coordinator = SyrConnectDataUpdateCoordinator(
            hass,
            MagicMock(),
            config_data,
            60,
        )
        coordinator.config_entry = setup_in_progress_config_entry

        # First refresh with valid data
        await coordinator.async_config_entry_first_refresh()
        assert coordinator.data["devices"][0]["status"]["getPRS"] == "50"

        # Second refresh returns None - should reuse previous data
        await coordinator.async_refresh()
        assert coordinator.data["devices"][0]["status"]["getPRS"] == "50"
        assert coordinator.data["devices"][0]["available"] is True


async def test_coordinator_device_status_exception(hass: HomeAssistant, setup_in_progress_config_entry) -> None:
    """Test coordinator handles exceptions when fetching device status."""
    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI") as mock_api_class, \
         patch("custom_components.syr_connect.coordinator.create_issue") as mock_create_issue:
        mock_api = MagicMock()
        mock_api.session_data = "test_session"
        mock_api.projects = [{"id": "project1", "name": "Test Project"}]
        mock_api.is_session_valid = MagicMock(return_value=True)
        mock_api.get_devices = AsyncMock(return_value=[
            {"id": "device1", "dclg": "f47ac10b-58cc-4372-a567-0e02b2c3d479", "cna": "Device 1"}
        ])
        # Raise exception when getting device status
        mock_api.get_device_status = AsyncMock(side_effect=Exception("Connection timeout"))
        mock_api_class.return_value = mock_api

        config_data = {
            CONF_USERNAME: "test@example.com",
            CONF_PASSWORD: "password",
        }
        coordinator = SyrConnectDataUpdateCoordinator(
            hass,
            MagicMock(),
            config_data,
            60,
        )
        coordinator.config_entry = setup_in_progress_config_entry
        await coordinator.async_config_entry_first_refresh()

        # Device should be present but marked unavailable
        assert coordinator.data is not None
        assert len(coordinator.data["devices"]) == 1
        device = coordinator.data["devices"][0]
        assert device["id"] == "device1"
        assert device["available"] is False
        assert device["status"] == {}

        # Verify repair issue was created
        mock_create_issue.assert_called_once_with(
            hass,
            "device_offline_device1",
            "device_offline",
            translation_placeholders={"device_name": "Device 1"},
        )


async def test_coordinator_device_status_exception_result(hass: HomeAssistant, setup_in_progress_config_entry) -> None:
    """Test coordinator handles exception results from gather."""
    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI") as mock_api_class:
        mock_api = MagicMock()
        mock_api.session_data = "test_session"
        mock_api.projects = [{"id": "project1", "name": "Test Project"}]
        mock_api.is_session_valid = MagicMock(return_value=True)
        mock_api.get_devices = AsyncMock(return_value=[
            {"id": "device1", "dclg": "f47ac10b-58cc-4372-a567-0e02b2c3d479"},
            {"id": "device2", "dclg": "c4d5e6f7-a8b9-0123-cdef-123456789012"},
        ])

        # First device succeeds, second fails
        call_count = 0
        async def get_device_status_side_effect(dclg):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {"getPRS": "50"}
            raise Exception("Device offline")

        mock_api.get_device_status = AsyncMock(side_effect=get_device_status_side_effect)
        mock_api_class.return_value = mock_api

        config_data = {
            CONF_USERNAME: "test@example.com",
            CONF_PASSWORD: "password",
        }
        coordinator = SyrConnectDataUpdateCoordinator(
            hass,
            MagicMock(),
            config_data,
            60,
        )
        coordinator.config_entry = setup_in_progress_config_entry
        await coordinator.async_config_entry_first_refresh()

        # Should have both devices, one available, one unavailable
        assert coordinator.data is not None
        assert len(coordinator.data["devices"]) == 2


async def test_coordinator_project_devices_auth_error(
    hass: HomeAssistant, setup_in_progress_config_entry
) -> None:
    """If a project device list returns a SyrConnectAuthError, raise auth failure."""
    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI") as mock_api_class:
        mock_api = MagicMock()
        mock_api.session_data = "test_session"
        mock_api.projects = [{"id": "project1", "name": "Test Project"}]
        mock_api.is_session_valid = MagicMock(return_value=True)
        # Simulate auth error raised while fetching device list
        mock_api.get_devices = AsyncMock(side_effect=SyrConnectAuthError("auth failed"))
        mock_api_class.return_value = mock_api

        config_data = {
            CONF_USERNAME: "test@example.com",
            CONF_PASSWORD: "password",
        }
        coordinator = SyrConnectDataUpdateCoordinator(
            hass,
            MagicMock(),
            config_data,
            60,
        )
        coordinator.config_entry = setup_in_progress_config_entry

        # Should raise ConfigEntryAuthFailed when project device fetch encounters auth error
        with pytest.raises(ConfigEntryAuthFailed):
            await coordinator.async_config_entry_first_refresh()


async def test_coordinator_device_status_poll_auth_error(
    hass: HomeAssistant, setup_in_progress_config_entry
) -> None:
    """If a device status poll returns SyrConnectAuthError, raise auth failure."""
    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI") as mock_api_class:
        mock_api = MagicMock()
        mock_api.session_data = "test_session"
        mock_api.projects = [{"id": "project1", "name": "Test Project"}]
        mock_api.is_session_valid = MagicMock(return_value=True)
        mock_api.get_devices = AsyncMock(return_value=[{"id": "device1", "dclg": "f47ac10b-58cc-4372-a567-0e02b2c3d479"}])
        mock_api_class.return_value = mock_api

        config_data = {
            CONF_USERNAME: "test@example.com",
            CONF_PASSWORD: "password",
        }
        coordinator = SyrConnectDataUpdateCoordinator(
            hass,
            MagicMock(),
            config_data,
            60,
        )
        coordinator.config_entry = setup_in_progress_config_entry

        # Patch internal fetch to raise SyrConnectAuthError so gather returns exception result
        with patch.object(coordinator, "_fetch_device_status", side_effect=SyrConnectAuthError("auth")):
            with pytest.raises(ConfigEntryAuthFailed):
                await coordinator.async_config_entry_first_refresh()


async def test_coordinator_auth_error_during_login(hass: HomeAssistant, setup_in_progress_config_entry) -> None:
    """Test coordinator raises ConfigEntryAuthFailed on auth error."""
    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI") as mock_api_class:
        mock_api = MagicMock()
        mock_api.session_data = None
        mock_api.is_session_valid = MagicMock(return_value=False)
        # Simulate auth error during login
        mock_api.login = AsyncMock(side_effect=SyrConnectAuthError("Invalid credentials"))
        mock_api_class.return_value = mock_api

        config_data = {
            CONF_USERNAME: "test@example.com",
            CONF_PASSWORD: "wrong_password",
        }
        coordinator = SyrConnectDataUpdateCoordinator(
            hass,
            MagicMock(),
            config_data,
            60,
        )
        coordinator.config_entry = setup_in_progress_config_entry

        # Should raise ConfigEntryAuthFailed
        with pytest.raises(ConfigEntryAuthFailed, match="Authentication failed"):
            await coordinator.async_config_entry_first_refresh()


async def test_coordinator_connection_error_during_login(hass: HomeAssistant, setup_in_progress_config_entry) -> None:
    """Test coordinator raises UpdateFailed on connection error."""
    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI") as mock_api_class:
        mock_api = MagicMock()
        mock_api.session_data = None
        mock_api.is_session_valid = MagicMock(return_value=False)
        # Simulate connection error during login
        mock_api.login = AsyncMock(side_effect=SyrConnectConnectionError("Network unreachable"))
        mock_api_class.return_value = mock_api

        config_data = {
            CONF_USERNAME: "test@example.com",
            CONF_PASSWORD: "password",
        }
        coordinator = SyrConnectDataUpdateCoordinator(
            hass,
            MagicMock(),
            config_data,
            60,
        )
        coordinator.config_entry = setup_in_progress_config_entry

        # Should raise UpdateFailed wrapped in ConfigEntryNotReady by async_config_entry_first_refresh
        with pytest.raises((UpdateFailed, ConfigEntryNotReady)):
            await coordinator.async_config_entry_first_refresh()


async def test_coordinator_general_exception_during_update(hass: HomeAssistant, setup_in_progress_config_entry) -> None:
    """Test coordinator handles general exceptions during update."""
    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI") as mock_api_class:
        mock_api = MagicMock()
        mock_api.session_data = "test_session"
        mock_api.is_session_valid = MagicMock(return_value=True)
        mock_api.projects = [{"id": "project1", "name": "Test Project"}]
        # Simulate general exception during device fetch - asyncio.gather catches it
        mock_api.get_devices = AsyncMock(return_value=[])
        mock_api_class.return_value = mock_api

        config_data = {
            CONF_USERNAME: "test@example.com",
            CONF_PASSWORD: "password",
        }
        coordinator = SyrConnectDataUpdateCoordinator(
            hass,
            MagicMock(),
            config_data,
            60,
        )
        coordinator.config_entry = setup_in_progress_config_entry

        # Should complete successfully with empty devices
        await coordinator.async_config_entry_first_refresh()
        assert coordinator.data is not None


async def test_coordinator_delete_offline_issue_on_recovery(hass: HomeAssistant, setup_in_progress_config_entry) -> None:
    """Test coordinator deletes repair issue when device comes back online."""
    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI") as mock_api_class, \
         patch("custom_components.syr_connect.coordinator.delete_issue") as mock_delete_issue:
        mock_api = MagicMock()
        mock_api.session_data = "test_session"
        mock_api.projects = [{"id": "project1", "name": "Test Project"}]
        mock_api.is_session_valid = MagicMock(return_value=True)
        mock_api.get_devices = AsyncMock(return_value=[
            {"id": "device1", "dclg": "f47ac10b-58cc-4372-a567-0e02b2c3d479", "cna": "Device 1"}
        ])
        mock_api.get_device_status = AsyncMock(return_value={"getPRS": "50"})
        mock_api_class.return_value = mock_api

        config_data = {
            CONF_USERNAME: "test@example.com",
            CONF_PASSWORD: "password",
        }
        coordinator = SyrConnectDataUpdateCoordinator(
            hass,
            MagicMock(),
            config_data,
            60,
        )
        coordinator.config_entry = setup_in_progress_config_entry
        await coordinator.async_config_entry_first_refresh()

        # Verify delete_issue was called when device status was successfully fetched
        mock_delete_issue.assert_called_once_with(hass, "device_offline_device1")


async def test_coordinator_device_without_cna_fallback_to_id(hass: HomeAssistant, setup_in_progress_config_entry) -> None:
    """Test coordinator uses device ID as fallback when cna is missing."""
    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI") as mock_api_class, \
         patch("custom_components.syr_connect.coordinator.create_issue") as mock_create_issue:
        mock_api = MagicMock()
        mock_api.session_data = "test_session"
        mock_api.projects = [{"id": "project1", "name": "Test Project"}]
        mock_api.is_session_valid = MagicMock(return_value=True)
        mock_api.get_devices = AsyncMock(return_value=[
            {"id": "device1", "dclg": "f47ac10b-58cc-4372-a567-0e02b2c3d479"}  # No 'cna' field
        ])
        mock_api.get_device_status = AsyncMock(side_effect=Exception("Device offline"))
        mock_api_class.return_value = mock_api

        config_data = {
            CONF_USERNAME: "test@example.com",
            CONF_PASSWORD: "password",
        }
        coordinator = SyrConnectDataUpdateCoordinator(
            hass,
            MagicMock(),
            config_data,
            60,
        )
        coordinator.config_entry = setup_in_progress_config_entry
        await coordinator.async_config_entry_first_refresh()

        # Verify create_issue was called with device ID as fallback
        mock_create_issue.assert_called_once_with(
            hass,
            "device_offline_device1",
            "device_offline",
            translation_placeholders={"device_name": "device1"},
        )


async def test_coordinator_optimistic_update_exception_handling(hass: HomeAssistant, setup_in_progress_config_entry) -> None:
    """Test coordinator handles exceptions during optimistic update gracefully."""
    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI") as mock_api_class:
        mock_api = MagicMock()
        mock_api.session_data = "test_session"
        mock_api.projects = [{"id": "project1", "name": "Test Project"}]
        mock_api.get_devices = AsyncMock(return_value=[
            {"id": "device1", "dclg": "f47ac10b-58cc-4372-a567-0e02b2c3d479"}
        ])
        mock_api.get_device_status = AsyncMock(return_value={"getSIR": "1"})
        mock_api.set_device_status = AsyncMock(return_value=True)
        mock_api.is_session_valid = MagicMock(return_value=True)
        mock_api_class.return_value = mock_api

        config_data = {
            CONF_USERNAME: "test@example.com",
            CONF_PASSWORD: "password",
        }
        coordinator = SyrConnectDataUpdateCoordinator(
            hass,
            MagicMock(),
            config_data,
            60,
        )
        coordinator.config_entry = setup_in_progress_config_entry
        await coordinator.async_config_entry_first_refresh()

        # Mock async_set_updated_data to raise exception
        with patch.object(coordinator, "async_set_updated_data", side_effect=ValueError("Update failed")):
            # Should not raise, exception is caught and logged
            await coordinator.async_set_device_value("device1", "setSIR", 0)

        # API call should still have been made
        mock_api.set_device_status.assert_called_once()


async def test_coordinator_refresh_schedule_exception_handling(hass: HomeAssistant, setup_in_progress_config_entry) -> None:
    """Test coordinator handles exceptions when scheduling refresh."""
    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI") as mock_api_class:
        mock_api = MagicMock()
        mock_api.session_data = "test_session"
        mock_api.projects = [{"id": "project1", "name": "Test Project"}]
        mock_api.get_devices = AsyncMock(return_value=[
            {"id": "device1", "dclg": "f47ac10b-58cc-4372-a567-0e02b2c3d479"}
        ])
        mock_api.get_device_status = AsyncMock(return_value={"getSIR": "1"})
        mock_api.set_device_status = AsyncMock(return_value=True)
        mock_api.is_session_valid = MagicMock(return_value=True)
        mock_api_class.return_value = mock_api

        config_data = {
            CONF_USERNAME: "test@example.com",
            CONF_PASSWORD: "password",
        }
        coordinator = SyrConnectDataUpdateCoordinator(
            hass,
            MagicMock(),
            config_data,
            60,
        )
        coordinator.config_entry = setup_in_progress_config_entry
        await coordinator.async_config_entry_first_refresh()

        # Mock async_create_task to raise exception
        def raise_and_close(coro):
            coro.close()
            raise Exception("Task creation failed")

        with patch.object(coordinator, "async_set_updated_data"):
            # Should not raise, exception is caught and logged
            await coordinator.async_set_device_value("device1", "setSIR", 0)

        # API call should still have been made
        mock_api.set_device_status.assert_called_once()


async def test_coordinator_device_without_dclg_uses_id(hass: HomeAssistant, setup_in_progress_config_entry) -> None:
    """Test coordinator uses device ID when dclg is missing."""
    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI") as mock_api_class:
        mock_api = MagicMock()
        mock_api.session_data = "test_session"
        mock_api.projects = [{"id": "project1", "name": "Test Project"}]
        mock_api.is_session_valid = MagicMock(return_value=True)
        mock_api.get_devices = AsyncMock(return_value=[
            {"id": "device1"}  # No 'dclg' field
        ])
        mock_api.get_device_status = AsyncMock(return_value={"getPRS": "50"})
        mock_api_class.return_value = mock_api

        config_data = {
            CONF_USERNAME: "test@example.com",
            CONF_PASSWORD: "password",
        }
        coordinator = SyrConnectDataUpdateCoordinator(
            hass,
            MagicMock(),
            config_data,
            60,
        )
        coordinator.config_entry = setup_in_progress_config_entry
        await coordinator.async_config_entry_first_refresh()

        # Verify get_device_status was called with device id (fallback)
        mock_api.get_device_status.assert_called_once_with("device1")
        assert coordinator.data["devices"][0]["available"] is True


async def test_coordinator_set_value_device_without_dclg(hass: HomeAssistant, setup_in_progress_config_entry) -> None:
    """Test setting value for device without dclg uses device id."""
    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI") as mock_api_class:
        mock_api = MagicMock()
        mock_api.session_data = "test_session"
        mock_api.projects = [{"id": "project1", "name": "Test Project"}]
        mock_api.get_devices = AsyncMock(return_value=[
            {"id": "device1"}  # No 'dclg' field
        ])
        mock_api.get_device_status = AsyncMock(return_value={"getSIR": "1"})
        mock_api.set_device_status = AsyncMock(return_value=True)
        mock_api.is_session_valid = MagicMock(return_value=True)
        mock_api_class.return_value = mock_api

        config_data = {
            CONF_USERNAME: "test@example.com",
            CONF_PASSWORD: "password",
        }
        coordinator = SyrConnectDataUpdateCoordinator(
            hass,
            MagicMock(),
            config_data,
            60,
        )
        coordinator.config_entry = setup_in_progress_config_entry
        await coordinator.async_config_entry_first_refresh()

        with patch.object(coordinator, "async_set_updated_data"):
            with patch.object(hass, "async_create_task", side_effect=_consume_coro_return_task) as mock_task:
                mock_task.return_value = None
                with patch.object(coordinator, "async_refresh", new_callable=AsyncMock):
                    await coordinator.async_set_device_value("device1", "setSIR", 0)

        # Should use device ID as fallback
        mock_api.set_device_status.assert_called_once_with("device1", [("setSIR", 0)])


async def test_coordinator_gather_returns_exception(hass: HomeAssistant, setup_in_progress_config_entry) -> None:
    """Test coordinator handles exception from gather for device tasks."""
    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI") as mock_api_class:
        mock_api = MagicMock()
        mock_api.session_data = "test_session"
        mock_api.projects = [{"id": "project1", "name": "Test Project"}]
        mock_api.is_session_valid = MagicMock(return_value=True)

        # Create an exception that will be returned by gather
        test_exception = Exception("Device fetch failed")
        mock_api.get_devices = AsyncMock(side_effect=test_exception)
        mock_api_class.return_value = mock_api

        config_data = {
            CONF_USERNAME: "test@example.com",
            CONF_PASSWORD: "password",
        }
        coordinator = SyrConnectDataUpdateCoordinator(
            hass,
            MagicMock(),
            config_data,
            60,
        )
        coordinator.config_entry = setup_in_progress_config_entry

        # Should handle exception gracefully
        await coordinator.async_config_entry_first_refresh()

        # Should have no devices
        assert coordinator.data is not None
        assert len(coordinator.data["devices"]) == 0


async def test_coordinator_data_structure_with_empty_devices(hass: HomeAssistant, setup_in_progress_config_entry) -> None:
    """Test coordinator data structure when projects have no devices."""
    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI") as mock_api_class:
        mock_api = MagicMock()
        mock_api.session_data = "test_session"
        mock_api.projects = [
            {"id": "project1", "name": "Empty Project 1"},
            {"id": "project2", "name": "Empty Project 2"},
        ]
        mock_api.is_session_valid = MagicMock(return_value=True)
        mock_api.get_devices = AsyncMock(return_value=[])
        mock_api_class.return_value = mock_api

        config_data = {
            CONF_USERNAME: "test@example.com",
            CONF_PASSWORD: "password",
        }
        coordinator = SyrConnectDataUpdateCoordinator(
            hass,
            MagicMock(),
            config_data,
            60,
        )
        coordinator.config_entry = setup_in_progress_config_entry
        await coordinator.async_config_entry_first_refresh()

        # Verify data structure is correct
        assert coordinator.data is not None
        assert "devices" in coordinator.data
        assert "projects" in coordinator.data


async def test_fetch_device_status_no_ignore_key_keeps_status(hass: HomeAssistant) -> None:
    """Non-getAB API values always pass through unchanged on each poll."""
    from unittest.mock import patch

    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI") as mock_api_class:
        mock_api = MagicMock()
        mock_api.session_data = "test_session"
        mock_api.projects = [{"id": "p1", "name": "P1"}]
        mock_api.is_session_valid = MagicMock(return_value=True)
        mock_api.get_device_status = AsyncMock(return_value={"getPRS": "75"})
        mock_api_class.return_value = mock_api

        config_data = {"username": "u", "password": "p"}
        coord = SyrConnectDataUpdateCoordinator(hass, MagicMock(), config_data, 60)

        device = {"id": "dev1", "dclg": "f47ac10b-58cc-4372-a567-0e02b2c3d479"}

        result = await coord._fetch_device_status(device)

        assert isinstance(result, dict)
        assert result.get("status", {}).get("getPRS") == "75"


async def test_coordinator_unexpected_exception_in_update(hass: HomeAssistant, setup_in_progress_config_entry) -> None:
    """Test coordinator handles unexpected exceptions in update cycle."""
    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI") as mock_api_class:
        mock_api = MagicMock()
        mock_api.session_data = "test_session"
        mock_api.is_session_valid = MagicMock(return_value=True)
        # Make projects property raise an exception when accessed
        type(mock_api).projects = property(lambda self: (_ for _ in ()).throw(RuntimeError("Unexpected error")))
        mock_api_class.return_value = mock_api

        config_data = {
            CONF_USERNAME: "test@example.com",
            CONF_PASSWORD: "password",
        }
        coordinator = SyrConnectDataUpdateCoordinator(
            hass,
            mock_api,
            config_data,
            60,
        )
        coordinator.config_entry = setup_in_progress_config_entry

        # Should raise ConfigEntryNotReady for unexpected errors during first refresh
        from homeassistant.exceptions import ConfigEntryNotReady
        with pytest.raises(ConfigEntryNotReady, match="Error communicating with API"):
            await coordinator.async_config_entry_first_refresh()


async def test_async_update_data_gather_raises(hass: HomeAssistant) -> None:
    """If asyncio.gather raises, coordinator should raise UpdateFailed (lines 154-156)."""
    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI") as mock_api_class, \
         patch("custom_components.syr_connect.coordinator.asyncio.gather", side_effect=Exception("gather failed")):
        mock_api = MagicMock()
        mock_api.session_data = "test_session"
        mock_api.is_session_valid = MagicMock(return_value=True)
        mock_api.projects = [{"id": "p1", "name": "P1"}]
        # Use MagicMock (not AsyncMock) so calling get_devices() returns a plain
        # MagicMock object instead of a coroutine. asyncio.gather is patched to
        # raise before it ever awaits anything, so the tasks don't need to be real
        # coroutines — and a plain MagicMock won't trigger an unawaited-coroutine
        # RuntimeWarning when it is garbage-collected.
        mock_api.get_devices = MagicMock(return_value=[{"id": "d1", "dclg": "d1", "project_id": "p1"}])
        mock_api_class.return_value = mock_api

        config_data = {
            CONF_USERNAME: "test@example.com",
            CONF_PASSWORD: "password",
        }
        coordinator = SyrConnectDataUpdateCoordinator(
            hass,
            MagicMock(),
            config_data,
            60,
        )

        with pytest.raises(UpdateFailed):
            await coordinator._async_update_data()


async def test_async_update_data_gather_returns_exception_result(hass: HomeAssistant) -> None:
    """If asyncio.gather returns Exception objects, they should be skipped."""
    import inspect
    from types import SimpleNamespace

    async def _fake_gather(*args, **kwargs):
        # Close any passed coroutines so they don't trigger unawaited warnings.
        for arg in args:
            if inspect.iscoroutine(arg):
                arg.close()
        return [Exception("proj fail")]

    async def _fake_get_devices(*args, **kwargs):
        return [{"id": "d1", "dclg": "d1", "project_id": "p1"}]

    # Use SimpleNamespace instead of MagicMock to avoid auto-created AsyncMock children
    mock_api = SimpleNamespace(
        session_data="test_session",
        is_session_valid=lambda: True,
        projects=[{"id": "p1", "name": "P1"}],
        get_devices=_fake_get_devices,
    )
    # Plain object for session — not used in this code path and avoids AsyncMock warnings
    fake_session = SimpleNamespace()

    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI", new=lambda *_a, **_kw: mock_api), \
         patch("custom_components.syr_connect.coordinator.asyncio.gather", new=_fake_gather):

        config_data = {
            CONF_USERNAME: "test@example.com",
            CONF_PASSWORD: "password",
        }
        coordinator = SyrConnectDataUpdateCoordinator(
            hass,
            fake_session,
            config_data,
            60,
        )

        result = await coordinator._async_update_data()
        # No devices should be returned since the project result was an Exception
        assert isinstance(result, dict)
        assert result.get("devices") == []


async def test_coordinator_init_json_api(hass: HomeAssistant) -> None:
    """Test coordinator initialization with JSON API."""
    with patch("custom_components.syr_connect.coordinator.SyrConnectJsonAPI") as mock_json_api_class:
        mock_api = MagicMock()
        mock_json_api_class.return_value = mock_api

        config_data = {
            CONF_API_TYPE: API_TYPE_JSON,
            CONF_MODEL: "safetechplus",
            CONF_HOST: "192.168.1.100",
        }
        coordinator = SyrConnectDataUpdateCoordinator(
            hass,
            MagicMock(),
            config_data,
            60,
        )

        # Verify JSON API was created with correct parameters
        mock_json_api_class.assert_called_once()
        call_args = mock_json_api_class.call_args
        assert call_args.kwargs["host"] == "192.168.1.100"
        assert call_args.kwargs["base_path"] == "/trio"
        assert coordinator._api_type == API_TYPE_JSON
        assert coordinator._username is None


async def test_coordinator_init_json_api_invalid_model(hass: HomeAssistant) -> None:
    """Test coordinator initialization with JSON API and invalid model."""
    config_data = {
        CONF_API_TYPE: API_TYPE_JSON,
        CONF_MODEL: "InvalidModel",
        CONF_HOST: "192.168.1.100",
    }

    with pytest.raises(ValueError, match="Model InvalidModel does not support local JSON API"):
        SyrConnectDataUpdateCoordinator(
            hass,
            MagicMock(),
            config_data,
            60,
        )


async def test_coordinator_init_defaults_to_xml_api(hass: HomeAssistant) -> None:
    """Test coordinator initialization defaults to XML API when API type not specified."""
    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI") as mock_xml_api_class:
        mock_api = MagicMock()
        mock_xml_api_class.return_value = mock_api

        # No CONF_API_TYPE specified
        config_data = {
            CONF_USERNAME: "test@example.com",
            CONF_PASSWORD: "password",
        }
        coordinator = SyrConnectDataUpdateCoordinator(
            hass,
            MagicMock(),
            config_data,
            60,
        )

        # Verify XML API was created
        mock_xml_api_class.assert_called_once()
        assert coordinator._api_type == "xml"
        assert coordinator._username == "test@example.com"


async def test_coordinator_init_default_scan_interval(hass: HomeAssistant) -> None:
    """Test coordinator uses default scan interval when none is provided (line 68)."""
    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI") as mock_api_class:
        mock_api_class.return_value = MagicMock()
        config_data = {
            CONF_USERNAME: "test@example.com",
            CONF_PASSWORD: "password",
        }
        # Do NOT pass scan_interval so the None branch fires
        coordinator = SyrConnectDataUpdateCoordinator(
            hass,
            MagicMock(),
            config_data,
        )
        # update_interval should be set to a timedelta (default, non-None)
        assert coordinator.update_interval is not None


async def test_resolve_device_dclg_returns_none_when_device_not_found(hass: HomeAssistant) -> None:
    """_resolve_device_dclg returns None when device_id is not in coordinator data (line 309)."""
    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI") as mock_api_class:
        mock_api_class.return_value = MagicMock()
        config_data = {CONF_USERNAME: "test@example.com", CONF_PASSWORD: "password"}
        coordinator = SyrConnectDataUpdateCoordinator(hass, MagicMock(), config_data, 60)
        coordinator.data = {"devices": [{"id": "other_device", "dclg": "e6f7a8b9-c0d1-2345-efab-345678901234"}], "projects": []}

        result = coordinator._get_device_dclg_from_srn("nonexistent_device")

        assert result is None


async def test_resolve_device_dclg_returns_none_when_no_data(hass: HomeAssistant) -> None:
    """_resolve_device_dclg returns None when coordinator.data is not yet available (line 322)."""
    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI") as mock_api_class:
        mock_api_class.return_value = MagicMock()
        config_data = {CONF_USERNAME: "test@example.com", CONF_PASSWORD: "password"}
        coordinator = SyrConnectDataUpdateCoordinator(hass, MagicMock(), config_data, 60)
        # data is None by default before the first successful update
        coordinator.data = None

        result = coordinator._get_device_dclg_from_srn("any_device")

        assert result is None


async def test_fetch_device_status_propagates_auth_error(hass: HomeAssistant) -> None:
    """If API raises `SyrConnectAuthError`, `_fetch_device_status` should re-raise it (covers line ~291)."""
    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI") as mock_api_class:
        mock_api = MagicMock()
        # make the underlying API raise an auth error when fetching status
        mock_api.get_device_status = AsyncMock(side_effect=SyrConnectAuthError("auth failed"))
        mock_api_class.return_value = mock_api

        config_data = {CONF_USERNAME: "test@example.com", CONF_PASSWORD: "password"}
        coord = SyrConnectDataUpdateCoordinator(hass, MagicMock(), config_data, 60)

        with pytest.raises(SyrConnectAuthError):
            await coord._fetch_device_status({"id": "deviceX", "dclg": "d5e6f7a8-b9c0-1234-defa-234567890123"})


async def test_coordinator_device_status_result_is_exception(
    hass: HomeAssistant, setup_in_progress_config_entry
) -> None:
    """Test coordinator handles exception result from _fetch_device_status gather (lines 183-184)."""
    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI") as mock_api_class:
        mock_api = MagicMock()
        mock_api.session_data = "test_session"
        mock_api.projects = [{"id": "project1", "name": "Test Project"}]
        mock_api.is_session_valid = MagicMock(return_value=True)
        mock_api.get_devices = AsyncMock(return_value=[{"id": "device1", "dclg": "f47ac10b-58cc-4372-a567-0e02b2c3d479"}])
        mock_api_class.return_value = mock_api

        config_data = {
            CONF_USERNAME: "test@example.com",
            CONF_PASSWORD: "password",
        }
        coordinator = SyrConnectDataUpdateCoordinator(
            hass,
            MagicMock(),
            config_data,
            60,
        )
        coordinator.config_entry = setup_in_progress_config_entry

        # Patch _fetch_device_status to raise so gather captures it as an exception result
        with patch.object(
            coordinator,
            "_fetch_device_status",
            side_effect=RuntimeError("unexpected internal error"),
        ):
            await coordinator.async_config_entry_first_refresh()

        # No devices should appear since the only device fetch raised
        assert coordinator.data is not None
        assert coordinator.data["devices"] == []


async def test_fetch_device_status_iwh_compute_error_is_logged(
    hass: HomeAssistant, setup_in_progress_config_entry
) -> None:
    """get_sensor_iwh_value raising ValueError/TypeError is caught and logged (lines 263-264)."""
    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI") as mock_api_class, \
         patch(
             "custom_components.syr_connect.coordinator.get_sensor_iwh_value",
             side_effect=ValueError("bad conductivity"),
         ):
        mock_api = MagicMock()
        mock_api.session_data = "test_session"
        mock_api.projects = [{"id": "project1", "name": "Test Project"}]
        mock_api.is_session_valid = MagicMock(return_value=True)
        mock_api.get_devices = AsyncMock(
            return_value=[{"id": "device1", "dclg": "f47ac10b-58cc-4372-a567-0e02b2c3d479", "project_id": "project1"}]
        )
        mock_api.get_device_status = AsyncMock(return_value={"getCND": "bad"})
        mock_api_class.return_value = mock_api

        config_data = {CONF_USERNAME: "test@example.com", CONF_PASSWORD: "password"}
        coordinator = SyrConnectDataUpdateCoordinator(hass, MagicMock(), config_data, 60)
        coordinator.config_entry = setup_in_progress_config_entry

        # Should complete without raising — the ValueError is caught and only logged
        await coordinator.async_config_entry_first_refresh()

        assert coordinator.data is not None
        assert len(coordinator.data["devices"]) == 1
        assert coordinator.data["devices"][0]["id"] == "device1"


async def test_coordinator_device_marked_unavailable_when_dst_is_0_or_1(
    hass: HomeAssistant, setup_in_progress_config_entry
) -> None:
    """When the API returns dst=1 the device must be marked unavailable (offline)."""
    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI") as mock_api_class:
        mock_api = MagicMock()
        mock_api.session_data = "test_session"
        mock_api.projects = [{"id": "project1", "name": "Test Project"}]
        mock_api.is_session_valid = MagicMock(return_value=True)
        mock_api.get_devices = AsyncMock(
            return_value=[{"id": "device1", "name": "Device 1", "dclg": "f47ac10b-58cc-4372-a567-0e02b2c3d479", "project_id": "project1"}]
        )
        # dst=1 means the device is offline / not reachable via the cloud
        mock_api.get_device_status = AsyncMock(return_value={"dst": "1", "getPRS": "50"})
        mock_api_class.return_value = mock_api

        config_data = {CONF_USERNAME: "test@example.com", CONF_PASSWORD: "password"}
        coordinator = SyrConnectDataUpdateCoordinator(hass, MagicMock(), config_data, 60)
        coordinator.config_entry = setup_in_progress_config_entry
        await coordinator.async_config_entry_first_refresh()

        assert coordinator.data is not None
        device = coordinator.data["devices"][0]
        assert device["available"] is False


async def test_coordinator_device_marked_unavailable_when_dst_is_0(
    hass: HomeAssistant, setup_in_progress_config_entry
) -> None:
    """When the API returns dst=0 (never been online) the device must be marked unavailable."""
    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI") as mock_api_class:
        mock_api = MagicMock()
        mock_api.session_data = "test_session"
        mock_api.projects = [{"id": "project1", "name": "Test Project"}]
        mock_api.is_session_valid = MagicMock(return_value=True)
        mock_api.get_devices = AsyncMock(
            return_value=[{"id": "device1", "name": "Device 1", "dclg": "f47ac10b-58cc-4372-a567-0e02b2c3d479", "project_id": "project1"}]
        )
        # dst=0 means the device has never been online
        mock_api.get_device_status = AsyncMock(return_value={"dst": "0", "getPRS": "50"})
        mock_api_class.return_value = mock_api

        config_data = {CONF_USERNAME: "test@example.com", CONF_PASSWORD: "password"}
        coordinator = SyrConnectDataUpdateCoordinator(hass, MagicMock(), config_data, 60)
        coordinator.config_entry = setup_in_progress_config_entry
        await coordinator.async_config_entry_first_refresh()

        assert coordinator.data is not None
        device = coordinator.data["devices"][0]
        assert device["available"] is False


async def test_coordinator_device_available_when_dst_is_2(
    hass: HomeAssistant, setup_in_progress_config_entry
) -> None:
    """When the API returns dst=2 (online) the device must remain available."""
    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI") as mock_api_class:
        mock_api = MagicMock()
        mock_api.session_data = "test_session"
        mock_api.projects = [{"id": "project1", "name": "Test Project"}]
        mock_api.is_session_valid = MagicMock(return_value=True)
        mock_api.get_devices = AsyncMock(
            return_value=[{"id": "device1", "name": "Device 1", "dclg": "f47ac10b-58cc-4372-a567-0e02b2c3d479", "project_id": "project1"}]
        )
        mock_api.get_device_status = AsyncMock(return_value={"dst": "2", "getPRS": "50"})
        mock_api_class.return_value = mock_api

        config_data = {CONF_USERNAME: "test@example.com", CONF_PASSWORD: "password"}
        coordinator = SyrConnectDataUpdateCoordinator(hass, MagicMock(), config_data, 60)
        coordinator.config_entry = setup_in_progress_config_entry
        await coordinator.async_config_entry_first_refresh()

        assert coordinator.data is not None
        device = coordinator.data["devices"][0]
        assert device["available"] is True


async def test_async_clear_device_alarm_xml_api_sends_clr_command(
    hass: HomeAssistant, setup_in_progress_config_entry
) -> None:
    """async_clear_device_alarm sends clrALA via set_device_status for XML API."""
    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI") as mock_api_class:
        mock_api = MagicMock()
        mock_api.session_data = "test_session"
        mock_api.projects = [{"id": "project1", "name": "Test Project"}]
        mock_api.is_session_valid = MagicMock(return_value=True)
        mock_api.get_devices = AsyncMock(return_value=[{"id": "device1", "dclg": "f47ac10b-58cc-4372-a567-0e02b2c3d479"}])
        mock_api.get_device_status = AsyncMock(return_value={})
        mock_api_class.return_value = mock_api

        config_data = {CONF_USERNAME: "test@example.com", CONF_PASSWORD: "password"}
        coordinator = SyrConnectDataUpdateCoordinator(hass, MagicMock(), config_data, 60)
        coordinator.config_entry = setup_in_progress_config_entry
        coordinator.data = {"devices": [{"id": "device1", "dclg": "f47ac10b-58cc-4372-a567-0e02b2c3d479", "status": {}}], "projects": []}
        mock_api.set_device_status = AsyncMock(return_value=True)
        coordinator.api = mock_api

        coordinator.hass.async_create_task = MagicMock(side_effect=_consume_coro_return_task)
        await coordinator.async_clear_device_alarm("device1")

        mock_api.set_device_status.assert_awaited_once_with("f47ac10b-58cc-4372-a567-0e02b2c3d479", [("clrALA", "")])


async def test_async_clear_device_alarm_json_api_success(hass: HomeAssistant) -> None:
    """async_clear_device_alarm succeeds with JSON API."""
    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI") as mock_xml_api_class:
        mock_xml_api_class.return_value = MagicMock()

        config_data = {CONF_USERNAME: "test@example.com", CONF_PASSWORD: "password"}
        coordinator = SyrConnectDataUpdateCoordinator(hass, MagicMock(), config_data, 60)

        mock_api = MagicMock(spec=SyrConnectJsonAPI)
        mock_api.request_json_data = AsyncMock(return_value=None)
        coordinator.api = mock_api

        await coordinator.async_clear_device_alarm("device1")

        mock_api.request_json_data.assert_called_once_with("clr/ala")


async def test_coordinator_init_xml_api_with_conf_service(hass: HomeAssistant) -> None:
    """Test coordinator XML API init looks up service parameters when CONF_SERVICE is set (lines 122-127)."""
    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI") as mock_xml_api_class:
        mock_xml_api_class.return_value = MagicMock()

        svc = _SYR_CONNECT_API_SERVICES[_SYR_CONNECT_DEFAULT_CF_BUNDLE_IDENTIFIER]
        config_data = {
            CONF_USERNAME: "test@example.com",
            CONF_PASSWORD: "password",
            CONF_SERVICE: svc["cf_bundle_identifier"],
        }
        SyrConnectDataUpdateCoordinator(hass, MagicMock(), config_data, 60)

        call_kwargs = mock_xml_api_class.call_args.kwargs
        assert call_kwargs["api_app_name"] == svc["api_app_name"]
        assert call_kwargs["api_base_url"] == svc["api_base_url"]
        assert call_kwargs["cf_bundle_identifier"] == svc["cf_bundle_identifier"]


# --- Measurement history (SafeFloor) -----------------------------------------------------------

SAFEFLOOR_SERIAL = "123456789"
SAFEFLOOR_DCLG = "7605aa61-73b8-ef11-800c-be3af2b6059f"
SAFEFLOOR_STATISTICS_URL = "WebServices/SyrControlWebServiceTest2.asmx/GetSafeFloorStatistics"
SAFEFLOOR_TEMPERATURE_XML = (
    Path(__file__).parent / "fixtures/xml/SyrSafeFloor_GetSafeFloorStatistics_Temperature.xml"
).read_text(encoding="utf-8")
TEMPERATURE_STATISTIC_ID = "syr_connect:123456789_getcel_history"
UPLOAD_1 = "28.09.2026 14:55:43"
UPLOAD_2 = "02.10.2026 14:55:40"


def _utc(text: str) -> datetime:
    return datetime.strptime(text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)


def _safefloor_status(upload: str | None = UPLOAD_1) -> dict:
    status = {"getSRN": SAFEFLOOR_SERIAL, "getVER": "Syr Floorsensor 2.23", "getCEL": "158", "getHMD": "81", "dst": "3"}
    if upload is not None:
        status["getSRN_dt"] = upload
    return status


def _recent(hours: int) -> datetime:
    return dt_util.utcnow().replace(microsecond=0) - timedelta(hours=hours)


def _history_coordinator(
    hass: HomeAssistant, status: dict, history: AsyncMock, recorder: bool = True
) -> tuple[SyrConnectDataUpdateCoordinator, MagicMock]:
    """Return a coordinator for one device (XML API) whose history requests go to `history`."""
    if recorder:
        hass.config.components.add("recorder")
    with patch("custom_components.syr_connect.coordinator.SyrConnectXmlAPI") as mock_api_class:
        mock_api = mock_api_class.return_value
        mock_api.projects = [{"id": "project1", "name": "Test Project"}]
        mock_api.is_session_valid = MagicMock(return_value=True)
        mock_api.get_devices = AsyncMock(
            side_effect=lambda _project_id: [{"id": SAFEFLOOR_SERIAL, "dclg": SAFEFLOOR_DCLG, "name": "Floor"}]
        )
        mock_api.get_device_status = AsyncMock(return_value=status)
        mock_api.get_device_history = history
        coordinator = SyrConnectDataUpdateCoordinator(
            hass, MagicMock(), {CONF_USERNAME: "test@example.com", CONF_PASSWORD: "password"}, 60
        )
    coordinator.config_entry = MockConfigEntry(domain=DOMAIN, state=ConfigEntryState.SETUP_IN_PROGRESS)
    return coordinator, mock_api


async def _refresh(hass: HomeAssistant, coordinator: SyrConnectDataUpdateCoordinator, first: bool = False) -> None:
    """Refresh and wait for the history import running in the background."""
    if first:
        await coordinator.async_config_entry_first_refresh()
    else:
        await coordinator.async_refresh()
    await hass.async_block_till_done(wait_background_tasks=True)


async def test_coordinator_history_imported_once_per_upload(hass: HomeAssistant) -> None:
    """The history is fetched after the start and then only when getSRN_dt changes."""
    measurements = [(_recent(hours=2), 15.8)]
    history = AsyncMock(return_value=measurements)
    coordinator, mock_api = _history_coordinator(hass, _safefloor_status(), history)

    with patch("custom_components.syr_connect.coordinator.async_import_history") as mock_import:
        await _refresh(hass, coordinator, first=True)
        assert [c.args for c in history.await_args_list] == [
            (SAFEFLOOR_DCLG, SAFEFLOOR_STATISTICS_URL, 1, 4, "°C"),
            (SAFEFLOOR_DCLG, SAFEFLOOR_STATISTICS_URL, 2, 4, "%"),
        ]
        assert [c.args[1:] for c in mock_import.call_args_list] == [
            (TEMPERATURE_STATISTIC_ID, "Floor Temperature history", "°C", measurements),
            ("syr_connect:123456789_gethmd_history", "Floor Humidity history", "%", measurements),
        ]

        # Same upload: nothing is fetched
        await _refresh(hass, coordinator)
        assert history.await_count == 2

        # New upload: fetched again
        mock_api.get_device_status.return_value = _safefloor_status(UPLOAD_2)
        await _refresh(hass, coordinator)
        assert history.await_count == 4
        assert coordinator.last_update_success


async def test_coordinator_history_ignores_implausible_timestamps(hass: HomeAssistant) -> None:
    """Measurements outside the cloud window (e.g. from a reset sensor clock) are not imported."""
    recent = (_recent(hours=2), 15.8)
    history = AsyncMock(return_value=[(_utc("2000-01-01 00:00:00"), 0.0), recent, (_recent(hours=-1), 1.0)])
    coordinator, _ = _history_coordinator(hass, _safefloor_status(), history)

    with patch("custom_components.syr_connect.coordinator.async_import_history") as mock_import:
        await _refresh(hass, coordinator, first=True)

    assert [c.args[4] for c in mock_import.call_args_list] == [[recent], [recent]]


async def test_coordinator_history_does_not_delay_the_update(hass: HomeAssistant) -> None:
    """The history is imported in the background, at most one import at a time."""
    release = asyncio.Event()

    async def slow_history(*_args) -> list:
        await release.wait()
        return [(_recent(hours=2), 15.8)]

    history = AsyncMock(side_effect=slow_history)
    coordinator, _ = _history_coordinator(hass, _safefloor_status(), history)

    with patch("custom_components.syr_connect.coordinator.async_import_history"):
        await coordinator.async_config_entry_first_refresh()
        await coordinator.async_refresh()
        assert coordinator.last_update_success
        assert history.await_count == 1  # the second refresh does not start another import

        release.set()
        await hass.async_block_till_done(wait_background_tasks=True)
    assert coordinator._history_imported_uploads == {SAFEFLOOR_SERIAL: UPLOAD_1}


async def test_coordinator_history_failure_is_retried_later(
    hass: HomeAssistant, caplog: pytest.LogCaptureFixture
) -> None:
    """A failed fetch keeps the update working, is logged once and retried after 3 hours."""
    history = AsyncMock(side_effect=SyrConnectConnectionError("cloud down"))
    coordinator, _ = _history_coordinator(hass, _safefloor_status(), history)

    with patch("custom_components.syr_connect.coordinator.async_import_history") as mock_import:
        await _refresh(hass, coordinator, first=True)
        assert coordinator.last_update_success
        assert coordinator.data["devices"][0]["status"]["getCEL"] == "158"
        mock_import.assert_not_called()
        retry_in = coordinator._history_retry_at[SAFEFLOOR_SERIAL] - datetime.now(UTC)
        assert timedelta(hours=2, minutes=59) < retry_in <= timedelta(hours=3)

        # Within the retry delay nothing is fetched
        await _refresh(hass, coordinator)
        assert history.await_count == 1

        # After the retry delay it is fetched again; the repeated failure is not warned again
        coordinator._history_retry_at[SAFEFLOOR_SERIAL] = datetime.now(UTC) - timedelta(seconds=1)
        await _refresh(hass, coordinator)
        assert history.await_count == 2
        warnings = [r for r in caplog.records if r.levelname == "WARNING" and "measurement history" in r.message]
        assert len(warnings) == 1

        # Success
        history.side_effect = None
        history.return_value = []
        coordinator._history_retry_at[SAFEFLOOR_SERIAL] = datetime.now(UTC) - timedelta(seconds=1)
        await _refresh(hass, coordinator)
        assert history.await_count == 4
        assert coordinator._history_retry_at == {}
        assert coordinator._history_imported_uploads == {SAFEFLOOR_SERIAL: UPLOAD_1}


@pytest.mark.parametrize(
    ("status", "recorder"),
    [
        ({"getSRN": "210AAA12345", "getSRN_dt": UPLOAD_1, "getPRS": "50"}, True),
        ({}, True),
        (_safefloor_status(upload=None), True),
        (_safefloor_status(), False),
    ],
    ids=["no_safefloor", "empty_status", "no_upload_timestamp", "no_recorder"],
)
async def test_coordinator_history_not_fetched(hass: HomeAssistant, status: dict, recorder: bool) -> None:
    """Other models, a status without upload timestamp or a missing recorder: no request, no error."""
    history = AsyncMock(return_value=[])
    coordinator, _ = _history_coordinator(hass, status, history, recorder)

    await _refresh(hass, coordinator, first=True)

    assert coordinator.last_update_success
    assert coordinator._history_task is None
    history.assert_not_awaited()


async def test_coordinator_history_not_due_for_json_api(hass: HomeAssistant) -> None:
    """The local JSON API has no history request."""
    coordinator, _ = _history_coordinator(hass, _safefloor_status(), AsyncMock())
    coordinator.api = MagicMock(spec=SyrConnectJsonAPI)

    assert coordinator._history_request_due({"id": SAFEFLOOR_SERIAL, "status": _safefloor_status()}) is None


def _row(start: str, mean: float, low: float, high: float) -> dict:
    return {"start": _utc(start), "mean": pytest.approx(mean), "min": low, "max": high}


def test_hourly_statistics_repeats_the_hour_before() -> None:
    """Every hour from the first to the last measurement gets a row; hours without one repeat the hour before."""
    rows = hourly_statistics([(_utc("2026-09-26 20:45:27"), 14.9), (_utc("2026-09-26 14:45:27"), 14.6)])

    assert [row["start"].hour for row in rows] == list(range(14, 21))
    assert rows[0] == _row("2026-09-26 14:00:00", 14.6, 14.6, 14.6)
    assert rows[5] == _row("2026-09-26 19:00:00", 14.6, 14.6, 14.6)
    assert rows[6] == _row("2026-09-26 20:00:00", 14.9, 14.9, 14.9)


def test_hourly_statistics_several_measurements_within_an_hour() -> None:
    """Measurements of the same hour are combined into mean, min and max."""
    rows = hourly_statistics([
        (_utc("2026-09-26 14:05:00"), 14.0),
        (_utc("2026-09-26 14:35:00"), 16.0),
        (_utc("2026-09-26 15:10:00"), 18.0),
    ])

    assert rows == [_row("2026-09-26 14:00:00", 15.0, 14.0, 16.0), _row("2026-09-26 15:00:00", 18.0, 18.0, 18.0)]
    assert hourly_statistics([]) == []


def test_import_history_metadata(hass: HomeAssistant) -> None:
    """Metadata describes an external mean statistic with unit and unit class; nothing without measurements."""
    measurements = ResponseParser.parse_device_history_response(SAFEFLOOR_TEMPERATURE_XML)

    with patch("custom_components.syr_connect.coordinator.async_add_external_statistics") as mock_add:
        async_import_history(hass, TEMPERATURE_STATISTIC_ID, "Floor Temperature history", "°C", [])
        mock_add.assert_not_called()
        async_import_history(hass, TEMPERATURE_STATISTIC_ID, "Floor Temperature history", "°C", measurements)

    _, metadata, statistics = mock_add.call_args.args
    assert metadata == {
        "has_sum": False,
        "mean_type": StatisticMeanType.ARITHMETIC,
        "name": "Floor Temperature history",
        "source": "syr_connect",
        "statistic_id": TEMPERATURE_STATISTIC_ID,
        "unit_class": "temperature",
        "unit_of_measurement": "°C",
    }
    # 26.09. 14:00 (first measurement) to 28.09. 14:00 (last measurement)
    assert len(statistics) == 49


async def _hourly_means(hass: HomeAssistant, statistic_id: str, start: datetime) -> list[tuple[str, float]]:
    stats = await get_instance(hass).async_add_executor_job(
        statistics_during_period, hass, start, None, {statistic_id}, "hour", None, {"mean"}
    )
    return [
        (datetime.fromtimestamp(row["start"], UTC).strftime("%d. %H:%M"), round(row["mean"], 3))
        for row in stats.get(statistic_id, [])
    ]


async def test_import_history_into_recorder(recorder_mock: Recorder, hass: HomeAssistant) -> None:
    """The measurements end up in the long-term statistics, importing the same days again adds no rows."""
    measurements = ResponseParser.parse_device_history_response(SAFEFLOOR_TEMPERATURE_XML)

    async_import_history(hass, TEMPERATURE_STATISTIC_ID, "Floor Temperature history", "°C", measurements)
    async_import_history(hass, TEMPERATURE_STATISTIC_ID, "Floor Temperature history", "°C", measurements)
    await async_wait_recording_done(hass)

    rows = await _hourly_means(hass, TEMPERATURE_STATISTIC_ID, _utc("2026-09-26 00:00:00"))
    assert len(rows) == 49
    assert rows[0] == ("26. 14:00", 14.6)
    assert rows[5] == ("26. 19:00", 14.6)
    assert rows[6] == ("26. 20:00", 14.9)
    assert rows[-1] == ("28. 14:00", 15.8)


async def test_import_history_does_not_interfere_with_the_recorder(
    recorder_mock: Recorder,
    hass: HomeAssistant,
    freezer: FrozenDateTimeFactory,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """External statistics can be written for an hour the recorder has not compiled yet.

    This is why the history is not written into the statistics of the sensor entities:
    the recorder compiles each of their hours exactly once and would fail for an hour
    that already has a row.
    """
    entity_id = "sensor.floor_temperature"
    attributes = {"device_class": "temperature", "state_class": "measurement", "unit_of_measurement": "°C"}
    await async_setup_component(hass, "sensor", {})
    await async_recorder_block_till_done(hass)
    freezer.move_to(_utc("2026-10-02 10:00:00"))
    hass.states.async_set(entity_id, "15.0", attributes)
    await async_wait_recording_done(hass)

    # Upload at 11:20: its measurements reach into the running hour 11:00
    freezer.move_to(_utc("2026-10-02 11:20:00"))
    measurements = [(_utc("2026-10-02 09:00:00"), 16.0), (_utc("2026-10-02 11:10:00"), 18.0)]
    async_import_history(hass, TEMPERATURE_STATISTIC_ID, "Floor Temperature history", "°C", measurements)
    await async_wait_recording_done(hass)

    # Afterwards the recorder compiles 10:00 and 11:00 of the sensor
    freezer.move_to(_utc("2026-10-02 12:00:00"))
    for minutes in range(0, 120, 5):
        do_adhoc_statistics(hass, start=_utc("2026-10-02 10:00:00") + timedelta(minutes=minutes))
    await async_wait_recording_done(hass)

    start = _utc("2026-10-02 00:00:00")
    assert await _hourly_means(hass, entity_id, start) == [("02. 10:00", 15.0), ("02. 11:00", 15.0)]
    assert await _hourly_means(hass, TEMPERATURE_STATISTIC_ID, start) == [
        ("02. 09:00", 16.0),
        ("02. 10:00", 16.0),
        ("02. 11:00", 18.0),
    ]
    assert "Blocked attempt to insert duplicated statistic rows" not in caplog.text
