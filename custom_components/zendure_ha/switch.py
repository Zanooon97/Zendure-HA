"""Interfaces with the Zendure Integration switch."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any

from homeassistant.components.switch import SwitchEntity, SwitchEntityDescription
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.restore_state import RestoreEntity
from homeassistant.helpers.template import Template

from .entity import EntityDevice, EntityZendure

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(_hass: HomeAssistant, _config_entry: ConfigEntry, async_add_entities: AddEntitiesCallback) -> None:
    """Set up the Zendure switch."""
    ZendureSwitch.add = async_add_entities


class ZendureSwitch(EntityZendure, SwitchEntity):
    add: AddEntitiesCallback

    def __init__(
        self,
        device: EntityDevice,
        uniqueid: str,
        onwrite: Callable,
        template: Template | None = None,
        deviceclass: Any | None = None,
        value: bool | None = None,
    ) -> None:
        """Initialize a switch entity."""
        super().__init__(device, uniqueid, "switch")
        self.entity_description = SwitchEntityDescription(key=uniqueid, name=uniqueid, device_class=deviceclass)

        self._attr_available = True
        self._value_template: Template | None = template
        self._onwrite = onwrite
        self._attr_is_on = value if value is not None else False

        device.add_entity(self.add, self)

    def update_value(self, value: Any) -> bool:
        try:
            is_on = bool(
                int(self._value_template.async_render_with_possible_json_value(value, None)) != 0
                if self._value_template is not None
                else int(value) != 0
            )

            if self._attr_is_on == is_on:
                return False

            _LOGGER.info(f"Update switch: {self._attr_unique_id} => {is_on}")
            self._attr_is_on = is_on

            if self.hass and self.hass.loop.is_running():
                self.schedule_update_ha_state()

        except Exception as err:
            _LOGGER.error(f"Error {err} setting state: {self._attr_unique_id} => {value}")
        return True

    async def async_turn_on(self, **_kwargs: Any) -> None:
        """Turn switch on."""
        if asyncio.iscoroutinefunction(self._onwrite):
            await self._onwrite(self, 1)
        else:
            self._onwrite(self, 1)
        self._attr_is_on = True
        self.schedule_update_ha_state()

    async def async_turn_off(self, **_kwargs: Any) -> None:
        """Turn switch off."""
        if asyncio.iscoroutinefunction(self._onwrite):
            await self._onwrite(self, 0)
        else:
            self._onwrite(self, 0)
        self._attr_is_on = False
        self.schedule_update_ha_state()


class ZendureRestoreSwitch(ZendureSwitch, RestoreEntity):
    """Representation of a Zendure switch entity with restore."""

    async def async_added_to_hass(self) -> None:
        """Restore previous state when entity is added to Home Assistant."""
        await super().async_added_to_hass()

        if state := await self.async_get_last_state():
            if state.state in ["on", "off"]:
                restored_state = state.state == "on"
                self._attr_is_on = restored_state
                _LOGGER.info(f"Restored state for {self._attr_unique_id}: {restored_state}")

                # Optional: sync restored state to device
                if self._onwrite is not None:
                    try:
                        if asyncio.iscoroutinefunction(self._onwrite):
                            await self._onwrite(self, 1 if restored_state else 0)
                        else:
                            self._onwrite(self, 1 if restored_state else 0)
                    except Exception as err:
                        _LOGGER.warning(f"Could not restore device state for {self._attr_unique_id}: {err}")
