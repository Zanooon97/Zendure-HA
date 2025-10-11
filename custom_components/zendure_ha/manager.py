"""Coordinator for Zendure integration."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import traceback
from collections import deque
from collections.abc import Callable
from datetime import datetime, timedelta
from math import sqrt
from typing import Any
from statistics import median

from homeassistant.auth.const import GROUP_ID_USER
from homeassistant.auth.providers import homeassistant as auth_ha
from homeassistant.components import bluetooth, persistent_notification
from homeassistant.components.number import NumberMode
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import Event, EventStateChangedData, HomeAssistant, callback
from homeassistant.helpers.event import async_track_state_change_event
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator
from homeassistant.loader import async_get_integration

from .api import Api
from .const import CONF_P1METER, DOMAIN, DeviceState, SmartMode
from .device import ZendureDevice, ZendureLegacy
from .entity import EntityDevice
from .fusegroup import FuseGroup
from .number import ZendureRestoreNumber
from .select import ZendureRestoreSelect, ZendureSelect
from .sensor import ZendureSensor
from .switch import ZendureRestoreSwitch, ZendureSwitch
from .binary_sensor import ZendureBinarySensor
from .power_distribution import MainState, SubState, decide_substate, distribute_power

SCAN_INTERVAL = timedelta(seconds=60)

_LOGGER = logging.getLogger(__name__)

type ZendureConfigEntry = ConfigEntry[ZendureManager]


class ZendureManager(DataUpdateCoordinator[None], EntityDevice):
    """Class to regular update devices."""

    devices: list[ZendureDevice] = []
    fuseGroups: list[FuseGroup] = []

    def __init__(self, hass: HomeAssistant, entry: ZendureConfigEntry) -> None:
        """Initialize Zendure Manager."""
        super().__init__(hass, _LOGGER, name="Zendure Manager", update_interval=SCAN_INTERVAL, config_entry=entry)
        EntityDevice.__init__(self, hass, "manager", "Zendure Manager", "Zendure Manager")
        self.api = Api()
        self.operation = 0
        self.zero_next = datetime.min
        self.zero_fast = datetime.min
        self.check_reset = datetime.min
        self.power_history: deque[int] = deque(maxlen=5)
        self.power_volatility_history: deque[int] = deque(maxlen=40)
        self.power_home_history: deque[int] = deque(maxlen=10)
        self.p1_history: deque[int] = deque([25, -25], maxlen=8)
        self.pwr_load = 0
        self.pwr_max = 0
        self._rotate_flag = False
        self._detect_volatility = False
        self._first_start = True
        self.p1meterEvent: Callable[[], None] | None = None
        self.update_count = 0
        self._last_volatility = None
        self._volatile_until = None

        self._last_allocation: dict[ZendureDevice, int] = {}
        self._vorlast_allocation: dict[ZendureDevice, int] = {}
        self._starting_device: ZendureDevice | None = None
        self._stopping_device: ZendureDevice | None = None
        self._stopping_devices: set[ZendureDevice] = set()

        self.p1_history_time: deque[tuple[datetime, int]] = deque(maxlen=200)  # Zeitstempel + Wert

    async def loadDevices(self) -> None:
        if self.config_entry is None or (data := await Api.Connect(self.hass, dict(self.config_entry.data), True)) is None:
            return
        if (mqtt := data.get("mqtt")) is None:
            return

        # get version number from integration
        integration = await async_get_integration(self.hass, DOMAIN)
        if integration is None:
            _LOGGER.error("Integration not found for domain: %s", DOMAIN)
            return
        self.attr_device_info["sw_version"] = integration.manifest.get("version", "unknown")

        self.operationmode = (
            ZendureRestoreSelect(self, "Operation", {0: "off", 1: "manual", 2: "smart", 3: "smart_discharging", 4: "smart_charging"}, self.update_operation),
        )
        self.manualpower = ZendureRestoreNumber(self, "manual_power", None, None, "W", "power", 10000, -10000, NumberMode.BOX, True)
        self.availableKwh = ZendureSensor(self, "available_kwh", None, "kWh", "energy", None, 1)
        self.power = ZendureSensor(self, "power", None, "W", "power", None, 0)
        self.power_avg = ZendureSensor(self, "power_avg", None, "W", "power", None, 0)
        self.p1 = ZendureSensor(self, "p1", None, "W", "power", None, 0)
        self.p1_avg = ZendureSensor(self, "p1_avg", None, "W", "power", None, 0)
        self.p1_stddev = ZendureSensor(self, "p1_stddev", None, "W", "power", None, 0)
        self.power_stddev = ZendureSensor(self, "power_stddev", None, "W", "power", None, 0)
        self.power_home_stddev = ZendureSensor(self, "power_home_stddev", None, "W", "power", None, 0)
        self.power_home_average = ZendureSensor(self, "power_home_average", None, "W", "power", None, 0)
        self.isFast = ZendureBinarySensor(self, "isFast")
        self.volatil = ZendureBinarySensor(self, "volatil")
        self.rotate_switch = ZendureSwitch(
            self,
            "manual_rotation_devices",
            self.update_rotation,  # Callback bei Schaltvorgang
            value=False
        )        
        self.detect_volatility_switch = ZendureRestoreSwitch(
            self,
            "detect_volatility_switch",
            self.update_volatility_switch,
            value=False
        )
        # load devices
        for dev in data["deviceList"]:
            try:
                if (deviceId := dev["deviceKey"]) is None or (prodModel := dev["productModel"]) is None:
                    continue
                _LOGGER.info(f"Adding device: {deviceId} {prodModel} => {dev}")

                init = Api.createdevice.get(prodModel.lower(), None)
                if init is None:
                    _LOGGER.info(f"Device {prodModel} is not supported!")
                    continue

                # create the device and mqtt server
                device = init(self.hass, deviceId, prodModel, dev)
                self.devices.append(device)
                Api.devices[deviceId] = device

                if Api.localServer is not None and Api.localServer != "":
                    try:
                        psw = hashlib.md5(deviceId.encode()).hexdigest().upper()[8:24]  # noqa: S324
                        provider: auth_ha.HassAuthProvider = auth_ha.async_get_provider(self.hass)
                        credentials = await provider.async_get_or_create_credentials({"username": deviceId.lower()})
                        user = await self.hass.auth.async_get_user_by_credentials(credentials)
                        if user is None:
                            user = await self.hass.auth.async_create_user(deviceId, group_ids=[GROUP_ID_USER], local_only=False)
                            await provider.async_add_auth(deviceId.lower(), psw)
                            await self.hass.auth.async_link_user(user, credentials)
                        else:
                            await provider.async_change_password(deviceId.lower(), psw)

                        _LOGGER.info(f"Created MQTT user: {deviceId} with password: {psw}")

                    except Exception as err:
                        _LOGGER.error(err)

            except Exception as e:
                _LOGGER.error(f"Unable to create device {e}!")
                _LOGGER.error(traceback.format_exc())

        _LOGGER.info(f"Loaded {len(self.devices)} devices")

        # initialize the api & p1 meter
        await EntityDevice.add_entities()
        self.api.Init(self.config_entry.data, mqtt)
        self.update_p1meter(self.config_entry.data.get(CONF_P1METER, "sensor.power_actual"))
        await asyncio.sleep(1)  # allow other tasks to run
        self.update_fusegroups()

    async def _async_update_data(self) -> None:
        _LOGGER.debug("Updating Zendure data")
        await EntityDevice.add_entities()

        def isBleDevice(device: ZendureDevice, si: bluetooth.BluetoothServiceInfoBleak) -> bool:
            for d in si.manufacturer_data.values():
                try:
                    if d is None or len(d) <= 1:
                        continue
                    sn = d.decode("utf8")[:-1]
                    if device.snNumber.endswith(sn):
                        _LOGGER.info(f"Found Zendure Bluetooth device: {si}")
                        device.attr_device_info["connections"] = {("bluetooth", str(si.address))}
                        return True
                except Exception:  # noqa: S112
                    continue
            return False

        for device in self.devices:
            if isinstance(device, ZendureLegacy) and device.bleMac is None:
                for si in bluetooth.async_discovered_service_info(self.hass, False):
                    if isBleDevice(device, si):
                        break

            _LOGGER.debug(f"Update device: {device.name} ({device.deviceId})")
            await device.dataRefresh(self.update_count)
            device.setStatus()
        self.update_count += 1

        # Manually update the timer
        if self.hass and self.hass.loop.is_running():
            self._schedule_refresh()

    def update_p1meter(self, p1meter: str | None) -> None:
        """Update the P1 meter sensor."""
        _LOGGER.debug("Updating P1 meter to: %s", p1meter)
        if self.p1meterEvent:
            self.p1meterEvent()
        if p1meter:
            self.p1meterEvent = async_track_state_change_event(self.hass, [p1meter], self._p1_changed)
        else:
            self.p1meterEvent = None

    @callback
    async def _p1_changed(self, event: Event[EventStateChangedData]) -> None:
        # update new entities
        await EntityDevice.add_entities()

        # exit if there is nothing to do
        if not self.hass.is_running or not self.hass.is_running or (new_state := event.data["new_state"]) is None:
            return

        try:  # convert the state to a float
            p1 = int(float(new_state.state))
        except ValueError:
            return

        # Check for fast delay
        time = datetime.now()
        if time < self.zero_fast:
            self.p1_history.append(p1)
            return

        # calculate the standard deviation
        isFast = True
        if len(self.p1_history) > 2:
            avg = int(sum(self.p1_history) / len(self.p1_history))
            stddev = max(10, min(50, sqrt(sum([pow(i - avg, 2) for i in self.p1_history]) / len(self.p1_history))))
            p1_stddev = sqrt(sum([pow(i - avg, 2) for i in self.p1_history]) / len(self.p1_history))
            self.p1_avg.update_value(avg)
            self.p1_stddev.update_value(stddev)
            if isFast := abs(p1 - avg) > SmartMode.Threshold * stddev:
                self.p1_history.clear()
                self.isFast.update_value(True)
            else:
                isFast = False
                self.isFast.update_value(False)

        self.p1_history.append(p1)

        # check minimal time between updates
        if isFast or time > self.zero_next:

            self.zero_next = time + timedelta(seconds=SmartMode.TIMEZERO)
            if isFast:
                self.zero_fast = self.zero_next
                await self.powerChanged(p1, True)
            else:
                self.zero_fast = time + timedelta(seconds=SmartMode.TIMEFAST)
                await self.powerChanged(p1, False)

    def count_large_swings(self, values: list[int], delta_threshold: int) -> list[int]:
        """Finde große Schwankungen (Peak ↔ Tal) in einer Werteliste."""
        if len(values) < 3:
            _LOGGER.debug("Zu wenige Werte für Schwankungserkennung: %s", values)
            return []

        extrema = []
        for i in range(1, len(values) - 1):
            if values[i] > values[i-1] and values[i] > values[i+1]:
                extrema.append(values[i])  # lokales Maximum
            elif values[i] < values[i-1] and values[i] < values[i+1]:
                extrema.append(values[i])  # lokales Minimum

        if not extrema:
            _LOGGER.debug("Keine Extrema gefunden in Werten: %s", values)
            return []

        swings = []
        for i in range(1, len(extrema)):
            amplitude = abs(extrema[i] - extrema[i-1])
            swings.append(amplitude)
            _LOGGER.debug(
                "Richtungswechsel #%s: von %s -> %s, Amplitude=%s W",
                i, extrema[i-1], extrema[i], amplitude
            )

        large_swings = [a for a in swings if a >= delta_threshold]

        _LOGGER.info(
            "Schwankungsanalyse: Extrema=%s, Schwankungen=%s, GroßeSchwankungen>=%s=%s",
            extrema, swings, delta_threshold, large_swings
        )

        return large_swings

    def detect_p1_volatility(
        self,
        p1: int,
        pwr_setpoint: int,
        isFast: bool,
        avg_window_size: int = 15,      # Größe des Mittelwert-Fensters für power_volatility_history
        avg_recent_size: int = 7,      # 🚀 NEU: Größe des Fensters für current_p1_avg
        window_sec: int = 60,
        change_threshold: int = 4,
        delta_threshold: int = 35,
        calm_time: int = 30
    ) -> tuple[bool, int]:
        now = datetime.now()
        self.p1_history_time.append((now, p1))
        self.power_volatility_history.append(pwr_setpoint)
        power_volatility_history = self.power_volatility_history
        if isFast:
            self.p1_history_time.clear()
            self.power_volatility_history.clear()
            self._baseline_p1_avg = None

        # Letzte Werte im Zeitfenster
        recent = [val for t, val in self.p1_history_time if (now - t).total_seconds() <= window_sec]
        if len(recent) < 5:
            _LOGGER.debug("Zu wenige Werte im Fenster (%s)", len(recent))
            return False, p1

        # 1) Prüfe ob überhaupt Schwankung da ist
        span = max(recent) - min(recent)
        if span < delta_threshold:
            _LOGGER.debug("Keine relevante Schwankung: max=%s, min=%s, span=%s (<%s)",
                        max(recent), min(recent), span, delta_threshold)
            return False, p1

        # 2) Schwankungen zählen
        large_swings = self.count_large_swings(recent, delta_threshold)

        # 3) Power-Average aus den letzten N Werten
        hist_values = list(power_volatility_history)[-avg_window_size:]
        if hist_values:
            power_avg_recent = int(sum(hist_values) / len(hist_values))
        else:
            power_avg_recent = p1  # fallback

        # 4) Gezappel erkannt
        if len(large_swings) >= change_threshold:
            #Nur die letzten avg_recent_size Werte verwenden
            subset = recent[-avg_recent_size:] if len(recent) > avg_recent_size else recent
            current_p1_avg = int(sum(subset) / len(subset))

            if not hasattr(self, "_baseline_p1_avg") or self._baseline_p1_avg is None:
                self._baseline_p1_avg = current_p1_avg

            correction = current_p1_avg - self._baseline_p1_avg
            setpoint = power_avg_recent + correction
            self._volatile_until = now + timedelta(seconds=calm_time)

            _LOGGER.warning(
                "Gezappel erkannt: %s große Schwankungen -> "
                "Setpoint=power_avg_recent(%s, letzte %s Werte)+Korrektur(%s)=%s",
                len(large_swings), power_avg_recent, avg_window_size, correction, setpoint
            )
            return True, setpoint

        # 5) Calm-Zeit
        if self._volatile_until and now < self._volatile_until:
            subset = recent[-avg_recent_size:] if len(recent) > avg_recent_size else recent
            current_p1_avg = int(sum(subset) / len(subset))
            correction = current_p1_avg - getattr(self, "_baseline_p1_avg", current_p1_avg)
            setpoint = power_avg_recent + correction
            _LOGGER.debug(
                "Calm-Zeit aktiv (%ss verbleibend) -> "
                "Setpoint=power_avg_recent(%s, letzte %s Werte)+Korrektur(%s)=%s",
                int((self._volatile_until - now).total_seconds()),
                power_avg_recent, avg_window_size, correction, setpoint
            )
            return True, setpoint

        # Reset wenn ruhig
        self._baseline_p1_avg = None
        _LOGGER.debug("Keine Volatilität, Rohwert durchgelassen: %s", p1)
        return False, p1

    async def powerChanged(self, p1: int, isFast: bool) -> None:
        """Entscheide MainState und verteile Leistung auf Devices."""
        # get the current power
        availEnergy = 0
        pwr_home = 0
        pwr_battery = 0
        pwr_solar = 0
        devices: list[ZendureDevice] = []
        for d in self.devices:
            if await d.power_get():

                if d.is_bypass or d.is_hand_bypass or d.is_throttled:
                    pwr_home_out = 0
                elif d.is_soc_protect or d.soc_helper_active or d.helper_active or d.extra_candidate_active or d.active:
                    pwr_home_out = d.pwr_home_out
                else:
                    pwr_home_out = 0

                pwr_home += pwr_home_out - (d.pwr_home_in if d.active else 0)
                availEnergy += d.availableKwh.asNumber
                pwr_battery += d.pwr_battery_out - d.pwr_battery_in
                pwr_solar += d.pwr_solar
                devices.append(d)

        # Update the power entities
        pwr_setpoint = pwr_home + p1
        self.power.update_value(pwr_home)
        self.availableKwh.update_value(availEnergy)
        self.p1.update_value(p1)

        #power home stddev
        self.power_home_history.append(pwr_home)
        power_home_average = sum(self.power_home_history) / len(self.power_home_history)
        self.power_home_average.update_value(power_home_average)
        if len(self.power_home_history) > 1:
            power_home_stddev = sqrt(sum([pow(i - power_home_average, 2) for i in self.power_home_history]) / len(self.power_home_history))
            self.power_home_stddev.update_value(power_home_stddev)

        #power stddev
        self.power_history.append(pwr_setpoint)
        power_average = sum(self.power_history) / len(self.power_history)
        self.power_avg.update_value(power_average)
        if len(self.power_history) > 1:
            power_stddev = sqrt(sum([pow(i - power_average, 2) for i in self.power_history]) / len(self.power_history))
            self.power_stddev.update_value(power_stddev)

        if self._detect_volatility:
            self.volatil.update_value(False)
            is_volatile, new_setpoint = self.detect_p1_volatility(p1, pwr_setpoint, isFast)
            if is_volatile:
                self.volatil.update_value(True)
                pwr_setpoint = new_setpoint

        # Update power distribution.
        match self.operation:
            case SmartMode.MATCHING:
                if (power_average > 0 and pwr_setpoint >= 0) or (power_average < 0 and pwr_setpoint <= 0):
                    await self.powerDistribution(devices, power_average, pwr_setpoint, pwr_solar, p1, isFast)
                else:
                    await self.powerDistribution(devices, power_average, 0, pwr_solar, p1, isFast)

            case SmartMode.MATCHING_DISCHARGE:
                await self.powerDistribution(devices, power_average, max(0, pwr_setpoint), pwr_solar, p1, isFast)

            case SmartMode.MATCHING_CHARGE:
                await self.powerDistribution(devices, power_average, min(0, pwr_setpoint), pwr_solar, p1, isFast)

            case SmartMode.MANUAL:
                await self.powerDistribution(devices, int(self.manualpower.asNumber), int(self.manualpower.asNumber), pwr_solar, p1, isFast)

    async def powerDistribution(self, devices: list[ZendureDevice], power_to_devide_avg: int, power_to_devide: int, pwr_solar: int, p1: int, isFast: bool) -> None:

        # Leistung bestimmen (Hauslast + Zählerstand)
        _LOGGER.info(f"PowerChanged: power_to_devide={power_to_devide}, power_to_devide_avg:{power_to_devide_avg}")

        # MainState entscheiden
        main_state = MainState.GRID_CHARGE if power_to_devide < 0 else MainState.GRID_DISCHARGE

        # Device-Daten holen und SubStates setzen
        active_devices = []
        for dev in self.devices:
            sub = decide_substate(dev, main_state)
            dev.state_machine.main = main_state
            dev.state_machine.sub = sub
            active_devices.append(dev)

        # Manager setzt Flag zurück nach Benutzung
        rotate_flag = self._rotate_flag

        # Leistung verteilen
        allocation = distribute_power(active_devices, power_to_devide, main_state, p1, rotate_flag)

        if rotate_flag:
            self._rotate_flag = False
            self.rotate_switch.update_value(0)

        # FuseGroup-Schutz: Allocation nach Sicherungslimits anpassen
        for fg in self.fuseGroups:
            total_power = sum(allocation.get(d, 0) for d in fg.devices)
            # Discharge-Limit prüfen
            if total_power > fg.maxpower:
                factor = fg.maxpower / total_power
                for d in fg.devices:
                    if d in allocation:
                        allocation[d] = int(allocation[d] * factor)
                _LOGGER.warning(f"FuseGroup {fg.name}: Begrenzung Discharge {total_power}W -> {fg.maxpower}W")
            # Charge-Limit prüfen (falls du das auch absichern willst)
            if total_power < fg.minpower:
                factor = fg.minpower / total_power if total_power != 0 else 0
                for d in fg.devices:
                    if d in allocation:
                        allocation[d] = int(allocation[d] * factor)
                _LOGGER.warning(f"FuseGroup {fg.name}: Begrenzung Charge {total_power}W -> {fg.minpower}W")

        # 1) Stop-Liste abarbeiten
        for dev in list(self._stopping_devices):
            # Prüfen ob Gerät wirklich bei 0 angekommen ist
            if dev.pwr_home_out == 0 and dev.pwr_home_in == 0:
                self._stopping_devices.remove(dev)
                _LOGGER.info(f"{dev.name} hat 0 W erreicht → aus Stop-Liste entfernt")
            else:
                # Solange weiter 0 erzwingen
                allocation[dev] = 0
                _LOGGER.debug(f"{dev.name} wird weiterhin auf 0 gehalten (noch nicht bei 0 W)")

        # 2) Geräte merken, die jetzt auf 0 gesetzt wurden
        for dev, power in allocation.items():
            if power == 0 and not dev.is_bypass and not dev.is_throttled and not dev.is_hand_bypass:
                self._stopping_devices.add(dev)
                _LOGGER.info(f"{dev.name} → Stop-Vorgang erkannt")

        # Start-Gerät-Erkennung
        if len(allocation) > 1 and self._starting_device is None and not isFast:

            self._vorlast_allocation = allocation.copy()

            for dev, new_power in allocation.items():
                last_power = self._last_allocation.get(dev, None)

                # Fall 1: Gerät neu drin
                if last_power is None and new_power > 0 and not dev.is_bypass and not dev.is_throttled:
                    self._starting_device = dev
                    _LOGGER.info(f"Startendes Gerät erkannt: {dev.name} Ziel {new_power}W")
                    break

                # Fall 2: Gerät von 0 -> >0
                elif last_power == 0 and new_power > 0 and not dev.is_bypass and not dev.is_throttled:
                    self._starting_device = dev
                    _LOGGER.info(f"Startendes Gerät erkannt (von 0→>0): {dev.name} Ziel {new_power}W")
                    break   

        if self._starting_device and not isFast:
            dev = self._starting_device
            if (main_state == MainState.GRID_DISCHARGE and dev.pwr_home_out > 0) or \
            (main_state == MainState.GRID_CHARGE and dev.pwr_home_in > 0):
                # Kickstart fertig
                _LOGGER.info(f"{dev.name} gestartet, Kickstart beendet.")
                self._starting_device = None
                allocation.update(self._vorlast_allocation)
            else:
                # Kickstart läuft
                if main_state == MainState.GRID_DISCHARGE:
                    await dev.power_discharge(min(dev.limitDischarge, 40))
                else:
                    await dev.power_charge(-40)
                _LOGGER.info(f"Kickstart für {dev.name}: 40W")
                return  # alle anderen Geräte warten
        elif self._starting_device and isFast:
            self._starting_device = None
            #allocation.update(self._vorlast_allocation)
            _LOGGER.warning(f"Kickstart abgebrochen durch isFast")

        #Bereinigen vor ersten start.
        if self._first_start:
            self._first_start = False
            for d in devices:
                if d not in allocation:   # nur Geräte die nicht in allocation stehen
                    allocation[d] = 0

        # Normale Allocation schicken
        for dev, power in allocation.items():
            if main_state == MainState.GRID_DISCHARGE:
                await dev.power_discharge(min(dev.limitDischarge, power))
                _LOGGER.info(f"Discharge={dev.name} power: {power}")
            else:
                if not dev.is_bypass and not dev.is_throttled:
                    await dev.power_charge(-power)
                    _LOGGER.info(f"Charge={dev.name} power: {-power}")
                if dev.is_bypass or dev.is_throttled:
                    await dev.power_discharge(min(dev.limitDischarge, power))
                    _LOGGER.info(f"Discharge={dev.name} power: {power}")

        # Allocation Speichern für nächste Runde
        self._last_allocation = allocation.copy()
        _LOGGER.info(f"Verteilung abgeschlossen: {allocation}") 

    def update_fusegroups(self) -> None:
        _LOGGER.info("Update fusegroups")

        # updateFuseGroup callback
        def updateFuseGroup(_entity: ZendureRestoreSelect, _value: Any) -> None:
            self.update_fusegroups()

        fuseGroups: dict[str, FuseGroup] = {}
        for device in self.devices:
            try:
                if device.fuseGroup.onchanged is None:
                    device.fuseGroup.onchanged = updateFuseGroup

                match device.fuseGroup.state:
                    case "owncircuit" | "group3600":
                        fg = FuseGroup(device.name, 3600, -3600)
                    case "group800":
                        fg = FuseGroup(device.name, 800, -1200)
                    case "group1200":
                        fg = FuseGroup(device.name, 1200, -1200)
                    case "group2000":
                        fg = FuseGroup(device.name, 2000, -2000)
                    case "group2400":
                        fg = FuseGroup(device.name, 2400, -2400)
                    case _:
                        continue

                fg.devices.append(device)
                fuseGroups[device.deviceId] = fg
            except:  # noqa: E722
                _LOGGER.error(f"Unable to create fusegroup for device: {device.name} ({device.deviceId})")

        # Update the fusegroups and select optins for each device
        for device in self.devices:
            try:
                fusegroups: dict[Any, str] = {
                    0: "unused",
                    1: "owncircuit",
                    2: "group800",
                    3: "group1200",
                    4: "group2000",
                    5: "group2400",
                    6: "group3600",
                }
                for deviceId, fg in fuseGroups.items():
                    if deviceId != device.deviceId:
                        fusegroups[deviceId] = f"Part of {fg.name} fusegroup"
                device.fuseGroup.setDict(fusegroups)
            except:  # noqa: E722
                _LOGGER.error(f"Unable to create fusegroup for device: {device.name} ({device.deviceId})")

        # Add devices to fusegroups
        for device in self.devices:
            if fg := fuseGroups.get(device.fuseGroup.value):
                fg.devices.append(device)
            device.setStatus()

        # check if we can split fuse groups
        self.fuseGroups.clear()
        for fg in fuseGroups.values():
            if len(fg.devices) > 1 and fg.maxpower >= sum(d.limitDischarge for d in fg.devices) and fg.minpower <= sum(d.limitCharge for d in fg.devices):
                for d in fg.devices:
                    self.fuseGroups.append(FuseGroup(d.name, d.limitDischarge, d.limitCharge, [d]))
            else:
                for d in fg.devices:
                    d.fuseGrp = fg
                self.fuseGroups.append(fg)

    async def update_operation(self, entity: ZendureSelect, _operation: Any) -> None:
        operation = int(entity.value)
        _LOGGER.info(f"Update operation: {operation} from: {self.operation}")

        self.operation = operation
        self.power_history.clear()
        if self.p1meterEvent is not None:
            if operation != SmartMode.NONE and (len(self.devices) == 0 or all(not d.online for d in self.devices)):
                _LOGGER.warning("No devices online, not possible to start the operation")
                persistent_notification.async_create(self.hass, "No devices online, not possible to start the operation", "Zendure", "zendure_ha")
                return

            match self.operation:
                case SmartMode.NONE:
                    if len(self.devices) > 0:
                        for d in self.devices:
                            await d.power_off()

    async def update_rotation(self, _entity, value: int) -> None:
        """Wird aufgerufen, wenn der Switch in HA betätigt wird."""
        if value == 1:
            self._rotate_flag = True
            _LOGGER.info("Manual rotation switch turned ON → rotation scheduled.")
        else:
            self._rotate_flag = False

    async def update_volatility_switch(self, _entity, value: int) -> None:
        """Wird aufgerufen, wenn der Switch in HA betätigt wird."""
        if value == 1:
            self._detect_volatility = True
            self.detect_volatility_switch.update_value(1)
        else:
            self._detect_volatility = False
            self.detect_volatility_switch.update_value(0)

    def detect_volatility_v3(
        self,
        pwr_setpoint: int,
        power_history: deque[int],
        sign_change_threshold: int = 4,
        min_delta_abs: int = 100,
        min_delta_rel: float = 0.05,
        anti_wobble_time: int = 4,
    ) -> int:
        if len(power_history) < 5:
            _LOGGER.debug("Zu wenige Werte für Volatilitätserkennung (<5) -> Rohwert %s", pwr_setpoint)
            return pwr_setpoint

        last_diff = abs(power_history[-1] - power_history[-2])
        min_window = 10
        max_window = 20
        scale = min(1.0, last_diff / 600)
        short_window_size = int(max_window - scale * (max_window - min_window))

        short_window = list(power_history)[-short_window_size:]
        avg_val = sum(short_window) / len(short_window)
        stddev_val = sqrt(sum((i - avg_val) ** 2 for i in short_window) / len(short_window))

        min_delta = max(min_delta_abs, int(min_delta_rel * abs(avg_val)))
        diffs = [short_window[i] - short_window[i-1] for i in range(1, len(short_window))]
        valid_diffs = [d for d in diffs if abs(d) >= min_delta]
        sign_changes = sum(valid_diffs[k] * valid_diffs[k-1] < 0 for k in range(1, len(valid_diffs)))

        # Debug-Ausgabe
        _LOGGER.debug(
            "VolatilityCheck: setpoint=%s, last_diff=%s, stddev=%.1f, window=%s, sign_changes=%s",
            pwr_setpoint, last_diff, stddev_val, short_window_size, sign_changes
        )

        # 🔹 Gezappel-Erkennung
        volatile = stddev_val > 150 or last_diff > 300 or sign_changes >= sign_change_threshold
        if volatile:
            _LOGGER.warning(
                "Volatilität erkannt! stddev=%.1f, last_diff=%s, sign_changes=%s -> Glättung aktiv",
                stddev_val, last_diff, sign_changes
            )

        # 🔹 Sofortiger Reset bei Ruhe
        instant_diff = abs(power_history[-1] - power_history[-2])
        if instant_diff < 50 and stddev_val < 100:
            if volatile:
                _LOGGER.info(
                    "Gezappel beendet: instant_diff=%s, stddev=%.1f -> zurück zum Rohwert",
                    instant_diff, stddev_val
                )
            volatile = False

        # 🔹 Anti-Wobble
        now = datetime.now()
        if volatile:
            self._last_volatility = now + timedelta(seconds=anti_wobble_time)
            _LOGGER.debug("Anti-Wobble gesetzt bis %s", self._last_volatility)
        elif self._last_volatility is not None and now < self._last_volatility:
            if stddev_val > 100:
                volatile = True
                _LOGGER.debug("Anti-Wobble aktiv, stddev=%.1f -> Glättung verlängert", stddev_val)
            else:
                _LOGGER.debug("Anti-Wobble aktiv, aber stddev niedrig -> keine Glättung")
                volatile = False

        # 🔹 Ausgabe
        if volatile:
            smoothed = int(median(short_window))
            _LOGGER.info("Glättung angewandt: Median(%s Werte) -> %s", short_window_size, smoothed)
            return smoothed
        else:
            _LOGGER.debug("Keine Volatilität -> Rohwert %s", pwr_setpoint)
            return pwr_setpoint
