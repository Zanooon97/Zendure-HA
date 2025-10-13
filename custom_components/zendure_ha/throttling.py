import logging
import sqlite3
import datetime
from typing import List, Any
from .const import DeviceState

_LOGGER = logging.getLogger(__name__)

class PVRatioMatrix:
    def __init__(self, db_path="pv_ratios.db"):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        cur = self.conn.cursor()
        cur.execute("PRAGMA table_info(pv_ratios)")
        cols = [row[1] for row in cur.fetchall()]
        if "second" not in cols:
            _LOGGER.warning("Alte pv_ratios-Tabelle ohne 'second' erkannt → wird neu erstellt!")
            cur.execute("DROP TABLE IF EXISTS pv_ratios")
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS pv_ratios (
                day TEXT,
                hour INTEGER,
                minute INTEGER,
                second INTEGER,
                device_a TEXT,
                device_b TEXT,
                ratio REAL,
                PRIMARY KEY (day, hour, minute, second, device_a, device_b)
            )
        """)
        self.conn.commit()

    def calibrate(self, devices: list):
        """Speichert alle Ratios zwischen allen Geräten pro Minute"""
        now = datetime.datetime.now()
        day = now.date().isoformat()

        for i, a in enumerate(devices):
            for j, b in enumerate(devices):
                if i == j or a.byPass.is_on or b.byPass.is_on or a.is_throttled or b.is_throttled or a.state == DeviceState.SOCFULL or b.state == DeviceState.SOCFULL:
                    continue
                if a.pwr_solar > 10 and b.pwr_solar > 10:
                    ratio = a.pwr_solar / b.pwr_solar
                    self.conn.execute(
                        "INSERT OR REPLACE INTO pv_ratios VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (day, now.hour, now.minute, now.second, a.name, b.name, ratio)
                    )
                    #_LOGGER.debug(
                    #    f"[CALIBRATION] {a.name}/{b.name}: "
                    #    f"{a.pwr_solar:.1f}W / {b.pwr_solar:.1f}W = Ratio {ratio:.3f}"
                    #)

        # Alte Daten (>7 Tage) entfernen
        self.conn.execute("DELETE FROM pv_ratios WHERE day < date('now', '-7 day')")
        self.conn.commit()

    def get_recent_avg_ratio(self, dev_a: str, dev_b: str, limit: int = 50):
        """Nimmt die letzten `limit` Ratio-Werte zwischen zwei Geräten (zeitunabhängig)"""
        cur = self.conn.execute(
            """
            SELECT ratio FROM pv_ratios
            WHERE device_a=? AND device_b=?
            ORDER BY day DESC, hour DESC, minute DESC, second DESC
            LIMIT ?
            """,
            (dev_a, dev_b, limit)
        )
        rows = [r[0] for r in cur.fetchall()]
        if not rows:
            #_LOGGER.debug(f"[RATIO] Keine Werte für {dev_a}/{dev_b} gefunden.")
            return None
        avg_ratio = sum(rows) / len(rows)
        #_LOGGER.debug(f"[RATIO] Durchschnitt aus {len(rows)} Werten für {dev_a}/{dev_b}: {avg_ratio:.3f}")
        return avg_ratio

    def expected_power(self, dev_a, dev_b_or_devices, min_ref_solar: int = 10):
        """
        Berechne erwartete PV-Leistung von dev_a.
        - Wenn dev_b_or_devices eine Liste ist → nutze alle gültigen Referenzen (wie calc_extra_power)
        - Wenn dev_b_or_devices ein einzelnes Gerät ist → klassischer Einzelvergleich
        """
        # --- Fall 1: Einzelgerät (alter Aufrufstil) ---
        if not isinstance(dev_b_or_devices, list):
            dev_b = dev_b_or_devices
            ratio = self.get_recent_avg_ratio(dev_a.name, dev_b.name)
            if not ratio:
                return None
            expected = dev_b.pwr_solar * ratio
            _LOGGER.debug(
                f"[EXPECTED] {dev_a.name}: erwartet {expected:.1f}W "
                f"(ref {dev_b.name}, ratio={ratio:.3f})"
            )
            return expected

        # --- Fall 2: Liste von Geräten (neuer Stil) ---
        devices = dev_b_or_devices
        refs = [
            r for r in devices
            if r.name != dev_a.name
            and getattr(r, "state", None) != DeviceState.SOCFULL
            and getattr(r, "pwr_solar", 0) > min_ref_solar
            and getattr(r, "soc_lvl", 999) < getattr(dev_a, "soc_lvl", -1)
            and not getattr(r, "byPass", None).is_on
        ]

        if not refs:
            return None

        expected_vals = []
        for r in refs:
            ratio = self.get_recent_avg_ratio(dev_a.name, r.name)
            if ratio is None:
                continue
            expected_vals.append(r.pwr_solar * ratio)

        if not expected_vals:
            return None

        avg_expected = sum(expected_vals) / len(expected_vals)
        _LOGGER.debug(
            f"[EXPECTED] {dev_a.name}: erwartet {avg_expected:.1f}W "
            f"(aus {len(expected_vals)} Referenzen)"
        )
        return avg_expected
        
    def is_throttled(self, dev: Any, devices: List[Any], tolerance: float = 0.15, min_ref_solar: int = 10) -> bool:
        """Prüft, ob Gerät 'dev' gedrosselt ist (über alle gültigen Referenzen)."""
        refs = [
            r for r in devices
            if r.name != dev.name
            and getattr(r, "state", None) != DeviceState.SOCFULL
            and getattr(r, "pwr_solar", 0) > min_ref_solar
            and getattr(r, "soc_lvl", 999) < getattr(dev, "soc_lvl", -1)
            and not r.byPass.is_on
        ]
        if not refs:
            dev._throttle_counter = 0
            return False

        diffs = []
        for r in refs:
            ratio = self.get_recent_avg_ratio(dev.name, r.name)
            if ratio is None:
                continue
            expected = r.pwr_solar * ratio
            if expected <= 0:
                continue
            diff = (expected - dev.pwr_solar) / expected
            diffs.append(diff)

        if not diffs:
            dev._throttle_counter = 0
            return False

        avg_diff = sum(diffs) / len(diffs)

        # --- Hysterese / Zähler ---
        if not hasattr(dev, "_throttle_counter"):
            dev._throttle_counter = 0

        if avg_diff > tolerance:
            dev._throttle_counter += 1
            _LOGGER.debug(f"[THROTTLE] {dev.name} erkannt ({avg_diff*100:.1f}%), Counter={dev._throttle_counter}")
        else:
            if getattr(dev, "_throttle_counter", 0) > 0:
                dev._throttle_counter = 0
            _LOGGER.debug(f"[THROTTLE] {dev.name} OK ({avg_diff*100:.1f}%), Counter={dev._throttle_counter}")

        # Erst nach 3x bestätigen
        if dev._throttle_counter >= 5:
            _LOGGER.info(f"[THROTTLE] {dev.name} gedrosselt erkannt.")
            return True
        else:
            return False

    def calc_extra_power(self, dev: Any, devices: List[Any], min_ref_solar: int = 10) -> int:
        """Berechnet durchschnittliche fehlende Leistung gegenüber allen gültigen Referenzen."""
        refs = [
            r for r in devices
            if r.name != dev.name
            and getattr(r, "state", None) != DeviceState.SOCFULL
            and getattr(r, "pwr_solar", 0) > min_ref_solar
            and getattr(r, "soc_lvl", 999) < getattr(dev, "soc_lvl", -1)
            and not r.byPass.is_on
        ]
        if not refs:
            return 0

        expected_vals = []
        for r in refs:
            ratio = self.get_recent_avg_ratio(dev.name, r.name)
            if ratio is None:
                continue
            expected_vals.append(r.pwr_solar * ratio)

        if not expected_vals:
            return 0

        avg_expected = sum(expected_vals) / len(expected_vals)
        extra = avg_expected - dev.pwr_solar
        _LOGGER.debug(f"[EXTRA] {dev.name}: +{extra:.1f}W (expected={avg_expected:.1f}, actual={dev.pwr_solar:.1f})")
        return int(round(extra))
