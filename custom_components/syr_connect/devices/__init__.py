"""Per-device-model entity allowlist overrides.

Each module here is named after a `name` value from
`models.MODEL_SIGNATURES` (e.g. `conelclearprofill.py` for the model with
`"name": "conelclearprofill"`) and optionally defines one POSITIVE allowlist
constant per platform: `SENSOR_KNOWN_KEYS`, `SELECT_KNOWN_KEYS`,
`SWITCH_KNOWN_KEYS`, `BINARY_SENSOR_KNOWN_KEYS`, `BUTTON_KNOWN_KEYS`.

When present, `helpers.get_model_known_keys()` uses these instead of the
global allowlists in const.py, so the entities created for a device are
limited to keys explicitly confirmed for that model rather than the union of
every model's keys. Models without a file here keep using the global
const.py allowlists unchanged.

See const.py's `_SYR_CONNECT_<PLATFORM>_KNOWN_KEYS` for the global fallback allowlists these override.

A module may also define `HISTORY_REQUEST`, the measurement history request
of its model (see safefloor.py). The coordinator then imports the measurements
as long-term statistics.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class HistoryRequest:
    """Measurement history request of a model (cloud API)."""

    url: str  # Endpoint path relative to the API base URL
    report_type: int  # Report type "rtyp"
    measurement_types: dict[str, int]  # Sensor key -> measurement type "t" (unit from _SYR_CONNECT_SENSOR_UNIT)
