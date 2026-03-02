const PSY_LOADS = Union{PSY.StandardLoad, PSY.PowerLoad}
const PSY_THERMAL_GENERATORS = Union{PSY.ThermalStandard, PSY.ThermalMultiStart}

const DEFAULT_TIME_RESOLUTION = 60
const DEFAULT_HOURS_PER_YEAR = 8760

const DEFAULT_LOAD_YEAR = 2000
const DEFAULT_RTS_LOAD = 75.0 # GW

const BASE_POWER = 100.0 # MW