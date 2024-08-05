const INERTIA_LIMIT = "inertia_limit"
const INERTIA_ENERGY_LIMIT = "inertia_energy_limit"
const RAMP_LIMIT = "RampLimit"
const FEEDFORWARD_ENERGY_TARGET = "FF_energy_target"
const FEEDFORWARD_RESERVE_BIN = "FF_reserve_bin"
const FEEDFORWARD_ENERGY_COMMITMENT = "FF_energy_commitment"
const ENERGY_CONTRIBUTION_LIMIT = "energy_contribution_limit"
const ACTIVE_POWER_SHORTAGE = "energy_commitment_shortage"
const FEEDFORWARD_SLACK_COST = 1e5

struct InertiaServiceVariable <: PSI.VariableType end

struct ActivePowerShortageVariable <: PSI.VariableType end

PSI.make_variable_name(
    ::Type{ActivePowerShortageVariable},
    ::Type{T},
) where {T <: PSY.Device} = PSI.encode_symbol(T, "P_shortage")
