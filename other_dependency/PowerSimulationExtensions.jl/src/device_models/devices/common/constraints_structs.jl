abstract type AbstractServiceConstraintInfo <: PSI.AbstractConstraintInfo end

struct ServiceRampConstraintInfo <: PSI.AbstractRampConstraintInfo
    component_name::String
    ramp_limits::PSI.UpDown
end

struct InertiaServiceConstraintInfo <: PSI.AbstractRangeConstraintInfo
    component_name::String
    h_constant::Float64
    base_power::Float64
    additional_terms_ub::Vector{Symbol}
    additional_terms_lb::Vector{Symbol}
end

function InertiaServiceConstraintInfo(
    component_name::String,
    h_constant::Float64,
    base_power::Float64,
)
    return InertiaServiceConstraintInfo(
        component_name,
        h_constant,
        base_power,
        Vector{Symbol}(),
        Vector{Symbol}(),
    )
end

struct InertiaServiceRenewableConstraintInfo <: PSI.AbstractRangeConstraintInfo
    component_name::String
    h_constant::Float64
    multiplier::Float64
    timeseries::Vector{Float64}
    additional_terms_ub::Vector{Symbol}
    additional_terms_lb::Vector{Symbol}
end

function InertiaServiceRenewableConstraintInfo(
    name::String,
    h_constant::Float64,
    multiplier::Float64,
    timeseries,
)
    ts::Vector{Float64} = timeseries
    return InertiaServiceRenewableConstraintInfo(
        name,
        h_constant,
        multiplier,
        ts,
        Vector{Symbol}(),
        Vector{Symbol}(),
    )
end

struct InertiaCommitmentConstraintInfo <: AbstractServiceConstraintInfo
    component_name::String
    ic_power::Union{Nothing, PSI.InitialCondition}
    h_constant::Float64
    base_power::Float64
end
