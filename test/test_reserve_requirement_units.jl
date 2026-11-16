# Unit tests for reserve-requirement units and realized ORDC provision. No solver needed.
#
#   reserve_requirement_scaling_factors   MW requirement -> values stored in the "requirement"
#                                         time series (PSI enforces value * get_requirement, pu)
#   update_realized_reserve_perc!         realized UC provision for ReserveDemandCurve products
#                                         (long-format results; storage uses the device method)
#
# The end-to-end check of the canonical reserve path (create_sys_w_updated_ts) is in
# test_constructed_systems_generator.jl.
#
# Run from REPL:
#   include("test/test_reserve_requirement_units.jl")

include(joinpath(@__DIR__, "includes.jl"))

using Dates

const RRU_E = EMISAgentSimulation

function _rru_system(base_power::Float64)
    sys = PSY.System(base_power)
    bus = PSY.ACBus(;
        number = 1, name = "bus1", available = true, bustype = PSY.ACBusTypes.REF,
        angle = 0.0, magnitude = 1.0, voltage_limits = (min = 0.9, max = 1.1), base_voltage = 230.0,
    )
    PSY.add_component!(sys, bus)
    PSY.add_component!(sys, PSY.ThermalStandard(;
        name = "th_1", available = true, status = true, bus = bus, active_power = 0.0,
        reactive_power = 0.0, rating = 2.0, active_power_limits = (min = 0.0, max = 2.0),
        reactive_power_limits = nothing, ramp_limits = nothing,
        operation_cost = PSY.ThermalGenerationCost(nothing), base_power = 100.0,
    ))
    return sys
end

_rru_reg_up(requirement) = PSY.VariableReserve{PSY.ReserveUp}(;
    name = "Reg_Up", available = true, time_frame = 300.0, requirement = requirement)

@testset "reserve_requirement_scaling_factors" begin
    mw = [55.0, 50.0, 62.5, 120.0]

    @testset "enforced requirement (value x get_requirement, system pu) equals the input MW" begin
        for base in (100.0, 50.0)
            sys = _rru_system(base)
            service = _rru_reg_up(0.72)
            PSY.add_component!(sys, service)
            factors = RRU_E.reserve_requirement_scaling_factors(mw, service, sys)
            enforced_pu = factors .* PSY.get_requirement(service)     # what PSI puts on the RHS
            @test enforced_pu .* base ≈ mw
        end
    end

    @testset "regression: MW must not be read as per-unit (the 100x error)" begin
        sys = _rru_system(100.0)
        service = _rru_reg_up(0.72)
        PSY.add_component!(sys, service)
        factors = RRU_E.reserve_requirement_scaling_factors(mw, service, sys)
        @test factors .* PSY.get_requirement(service) ≈ mw ./ 100
        @test !(factors .* PSY.get_requirement(service) ≈ mw)
    end

    @testset "integer input is accepted" begin
        sys = _rru_system(100.0)
        service = _rru_reg_up(0.5)
        PSY.add_component!(sys, service)
        @test RRU_E.reserve_requirement_scaling_factors([50, 100], service, sys) ≈ [1.0, 2.0]
    end

    @testset "a zero requirement cannot scale an MW series" begin
        sys = _rru_system(100.0)
        service = _rru_reg_up(0.0)
        PSY.add_component!(sys, service)
        @test_throws ArgumentError RRU_E.reserve_requirement_scaling_factors(mw, service, sys)
    end
end

@testset "realized ORDC provision (update_realized_reserve_perc!, ReserveDemandCurve)" begin
    sys = _rru_system(100.0)
    device = PSY.get_component(PSY.ThermalStandard, sys, "th_1")
    service = PSY.ReserveDemandCurve{PSY.ReserveUp}(nothing, "Synchronous", true, 600.0)
    base_power = 100.0
    n = 4
    stamps = DateTime(2020, 1, 1) .+ Hour.(0:(n - 1))
    th_mw = [10.0, 0.0, 35.5, 120.0]
    # Long format, as returned by PSI.read_realized_variables.
    key = "ActivePowerReserveVariable__ReserveDemandCurve__ReserveUp__Synchronous"
    results_uc = Dict(key => DataFrames.DataFrame(
        DateTime = vcat(stamps, stamps),
        name = vcat(fill("th_1", n), fill("other_unit", n)),
        value = vcat(th_mw, fill(99.0, n)),
    ))
    fresh() = Dict("th_1" => Dict("Synchronous" => zeros(1, n)))
    perc_md, perc_uc, perc_ed = fresh(), fresh(), fresh()
    products = SubString{String}["Synchronous"]

    RRU_E.update_realized_reserve_perc!(device, service, nothing, results_uc, nothing,
        perc_md, perc_uc, perc_ed, Dict{String, Array{Float64, 2}}(),
        products, products, products, base_power, false, true)

    @test vec(perc_uc["th_1"]["Synchronous"]) ≈ th_mw ./ RRU_E.get_device_size(device) ./ base_power
    @test any(>(0), perc_uc["th_1"]["Synchronous"])          # was all zero before the fix
    @test all(iszero, perc_md["th_1"]["Synchronous"])         # MD not run (md_market_bool = false)

    @testset "device absent from the results: warn and leave zeros" begin
        perc_uc2 = Dict("th_1" => Dict("Synchronous" => zeros(1, n)))
        absent = Dict(key => DataFrames.DataFrame(DateTime = stamps, name = fill("other_unit", n),
            value = fill(5.0, n)))
        @test_logs (:warn, r"No UC reserve variable found for th_1 - Synchronous") match_mode = :any RRU_E.update_realized_reserve_perc!(
            device, service, nothing, absent, nothing, fresh(), perc_uc2, fresh(),
            Dict{String, Array{Float64, 2}}(), products, products, products, base_power, false, true)
        @test all(iszero, perc_uc2["th_1"]["Synchronous"])
    end

    @testset "storage uses the device method (its ActivePowerReserveVariable includes charge + discharge)" begin
        m = which(RRU_E.update_realized_reserve_perc!,
            Tuple{PSY.EnergyReservoirStorage, PSY.ReserveDemandCurve{PSY.ReserveUp},
                  Nothing, Dict{String, DataFrames.DataFrame}, Nothing,
                  Dict{String, Dict{String, Array{Float64, 2}}}, Dict{String, Dict{String, Array{Float64, 2}}},
                  Dict{String, Dict{String, Array{Float64, 2}}}, Dict{String, Array{Float64, 2}},
                  Vector{SubString{String}}, Vector{SubString{String}}, Vector{SubString{String}},
                  Float64, Bool, Bool})
        @test m.sig.parameters[2] == PSY.Device
    end
end
