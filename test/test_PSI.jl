# PSI/PSY API compatibility tests.
# Guards against regressions from the PSI 0.31→0.37 / PSY 4.x→5.11 upgrade.
# Run from REPL after loading the package:
#   using EMISAgentSimulation
#   include("test/test_PSI.jl")

using Test
using PowerSimulations
using PowerSystems
using StorageSystemsSimulations
using HydroPowerSimulations
import EMISAgentSimulation

const PSI = PowerSimulations
const PSY = PowerSystems
const SSI = StorageSystemsSimulations
const HSI = HydroPowerSimulations

@testset "PSI/PSY API compatibility (PSI ~0.37 / PSY ~5.11)" begin

    # ── Package version checks ──────────────────────────────────────────────
    @testset "Package versions are in expected range" begin
        @test pkgversion(PowerSimulations) >= v"0.37.0"
        @test pkgversion(PowerSimulations) < v"0.38.0"
        @test pkgversion(PowerSystems) >= v"5.0.0"
    end

    # ── Regression: serialize kwarg removed from PSI.build! in 0.37 ────────
    @testset "PSI.build! has no serialize kwarg" begin
        # Old call was PSI.build!(sim; serialize=false); 0.37 dropped the kwarg.
        @test !any(m -> :serialize ∈ Base.kwarg_decl(m), methods(PSI.build!))
    end

    # ── Regression: PSY 5.x renamed to plural form ─────────────────────────
    @testset "PSY.get_time_series_resolutions (plural form) exists" begin
        @test hasmethod(PSY.get_time_series_resolutions, Tuple{PSY.System})
    end

    # ── Key PSI network / simulation types ─────────────────────────────────
    @testset "Key PSI model and simulation types exist" begin
        for sym in [
            :ProblemTemplate, :NetworkModel, :AreaBalancePowerModel,
            :CopperPlateBalanceConstraint, :DecisionModel, :SimulationModels,
            :SimulationSequence, :Simulation, :SimulationResults,
            :ServiceModel, :RequirementConstraint, :InterProblemChronology,
            :OptimizationContainer, :OperationModel, :SimulationState,
        ]
            @test isdefined(PSI, sym) broken=false
        end
    end

    # ── Device formulations used in UC/ED/MD templates ─────────────────────
    @testset "PSI device formulations used in templates exist" begin
        for sym in [
            :ThermalBasicUnitCommitment, :ThermalBasicDispatch,
            :RenewableFullDispatch, :FixedOutput, :StaticPowerLoad,
            :StaticBranch, :HVDCTwoTerminalLossless, :RangeReserve,
        ]
            @test isdefined(PSI, sym) broken=false
        end
    end

    # ── Variable / constraint types ─────────────────────────────────────────
    @testset "PSI variable and constraint types exist" begin
        for sym in [
            :EnergyVariable, :OnVariable, :ActivePowerVariable,
            :SystemBalanceSlackUp, :SystemBalanceSlackDown,
            :ReserveRequirementSlack, :VariableKey,
            :ActivePowerReserveVariable, :DeviceStatus,
            :InitialEnergyLevel, :InitialCondition,
        ]
            @test isdefined(PSI, sym) broken=false
        end
    end

    # ── SimulationState APIs used in update_initial_conditions! overrides ──
    @testset "PSI simulation state APIs exist" begin
        for sym in [
            :get_system_state_value, :get_decision_state_data,
            :get_dataset_value, :get_update_timestamp, :get_system_state_data,
            :set_ic_quantity!, :get_value,
        ]
            @test isdefined(PSI, sym) broken=false
        end
    end

    # ── Internal PSI APIs used in add_constraint_dual! / adjust_reserve_voll! ─
    @testset "PSI internal APIs used in custom dispatch overrides exist" begin
        for sym in [
            :get_duals, :get_service_name, :get_available_components,
            :get_time_steps, :add_dual_container!, :assign_dual_variable!,
            :should_write_resulting_value, :get_optimization_container,
            :get_variables, :add_to_objective_variant_expression!,
            :get_component, :get_component_type, :get_component_name,
        ]
            @test isdefined(PSI, sym) broken=false
        end
    end

    # ── Feedforward types ───────────────────────────────────────────────────
    @testset "PSI feedforward types exist" begin
        @test isdefined(PSI, :SemiContinuousFeedforward)
    end

    # ── HydroPowerSimulations formulations ─────────────────────────────────
    @testset "HydroPowerSimulations formulations exist" begin
        @test isdefined(HSI, :HydroCommitmentRunOfRiver)
        @test isdefined(HSI, :HydroDispatchRunOfRiver)
    end

    # ── StorageSystemsSimulations types ────────────────────────────────────
    @testset "StorageSystemsSimulations types exist" begin
        @test isdefined(SSI, :StorageDispatchWithReserves)
        @test isdefined(SSI, :StorageEnergyShortageVariable)
        @test isdefined(SSI, :EnergyTargetFeedforward)
    end

    # ── Simulation results API ──────────────────────────────────────────────
    @testset "PSI simulation results API exists" begin
        @test hasmethod(
            PSI.get_decision_problem_results,
            Tuple{PSI.SimulationResults, String},
        )
        @test isdefined(PSI, :read_realized_duals)
        @test isdefined(PSI, :read_realized_variables)
    end

    # ── Template construction (no PSY system needed) ───────────────────────
    @testset "UC template construction without error (no inertia)" begin
        template = EMISAgentSimulation.create_uc_template([])
        @test template isa PSI.ProblemTemplate
    end

    @testset "ED template construction without error (no inertia)" begin
        template = EMISAgentSimulation.create_ed_template([])
        @test template isa PSI.ProblemTemplate
    end

    @testset "MD template construction without error (no inertia)" begin
        template = EMISAgentSimulation.create_md_template([])
        @test template isa PSI.ProblemTemplate
    end

    # ── Custom dispatch overrides ───────────────────────────────────────────
    @testset "Custom should_write_resulting_value override suppresses StorageEnergyShortageVariable" begin
        @test PSI.should_write_resulting_value(SSI.StorageEnergyShortageVariable) == false
    end

    @testset "Custom add_constraint_dual! overrides are defined" begin
        # ServiceModel dispatch (fixes dual registration for named services)
        @test hasmethod(
            PSI.add_constraint_dual!,
            Tuple{
                PSI.OptimizationContainer,
                PSY.System,
                PSI.ServiceModel{<:PSY.Service, <:PSI.AbstractServiceFormulation},
            },
        )
        # AreaBalancePowerModel dispatch (fixes dual keyed on Area vs ACBus)
        @test hasmethod(
            PSI.add_constraint_dual!,
            Tuple{
                PSI.OptimizationContainer,
                PSY.System,
                PSI.NetworkModel{PSI.AreaBalancePowerModel},
            },
        )
    end
end

# EMIS fixes for PSI bugs are written as methods more specific than PSI's own (replacing a
# PSI method is not allowed during precompilation). Guard that they win dispatch for every
# type EMIS uses, and that PSI's general methods are still there.
@testset "EMIS PSI fixes dispatch as more specific methods" begin
    EMIS = EMISAgentSimulation
    # Every service type EMIS builds a ServiceModel for must hit the EMIS dual fix.
    source = read(joinpath(@__DIR__, "..", "src", "markets_simulation", "siip_simulation_definition.jl"), String)
    used_types = unique(m.captures[1] for m in eachmatch(r"PSI\.ServiceModel\(\s*(PSY\.\w+\{PSY\.\w+\})", source))
    @test !isempty(used_types)
    for type_text in used_types, formulation in (PSI.RangeReserve, PSI.StepwiseCostReserve)
        service_type = Core.eval(EMIS, Meta.parse(type_text))
        @test service_type <: EMIS.EMIS_DUAL_SERVICE_TYPES
        method = which(
            PSI.add_constraint_dual!,
            (PSI.OptimizationContainer, PSY.System, PSI.ServiceModel{service_type, formulation}),
        )
        @test method.module === EMIS
    end
    @test any(
        m -> m.module === PSI && occursin("ServiceModel", string(m.sig)),
        methods(PSI.add_constraint_dual!),
    )
    area_method = which(
        PSI.add_constraint_dual!,
        (PSI.OptimizationContainer, PSY.System, PSI.NetworkModel{PSI.AreaBalancePowerModel}),
    )
    @test area_method.module === EMIS

    for key in (PSI.InitialEnergyLevel, PSI.DeviceStatus), value in (Float64, EMIS.JuMP.VariableRef)
        ics = Vector{Union{PSI.InitialCondition{key, Nothing}, PSI.InitialCondition{key, value}}}
        method = which(PSI.update_initial_conditions!, (ics, PSI.SimulationState, EMIS.Dates.Millisecond))
        @test method.module === EMIS
    end
end
