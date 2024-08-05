using Pkg;
Pkg.activate(".");

using Distributed

#Import Solvers
using JuMP
using GLPK
using Cbc
using Xpress

const Glpk_optimizer = optimizer_with_attributes(GLPK.Optimizer, "msg_lev" => GLPK.OFF, "mip_gap" => 1e-4, "tm_lim" => 100)
const Cbc_optimizer = optimizer_with_attributes(Cbc.Optimizer, "logLevel" => 1, "ratioGap" => 0.5)
const Xpress_optimizer = optimizer_with_attributes(Xpress.Optimizer,
                                                  "MIPRELSTOP" => 1e-2,
                                                  "BARGAPSTOP" => 1e-8,
                                                  "BARDUALSTOP" => 1e-8,
                                                  "BARPRIMALSTOP" => 1e-8,
                                                  "MATRIXTOL" => 1e-8,
                                                  "BARORDER" => 1,
                                                  "OUTPUTLOG" => 0,
                                                  "MAXTIME" => 2400)

using EMISAgentSimulation
using Revise

const EAS = EMISAgentSimulation

simulation_settings = Dict(r["SETTING"] => String(r["VALUE"]) for r in eachrow(EAS.read_data(joinpath(@__DIR__, "simulation_settings.csv"))))
markets_included = Dict(Symbol(r["MARKET"]) => r["INCLUDED"] for r in eachrow(EAS.read_data(joinpath(@__DIR__, "markets_included.csv"))))

vre_reserves_bool = EAS.parsebool(simulation_settings["vre_reserves"])

if vre_reserves_bool
  base_dir = "../EMIS_RTS_Analysis"
  test_system_dir = "../../RTS-GMLC/RTS-GMLC"
else
  base_dir = "../EMIS_RTS_Analysis_No_VRE_Reserves"
  test_system_dir = "../../RTS-GMLC_No_VRE_Reserves/RTS-GMLC"
end



name = simulation_settings["name"]

case = EAS.CaseDefinition(name,
                          base_dir,
                          test_system_dir,
                          Xpress_optimizer,
                          siip_market_clearing = EAS.parsebool(simulation_settings["siip_market_clearing"]),
                          start_year = EAS.parseint(simulation_settings["start_year"]),
                          total_horizon = EAS.parseint(simulation_settings["total_horizon"]),
                          rolling_horizon = EAS.parseint(simulation_settings["rolling_horizon"]),
                          simulation_years = EAS.parseint(simulation_settings["simulation_years"]),
                          num_rep_days = EAS.parseint(simulation_settings["num_rep_days"]),
                          da_resolution = EAS.parseint(simulation_settings["da_resolution"]),
                          rt_resolution = EAS.parseint(simulation_settings["rt_resolution"]),
                          rps_target = simulation_settings["rps_target"],
                          markets = markets_included,
                          ordc_curved = EAS.parsebool(simulation_settings["ordc_curved"]),
                          ordc_unavailability_method = simulation_settings["ordc_unavailability_method"],
                          reserve_penalty = simulation_settings["reserve_penalty"],
                          static_capacity_market = EAS.parsebool(simulation_settings["static_capacity_market"]),
                          irm_scalar = EAS.parsefloat(simulation_settings["irm_scalar"]),
                          derating_scale = EAS.parsefloat(simulation_settings["derating_scale"]),
                          mopr = EAS.parsebool(simulation_settings["mopr"]),
                          battery_cap_mkt = EAS.parsebool(simulation_settings["battery_cap_mkt"]),
                          vre_reserves = vre_reserves_bool,
                          heterogeneity = EAS.parsebool(simulation_settings["heterogeneity"]),
                          forecast_type = simulation_settings["forecast_type"],
                          max_carbon_tax_increase =  EAS.parsefloat(simulation_settings["max_carbon_tax_increase"]),
                          info_symmetry = EAS.parsebool(simulation_settings["info_symmetry"]),
                          belief_update =EAS.parsebool(simulation_settings["belief_update"]),
                          uncertainty = EAS.parsebool(simulation_settings["uncertainty"]),
                          risk_aversion = EAS.parsebool(simulation_settings["risk_aversion"]),
                          parallel_investors = EAS.parsebool(simulation_settings["parallel_investors"]),
                          parallel_scenarios = EAS.parsebool(simulation_settings["parallel_scenarios"])
                        )
simulation = EAS.create_agent_simulation(case)

hpc = false

EAS.create_parallel_workers(case, hpc)

@everywhere begin
    using Pkg; Pkg.activate(".");
    using Xpress
    using EMISAgentSimulation
end

current_siip_sim = Any[]

EAS.run_agent_simulation(simulation, EAS.get_simulation_years(case), current_siip_sim)
