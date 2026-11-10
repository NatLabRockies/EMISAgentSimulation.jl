# One-time migration of investor expected-market data from the old JLD2 format to HDF5.
# Moved out of src/utils/save_load_utils.jl; not part of the package API.
# Usage: julia --project=<run env> scripts/migrate_jld2_to_h5.jl <path/to/investors>
using EMISAgentSimulation
using FileIO
using JLD2

# investor_iteration.jl calls save_expected_market_data
# This code snippet transforms the jld2 to new h5 format
# Note: this is a one-time utility function to convert existing JLD2 files to the new HDF5 format.
# base_path = "/projects/gmlcmarkets/Phase2_EMIS_Analysis/GS_AAYAD/EMIS_RTS_Analysis_GS/20250310_no_sdes_High_RECT_Static_ORDC_RA_Cap_wo_md_storff_High_RPS/investors"

function transform_jld2_to_h5(base_path::String)
    for investor_id in 1:4
        investor_name = "investor$(investor_id)"
        for scenario_name in ["scenario_1", "scenario_2", "scenario_3"]
            for iteration_year in 1:15
                @info "Loading OLD format expected market data for $(investor_name) iteration year $(iteration_year)"
                data_path = joinpath(
                    base_path,
                    investor_name,
                    "expected_market_data",
                    "$(scenario_name)_year_$(iteration_year).jld2",
                )
                expected_data = FileIO.load(data_path)

                capacity_price = expected_data["capacity_price"]
                energy_price = expected_data["energy_price"]
                reserve_price = expected_data["reserve_price"]
                rec_price = expected_data["rec_price"]
                inertia_price = expected_data["inertia_price"]
                capacity_factors = expected_data["capacity_factors"]
                total_utilization = expected_data["total_utilization"]
                capacity_accepted_perc = expected_data["capacity_accepted_perc"]
                new_options = expected_data["new_options"]
                new_options_by_type = expected_data["new_options_by_type"]
                REC_slack = expected_data["REC_slack"]
                REC_supply = expected_data["REC_supply"]
                REC_demand = expected_data["REC_demand"]
                rps_compliant_projects = expected_data["rps_compliant_projects"]
                rec_requirement = expected_data["rec_requirement"]
                investment = expected_data["investment"]
                retirement = expected_data["retirement"]
                max_new_options = expected_data["max_new_options"]
                max_gen = expected_data["max_gen"]
                demand_e = expected_data["demand_e"]
                rep_hour_weight = expected_data["rep_hour_weight"]
                p_e_print = expected_data["p_e_print"]
                in_flow_print = expected_data["in_flow_print"]
                out_flow_print = expected_data["out_flow_print"]
                v_e_print = expected_data["v_e_print"]
                p_in_print = expected_data["p_in_print"]
                linepowerlimit = expected_data["linepowerlimit"]
                rec_correction = expected_data["rec_correction"]
                p_e_storage_print = expected_data["p_e_storage_print"]
                zone_storage = expected_data["zone_storage"]
                zone_projects = expected_data["zone_projects"]
                lines_to_zone = expected_data["lines_to_zone"]
                lines_from_zone = expected_data["lines_from_zone"]
                init_storage = expected_data["init_storage"]
                p_e_detail = expected_data["p_e_detail"]
                remaining_buildtime = expected_data["remaining_buildtime"]
                projects = expected_data["projects"]
                generator_projects = expected_data["generator_projects"]
                storage_projects = expected_data["storage_projects"]
                reserve_up_products = expected_data["reserve_up_products"]
                ordc_products = expected_data["ordc_products"]
                reserve_down_products = expected_data["reserve_down_products"]
                p_in_ru_detail = expected_data["p_in_ru_detail"]
                p_in_ordc_detail = expected_data["p_in_ordc_detail"]
                p_in_inertia_detail = expected_data["p_in_inertia_detail"]
                p_in_rd_detail = expected_data["p_in_rd_detail"]
                p_out_ru_detail = expected_data["p_out_ru_detail"]
                p_out_ordc_detail = expected_data["p_out_ordc_detail"]
                p_out_inertia_detail = expected_data["p_out_inertia_detail"]
                p_out_rd_detail = expected_data["p_out_rd_detail"]

                @info "Saving NEW format expected market data for $(investor_name) iteration year $(iteration_year)"
                h5_data_path = joinpath(
                    base_path,
                    investor_name,
                    "expected_market_data",
                    "$(scenario_name)_year_$(iteration_year).h5",
                )
                EMISAgentSimulation.save_expected_market_data(h5_data_path,
                    capacity_price, energy_price, reserve_price, rec_price, inertia_price,
                    capacity_factors, total_utilization, capacity_accepted_perc,
                    new_options, new_options_by_type,
                    REC_slack, REC_supply, REC_demand,
                    rps_compliant_projects, rec_requirement,
                    investment, retirement,
                    max_new_options, max_gen, demand_e, rep_hour_weight,
                    p_e_print, in_flow_print, out_flow_print, v_e_print, p_in_print,
                    linepowerlimit, rec_correction, p_e_storage_print,
                    zone_storage, zone_projects, lines_to_zone, lines_from_zone,
                    init_storage, p_e_detail, remaining_buildtime,
                    projects, generator_projects, storage_projects,
                    reserve_up_products, ordc_products, reserve_down_products,
                    p_in_ru_detail, p_in_ordc_detail, p_in_inertia_detail, p_in_rd_detail,
                    p_out_ru_detail, p_out_ordc_detail, p_out_inertia_detail,
                    p_out_rd_detail,
                )
            end
        end
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    transform_jld2_to_h5(ARGS[1])
end
