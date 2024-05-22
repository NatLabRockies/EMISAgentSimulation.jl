function prune_system_devices!(sys::PSY.System, prune_dict::Dict{Type{<:PSY.Component}, Array{AbstractString}})
    for (type, device_names) in prune_dict
        for name in device_names
            device = PSY.get_component(type, sys, name)
            PSY.remove_component!(sys, device)
        end
    end
    return
end

"""
Use this function to specify which devices to remove from the RTS at the start of the simulation.
"""
function specify_pruned_units()
    pruned_unit = Dict{Type{<:PSY.Component}, Array{AbstractString}}()
    pruned_unit[PSY.ThermalStandard] = ["115_STEAM_1", "115_STEAM_2", "315_STEAM_1", "315_STEAM_2", "315_STEAM_3", "315_STEAM_4", "315_STEAM_5",
                                         "101_CT_1", "101_CT_2", "102_CT_1", "102_CT_2",
                                         "201_CT_1", "201_CT_2", "202_CT_1", "202_CT_2",
                                         "301_CT_1", "301_CT_2", "302_CT_1", "302_CT_2",
                                         "207_CT_1", "307_CT_1", "101_STEAM_4",
                                         "123_STEAM_3", "223_STEAM_1", "223_STEAM_3"]
    pruned_unit[PSY.RenewableFix] = ["308_RTPV_1", "313_RTPV_1", "313_RTPV_2", "313_RTPV_3", "313_RTPV_4", "313_RTPV_5", "313_RTPV_6", "313_RTPV_7",
                                        "313_RTPV_8", "313_RTPV_9", "313_RTPV_10", "313_RTPV_11", "313_RTPV_12", "320_RTPV_1", "320_RTPV_2", "320_RTPV_3",
                                        "313_RTPV_13", "320_RTPV_4", "320_RTPV_5", "118_RTPV_1", "118_RTPV_2", "118_RTPV_3", "118_RTPV_4", "118_RTPV_5",
                                        "118_RTPV_6", "320_RTPV_6", "118_RTPV_7", "118_RTPV_8", "118_RTPV_9", "118_RTPV_10", "213_RTPV_1"]
    pruned_unit[PSY.RenewableDispatch] = ["309_WIND_1", "212_CSP_1"]
    pruned_unit[PSY.Generator] = ["114_SYNC_COND_1", "314_SYNC_COND_1", "214_SYNC_COND_1"]
    pruned_unit[PSY.GenericBattery] = ["313_STORAGE_1"]

    return pruned_unit
    end


function create_rts_sys(rts_dir::String,
                        base_power::Float64,
                        simulation_dir::String,
                        scratch_dir::String,
                        scenarios::Vector{String},
                        pcm_scenario::String,
                        simulation_years::Int64,
                        da_resolution::Int64,
                        rt_resolution::Int64,
                        MD_horizon::Int64,
                        MD_interval::Int64,
                        UC_horizon::Int64,
                        UC_interval::Int64,
                        ED_horizon::Int64,
                        ED_interval::Int64,)

    # da_products = split(read_data(joinpath(simulation_dir, "markets_data", "reserve_products.csv"))[1,"da_products"], "; ")
    #
    # rts_src_dir = joinpath(rts_dir, "RTS_Data", "SourceData")
    # rts_siip_dir = joinpath(rts_dir, "RTS_Data", "FormattedData", "SIIP");
    #
    # rawsys = PSY.PowerSystemTableData(
    #         rts_src_dir,
    #         base_power,
    #         joinpath(rts_siip_dir, "user_descriptors.yaml"),
    #         timeseries_metadata_file = joinpath(rts_siip_dir, "timeseries_pointers.json"),
    #         );
    #
    # sys_UC = PSY.System(rawsys; time_series_resolution = Dates.Minute(da_resolution));
    #
    # services_UC = get_system_services(sys_UC)
    #
    # for service in services_UC
    #     if !(PSY.get_name(service) in da_products)
    #         PSY.remove_component!(sys_UC, service)
    #     end
    # end
    #
    # rt_products = split(read_data(joinpath(simulation_dir, "markets_data", "reserve_products.csv"))[1,"rt_products"], "; ")
    #
    # sys_ED = PSY.System(rawsys; time_series_resolution = Dates.Minute(rt_resolution));
    #
    # services_ED = get_system_services(sys_ED)
    #
    # for service in services_ED
    #     if !(PSY.get_name(service) in rt_products)
    #         PSY.remove_component!(sys_ED, service)
    #     end
    # end
    #
    # pruned_unit = specify_pruned_units()
    # prune_system_devices!(sys_UC, pruned_unit)
    # prune_system_devices!(sys_ED, pruned_unit)

    ntp_ts_data_dir = joinpath(rts_dir, "..", "NTP_TimeSeries_Data")

    sys_MDs = Vector{PSY.System}()
    sys_UCs = Vector{PSY.System}()
    sys_EDs = Vector{PSY.System}()
    sys_EDs_dict = Dict(scenario => Vector{PSY.System}() for scenario in scenarios)

    for sim_year in 1:simulation_years
        println(sim_year)

        MD_sys_filename = joinpath(rts_dir, "constructed_systems", pcm_scenario, "sim_year_$(sim_year)", "MD_sys_EMIS_$(MD_horizon)hor_$(MD_interval)int.json")
        MD_num_forecast_filename = joinpath(rts_dir, "constructed_systems", pcm_scenario, "sim_year_$(sim_year)", "MD_num_forecast_$(MD_horizon)hor_$(MD_interval)int.txt")
        if !(isfile(MD_sys_filename) && isfile(MD_num_forecast_filename))
            println("MD json file doesn't exist. Creating required data.")   
            dir_exists(dirname(MD_sys_filename))         
            sys_MD_initial = PSY.System(joinpath(rts_dir,"DA_sys_EMIS_w2011_2hrRT_with_outage_PSY3.json"), time_series_directory = scratch_dir);
            # create MD system
            create_sys_w_updated_ts(
                ntp_ts_data_dir,
                sys_MD_initial,
                2011,
                2021,
                "dayahead",
                "baseline",
                75.0, #GW
                MD_horizon, # hours
                MD_interval, # hours
                MD_sys_filename,
                true,
                nothing,
                nothing,
                MD_num_forecast_filename,
            )
        end        
        push!(sys_MDs, PSY.System(MD_sys_filename, time_series_directory = scratch_dir));


        UC_filename = joinpath(rts_dir, "constructed_systems", pcm_scenario, "sim_year_$(sim_year)", "DA_sys_EMIS_$(UC_horizon)hor_$(UC_interval)int_$(MD_horizon)mdhor_$(MD_interval)mdint.json")
        if !(isfile(UC_filename))
            println("UC json file doesn't exist. Creating required json file.")  
            dir_exists(dirname(UC_filename))   
            sys_UC_initial = PSY.System(joinpath(rts_dir,"DA_sys_EMIS_w2011_2hrRT_with_outage_PSY3.json"), time_series_directory = scratch_dir);
            create_sys_w_updated_ts(
                ntp_ts_data_dir,
                sys_UC_initial,
                2011,
                2021,
                "dayahead",
                "baseline",
                75.0, #GW
                UC_horizon, # hours
                UC_interval, # hours
                UC_filename,
                false,
                MD_horizon,
                MD_interval,
                MD_num_forecast_filename,
            )
        end
        push!(sys_UCs, PSY.System(UC_filename, time_series_directory = scratch_dir));

        for scenario in scenarios
            ED_filename = joinpath(rts_dir, "constructed_systems", scenario, "sim_year_$(sim_year)", "RT_sys_EMIS_$(ED_horizon)hor_$(ED_interval)int_$(MD_horizon)mdhor_$(MD_interval)mdint.json")
            if !isfile(ED_filename)
                println("ED json file doesn't exist. Creating required json file.")  
                dir_exists(dirname(ED_filename))   
                sys_ED_initial = PSY.System(joinpath(rts_dir,"RT_sys_EMIS_w2011_2hrRT_with_outage_PSY3.json"), time_series_directory = scratch_dir);
                create_sys_w_updated_ts(
                    ntp_ts_data_dir,
                    sys_ED_initial,
                    2011,
                    2021,
                    "realtime",
                    "baseline",
                    75.0, #GW
                    ED_horizon, # hours
                    ED_interval, # hours
                    ED_filename,
                    false,
                    MD_horizon,
                    MD_interval,
                    MD_num_forecast_filename,
                )
            end
            push!(sys_EDs_dict[scenario], PSY.System(ED_filename, time_series_directory = scratch_dir));
        end
        sys_EDs = sys_EDs_dict[pcm_scenario]

        removegen_name = ["AUSTIN_1","AUSTIN_2"]
        for sys in [sys_MDs[sim_year], sys_UCs[sim_year], sys_EDs[sim_year]]
            for d in PSY.get_components(PSY.Generator, sys)
                if d.name in removegen_name
                    PSY.remove_component!(sys, d)
                end
            end
        end
    
        for sys in [sys_MDs[sim_year], sys_UCs[sim_year], sys_EDs[sim_year]]
            d= PSY.get_component(PSY.VariableReserve,sys,"SPIN")
            PSY.remove_component!(sys,d)
        end
    
        for sys in [sys_MDs[sim_year], sys_UCs[sim_year], sys_EDs[sim_year]]
            d= PSY.get_component(PSY.VariableReserveNonSpinning,sys,"NONSPIN")
            PSY.remove_component!(sys,d)
        end
    
        for sys in [sys_MDs[sim_year], sys_UCs[sim_year], sys_EDs[sim_year]]
            d= PSY.get_component(PSY.VariableReserve,sys,"REG_DN")
            PSY.set_name!(sys,d,"Reg_Down")
        end
    
        for sys in [sys_MDs[sim_year], sys_UCs[sim_year], sys_EDs[sim_year]]
            d= PSY.get_component(PSY.VariableReserve,sys,"REG_UP")
            PSY.set_name!(sys,d,"Reg_Up")
        end
    
        PSY.set_units_base_system!(sys_MDs[sim_year], PSY.IS.UnitSystem.DEVICE_BASE)
        PSY.set_units_base_system!(sys_UCs[sim_year], PSY.IS.UnitSystem.DEVICE_BASE)
        PSY.set_units_base_system!(sys_EDs[sim_year], PSY.IS.UnitSystem.DEVICE_BASE)
               
    end

    sys_PRAS = Dict{String, PSY.System}()

    for scenario in scenarios
        PRAS_filename = joinpath(rts_dir, "constructed_systems", scenario, "sim_year_1", "PRAS_sys_EMIS_$(ED_horizon)hor_$(ED_interval)int_$(MD_horizon)mdhor_$(MD_interval)mdint.json")
        if !isfile(PRAS_filename)
            println("PRAS json file doesn't exist. Creating required json file.")  
            create_PRAS_sys_json(sys_EDs_dict[scenario], PRAS_filename)
        end
        sys_PRAS[scenario] = PSY.System(PRAS_filename, time_series_directory = scratch_dir)
    end
    
    return sys_MDs, sys_UCs, sys_EDs, sys_PRAS, MD_horizon, MD_interval, UC_horizon, UC_interval, ED_horizon, ED_interval
end

function remove_vre_gens!(sys::PSY.System)
    for gen in get_all_techs(sys)
        if typeof(gen) == PSY.RenewableDispatch
            #println(PSY.get_name(gen))
            #println(PSY.get_ext(gen))
            PSY.remove_component!(sys, gen)
        end
    end
end


function create_sys_w_updated_ts(
    data_dir::String,
    initial_sys::PSY.System,
    weatheryear::Int64,
    loadyear::Int64,
    market_stage::String, # dayahead, realtime
    scenario::String,
    loadscaler_base::Float64, #GW
    horizon::Int64, # hours
    interval::Int64, # hours
    output_file::String,
    first_stage::Bool=false,
    first_stage_horizon::Union{Nothing, Integer}=nothing, # hour (only need input if first_stage is false)
    first_stage_interval::Union{Nothing, Integer}=nothing, # hour (only need input if first_stage is false)
    first_stage_number_of_forecast_filename::Union{Nothing, String}=nothing, # (only need input if first_stage is false)
)

    #--------------------------------------------
    # Calculate load scaling factor: scale 2021 load to 75 GW
    #--------------------------------------------
    loadscaler_profile=DataFrame(CSV.File(joinpath(data_dir, "Load_Actuals_tzcorrect", "20221010_ercot_regional_load_$(scenario)_e2021_w$(weatheryear)_cst.csv"))) #in GW
    loadscaler_peak=maximum(sum(eachcol(select(loadscaler_profile, Not([:year, :timestamp])))))
    loadscaler = loadscaler_peak/loadscaler_base

    sys_MD = initial_sys
    PSY.set_units_base_system!(sys_MD, PSY.IS.UnitSystem.NATURAL_UNITS)

    # Remove the generator that availability is false
    # for sys in [sys_MD]
    # 	d=PSY.get_component(Generator, sys, "Glove Solar")
    # 	PSY.remove_component!(sys, d)
    # end
    # 
    # PSY.get_available(PSY.get_component(Generator, sys_DA, "Glove Solar"))
    # PSY.get_components(PSY.Generator, sys_DA, x -> x.available == false)

    #-----------------------------------------------------------------------------------
    # Construct hydro timeseries with new horizon from existing Deterministic timeseries
    #-----------------------------------------------------------------------------------
    first_ts_temp_MD = first(PSY.get_time_series_multiple(sys_MD))
    start_datetime_MD = PSY.IS.get_initial_timestamp(first_ts_temp_MD)
    sys_MD_res = PSY.get_time_series_resolution(sys_MD)
    sys_MD_initial_interval = Int(sys_MD.data.time_series_params.forecast_params.interval / sys_MD_res)

    # if not the first stage, i think finish_datetime_MD needs to be first stage's number_of_forecast * interval + (horizon - interval) -- all from first stage (need to pass down these parameters)
    if first_stage == true
        finish_datetime_MD = start_datetime_MD + Dates.Hour(8759*sys_MD_res)
    else
        first_stage_number_of_forecast = 0
        open(first_stage_number_of_forecast_filename, "r") do file
            first_stage_number_of_forecast = parse(Int, readline(file))
        end
        finish_datetime_MD = start_datetime_MD + Dates.Hour((first_stage_number_of_forecast * first_stage_interval + (first_stage_horizon - first_stage_interval) - 1) *sys_MD_res)
    end
    # timestep here indicate how many MD periods are being constructed
    timestep = StepRange(start_datetime_MD, sys_MD_res*interval, finish_datetime_MD);
    first_stage_total = Int((finish_datetime_MD - start_datetime_MD) / sys_MD_res + 1)
    additional_timestep = Int(horizon - (first_stage_total-(interval*(length(timestep)-1))) + (first_stage_total - 8760))

    for component in collect(get_components(HydroDispatch, sys_MD))
        forecast = get_time_series(Deterministic, component, "max_active_power")

        reconstruct_single_ts = Float64[]
        for (key, value) in forecast.data
            append!(reconstruct_single_ts, value[1:sys_MD_initial_interval])
        end
        append!(reconstruct_single_ts, reconstruct_single_ts[1:additional_timestep])

        dates = range(DateTime("2018-01-01T00:00:00"), step = sys_MD_res, length = 8760 + additional_timestep)
        data = TS.TimeArray(dates, reconstruct_single_ts)
        single_time_series = SingleTimeSeries("max_active_power", data)

        add_time_series!(sys_MD, component, single_time_series)
    end

    remove_time_series!(sys_MD, Deterministic)

    for component in collect(get_components(HydroDispatch, sys_MD))

        revisedts = DataStructures.SortedDict{DateTime,Vector}()
        newtsdata = values(get_time_series(SingleTimeSeries, component, "max_active_power").data)

        for t in 1:length(timestep)
            rtseries=[]
            datetimeindex = timestep[t]
            rtseries = newtsdata[(interval*(t-1)+1):(interval*(t-1)+horizon)]
            push!(revisedts, datetimeindex => rtseries)
        end

        # conver to deterministic time series
        revisedts_deterministic = PSY.Deterministic(;
            name="max_active_power",
            data=revisedts,
            resolution=Dates.Hour(1),
            scaling_factor_multiplier=get_max_active_power
        )

        # remove old time series
        # remove_time_series!(sys_MD, Deterministic, d, "max_active_power")
        # add new time series to dataset
        add_time_series!(sys_MD, component, revisedts_deterministic)
    end

    remove_time_series!(sys_MD, SingleTimeSeries)

    #-----------------------------------------------------------
    # Replacing wind & solar time series  !! In NATURAL_UNITS !!
    #-----------------------------------------------------------
    namemapping = DataFrame(CSV.File(joinpath(data_dir, "GeneratorNameMapping.csv")))
    # To get raw DA data time stamps
    # first_ts_temp_MD = first(PSY.get_time_series_multiple(sys_MD))
    # start_datetime_MD = PSY.IS.get_initial_timestamp(first_ts_temp_MD);
    # sys_MD_res = PSY.get_time_series_resolution(sys_MD)
    # finish_datetime_MD = start_datetime_MD + Dates.Hour(8759*sys_MD_res);
    # timestep here indicate how many MD periods are being constructed
    # timestep = StepRange(start_datetime_MD, sys_MD_res*interval, finish_datetime_MD);
    # hourlytimestep  = StepRange(start_datetime_DA, sys_DA_res, finish_datetime_DA);

    # remove_time_series!(sys_MD, Deterministic)

    for d in get_components(x -> get_prime_mover_type(x) in [PrimeMovers.PVe, PrimeMovers.WT], Generator, sys_MD)
        # println("Processing generator: $(get_name(d))")
        #create dictionary
        # revisedts = Dict{DateTime, Array{Float64}}()
        revisedts = DataStructures.SortedDict{DateTime,Vector}()

        # tstype = "1dayahead" ## actuals/, 1dayahead/, intraday/
        if market_stage == "dayahead"
            tstype = "1dayahead"
        elseif market_stage == "realtime"
            # TODO: the real-time timeseries actually uses both "actuals" and "intraday", but it limits the RT horizon to 2-hour.
            tstype = "actuals"
        end

        if get_prime_mover_type(d) == PrimeMovers.WT
            technology = "wind" 
        else
            technology = "pv" 
        end

        profile = DataFrame(CSV.File(joinpath(data_dir, "Wind_PV_Profiles_tzcorrected", "$(tstype)/ercot_$(technology)_build-$(weatheryear)_cst.csv")))
        newtsdata = profile[!, namemapping[in([PSY.get_name(d)]).(namemapping.jsonname), :csvname][1]]
        basepower = get_rating(d)
        newtsdata = newtsdata ./ basepower

        for t in 1:length(timestep)
            rtseries=[]
            datetimeindex = timestep[t]
            if t < 8760/interval
                rtseries = newtsdata[(interval*(t-1)+1):(interval*(t-1)+horizon)]
            elseif t == ceil(Int, 8760/interval)
                rtseries = [newtsdata[(interval*(t-1)+1):8760];newtsdata[1:horizon-(8760-(interval*(t-1)))]]
            else
                # simply use the end of the previous timeseries as the new start
                new_start = horizon-(8760-(interval*(ceil(Int, 8760/interval)-1)))
                rtseries = newtsdata[new_start+(t-ceil(Int, 8760/interval)-1)*interval+1:new_start+(t-ceil(Int, 8760/interval)-1)*interval+horizon]
            end
            push!(revisedts, datetimeindex => rtseries)
        end

        # conver to deterministic time series
        revisedts_deterministic = PSY.Deterministic(;
            name="max_active_power",
            data=revisedts,
            resolution=Dates.Hour(1),
            scaling_factor_multiplier=get_max_active_power
        )

        # remove old time series
        # remove_time_series!(sys_MD, Deterministic, d, "max_active_power")
        # add new time series to dataset
        add_time_series!(sys_MD, d, revisedts_deterministic)
    end

    #--------------------------------------------
    # Load Forecasts !!!!!!!!!!!!! CHECK SYSTEM BASE !!!!!!!!!
    #--------------------------------------------
    if market_stage == "dayahead"
        profile=DataFrame(CSV.File(joinpath(data_dir, "Load_Forecast_from_Local", "$(scenario)", "$(loadyear)", "preds_$(weatheryear)0101_365days.csv"))) #in GW
    elseif market_stage == "realtime"
        profile=DataFrame(CSV.File(joinpath(data_dir, "Load_Actuals_tzcorrect", "20221010_ercot_regional_load_$(scenario)_e$(loadyear)_w$(weatheryear)_cst.csv"))) #in GW
    end

    for d in get_components(PowerLoad, sys_MD)
        # println("Processing region: $(get_name(get_bus(d)))")
        #create dictionary
        # revisedts = Dict{DateTime, Array{Float64}}()
        revisedts = DataStructures.SortedDict{DateTime,Vector}()

        newtsdata = profile[!,lowercase(PSY.get_name(PSY.get_bus(d)))] 
        baseload = get_max_active_power(d)
        newtsdata = newtsdata./baseload
        newtsdata = newtsdata.*1000
        newtsdata = newtsdata./loadscaler # scale 2021 to 75GW peak

        for t in 1:length(timestep)
            rtseries=[]
            datetimeindex = timestep[t]
            if t < 8760/interval
                rtseries = newtsdata[(interval*(t-1)+1):(interval*(t-1)+horizon)]
            elseif t == ceil(Int, 8760/interval)
                rtseries = [newtsdata[(interval*(t-1)+1):8760];newtsdata[1:horizon-(8760-(interval*(t-1)))]]
            else
                # simply use the end of the previous timeseries as the new start
                new_start = horizon-(8760-(interval*(ceil(Int, 8760/interval)-1)))
                rtseries = newtsdata[new_start+(t-ceil(Int, 8760/interval)-1)*interval+1:new_start+(t-ceil(Int, 8760/interval)-1)*interval+horizon]
            end
            push!(revisedts, datetimeindex => rtseries)
        end

        # conver to deterministic time series
        revisedts_deterministic = PSY.Deterministic(;
            name="max_active_power",
            data=revisedts,
            resolution=Dates.Hour(1),
            scaling_factor_multiplier=get_max_active_power
        )

        # remove old time series
        # remove_time_series!(sys_DA, Deterministic, d, "max_active_power")
        # add new time series to dataset
        add_time_series!(sys_MD, d, revisedts_deterministic)
    end

    #-----------------------------------------------------------------
    # Regulation time series update
    #-----------------------------------------------------------------
    regdown_ts = DataFrame(CSV.File(joinpath(data_dir, "TS_for_Regulation_Req_Calc", "RegulationTS_Bethany", "DA_regDown_baseline_gmlc$(weatheryear).csv"))) #in MW
    regup_ts = DataFrame(CSV.File(joinpath(data_dir, "TS_for_Regulation_Req_Calc", "RegulationTS_Bethany", "DA_regUp_baseline_gmlc$(weatheryear).csv"))) #in MW
    regdown_ts= select!(regdown_ts, Not(:DATETIME))
    regup_ts= select!(regup_ts, Not(:DATETIME))
    regdown_ts[!,"RegDown"] = sum(eachcol(regdown_ts))
    regup_ts[!,"RegUp"] = sum(eachcol(regup_ts))
    reg_profile = DataFrame(REG_DN = regdown_ts[!,"RegDown"], REG_UP = regup_ts[!,"RegUp"])

    for d in get_components(x -> PSY.get_name(x) in ["REG_DN","REG_UP"], Service, sys_MD)
        # println("Processing serve: $(get_name(d))")
        #create dictionary
        # revisedts = Dict{DateTime, Array{Float64}}()
        revisedts = DataStructures.SortedDict{DateTime,Vector}()

        newtsdata = reg_profile[!,PSY.get_name(d)] 
        basereq = get_requirement(d)
        newtsdata = newtsdata./basereq

        for t in 1:length(timestep)
            rtseries=[]
            datetimeindex = timestep[t]
            if t < 8760/interval
                rtseries = newtsdata[(interval*(t-1)+1):(interval*(t-1)+horizon)]
            elseif t == ceil(Int, 8760/interval)
                rtseries = [newtsdata[(interval*(t-1)+1):8760];newtsdata[1:horizon-(8760-(interval*(t-1)))]]
            else
                # simply use the end of the previous timeseries as the new start
                new_start = horizon-(8760-(interval*(ceil(Int, 8760/interval)-1)))
                rtseries = newtsdata[new_start+(t-ceil(Int, 8760/interval)-1)*interval+1:new_start+(t-ceil(Int, 8760/interval)-1)*interval+horizon]
            end
            push!(revisedts, datetimeindex => rtseries)
        end

        # conver to deterministic time series
        revisedts_deterministic = PSY.Deterministic(;
            name="requirement",
            data=revisedts,
            resolution=Dates.Hour(1),
            scaling_factor_multiplier=get_requirement
        )

        # remove old time series
        # remove_time_series!(sys_MD, Deterministic, d, "requirement")
        # add new time series to dataset
        add_time_series!(sys_MD, d, revisedts_deterministic)
    end

    # TODO: add outage timeseries
    #--------------------------------------------
    # Add thermal outage time series
    # see /lustre/eaglefs/projects/gmlcmarkets/Phase2_EMIS_Analysis/ERCOT_Data_Prep/20221214_outage_example/
    #--------------------------------------------
    # outage_csv_location = "/lustre/eaglefs/projects/gmlcmarkets/PowerSystems2PRAS.jl/data/Generated-Outage-Profile-JSON/04371469-18e0-421c-b4e4-44a0f1c1213f/16-Jan-22-14-6-22/"
    # sys_DA = System(joinpath(data_dir,"DA_sys_EMIS_v0811.json"), time_series_directory = "/tmp/scratch") #365*36
    # sys_RT = System(joinpath(data_dir,"RT_sys_EMIS_v0811.json"), time_series_directory = "/tmp/scratch") #8760*24

    # outage_csv_location = "/home/ysun/gmlcmarkets/Phase2_EMIS_Analysis/ERCOT_Data_Prep/20221214_outage_example/"
    # outagescenario = 1

    # sys_DA, sys_RT = add_csv_time_series!(sys_DA,sys_RT,outage_csv_location,add_scenario = outagescenario); # align outage structure to be the same as PRAS-ED (same value persist 36 times)

    PSY.set_units_base_system!(sys_MD, PSY.IS.UnitSystem.SYSTEM_BASE)
    to_json(sys_MD, output_file, force=true)

    number_of_forecast = sys_MD.data.time_series_params.forecast_params.count
    if first_stage
        open(first_stage_number_of_forecast_filename, "w") do file
            write(first_stage_number_of_forecast_filename, string(number_of_forecast))
        end
    end

    return

end


function create_PRAS_sys_json(
    sys_EDs::Vector{PSY.System},
    output_file::String
)
    sys_PRAS = deepcopy(first(sys_EDs))

    first_ts_temp_PRAS = first(PSY.get_time_series_multiple(sys_PRAS))
    start_datetime_PRAS = PSY.IS.get_initial_timestamp(first_ts_temp_PRAS)
    sys_PRAS_res = PSY.get_time_series_resolution(sys_PRAS)
    sys_PRAS_interval = Int(sys_PRAS.data.time_series_params.forecast_params.interval / sys_PRAS_res)
    sys_PRAS_horizon = sys_PRAS.data.time_series_params.forecast_params.horizon
    finish_datetime_PRAS = start_datetime_PRAS + Dates.Hour(8760 * sys_PRAS_res * length(sys_EDs) - sys_PRAS_res)
    
    timestep = StepRange(start_datetime_PRAS, sys_PRAS_res * sys_PRAS_interval, finish_datetime_PRAS);

    ts_objects = Dict{String, Any}()

    ts_objects["gens"] = collect(PSY.get_components(x -> PSY.get_prime_mover_type(x) in [PrimeMovers.PVe, PrimeMovers.WT, PrimeMovers.HY], Generator, sys_PRAS))
    ts_objects["loads"] = collect(PSY.get_components(PowerLoad, sys_PRAS))

    for (key, devices) in ts_objects
        for component_PRAS in devices
            name = PSY.get_name(component_PRAS)
            reconstruct_single_ts = Float64[]
            for sys in sys_EDs
                component = first(PSY.get_components_by_name(PSY.Device, sys, name))
                forecast = PSY.get_time_series(Deterministic, component, "max_active_power")
                weather_year = Dates.year(first(keys(forecast.data)))
                for (key, value) in forecast.data
                    if Dates.year(key) == weather_year
                        append!(reconstruct_single_ts, value[1:sys_PRAS_interval])
                    end
                end
            end
            append!(reconstruct_single_ts, reconstruct_single_ts[1:sys_PRAS_horizon - 1])
            dates = range(DateTime("2018-01-01T00:00:00"), step = sys_PRAS_res, length = length(timestep) + sys_PRAS_horizon - 1)
            data = TS.TimeArray(dates, reconstruct_single_ts)
            single_time_series = SingleTimeSeries("max_active_power", data)
            add_time_series!(sys_PRAS, component_PRAS, single_time_series)
        end
    end
    
    services = collect(PSY.get_components(x -> PSY.get_name(x) in ["Reg_Down","Reg_Up"], Service, sys_PRAS))
    for service_PRAS in services
        name = PSY.get_name(service_PRAS)
        reconstruct_single_ts = Float64[]
        for sys in sys_EDs
            service = first(PSY.get_components_by_name(PSY.Service, sys, name))
            forecast = PSY.get_time_series(Deterministic, service, "requirement")
            weather_year = Dates.year(first(keys(forecast.data)))
            for (key, value) in forecast.data
                if Dates.year(key) == weather_year
                    append!(reconstruct_single_ts, value[1:sys_PRAS_interval])
                end
            end
        end
        append!(reconstruct_single_ts, reconstruct_single_ts[1:sys_PRAS_horizon - 1])
        dates = range(DateTime("2018-01-01T00:00:00"), step = sys_PRAS_res, length = length(timestep) + sys_PRAS_horizon - 1)
        data = TS.TimeArray(dates, reconstruct_single_ts)
        single_time_series = SingleTimeSeries("requirement", data)
        add_time_series!(sys_PRAS, service_PRAS, single_time_series)
    end
    remove_time_series!(sys_PRAS, Deterministic)


    for (key, devices) in ts_objects
        for component in devices
            revisedts = DataStructures.SortedDict{DateTime,Vector}()
            newtsdata = values(get_time_series(SingleTimeSeries, component, "max_active_power").data)
    
            for t in 1:length(timestep)
                rtseries=[]
                datetimeindex = timestep[t]
                rtseries = newtsdata[(sys_PRAS_interval * (t - 1) + 1):(sys_PRAS_interval * ( t - 1) + sys_PRAS_horizon)]
                push!(revisedts, datetimeindex => rtseries)
            end
    
            # conver to deterministic time series
            revisedts_deterministic = PSY.Deterministic(;
                name="max_active_power",
                data=revisedts,
                resolution=Dates.Hour(1),
                scaling_factor_multiplier=get_max_active_power
            )
    
            add_time_series!(sys_PRAS, component, revisedts_deterministic)
        end
    end

    for service in services
        revisedts = DataStructures.SortedDict{DateTime,Vector}()
        newtsdata = values(get_time_series(SingleTimeSeries, service, "requirement").data)

        for t in 1:length(timestep)
            rtseries=[]
            datetimeindex = timestep[t]
            rtseries = newtsdata[(sys_PRAS_interval * (t - 1) + 1):(sys_PRAS_interval * ( t - 1) + sys_PRAS_horizon)]
            push!(revisedts, datetimeindex => rtseries)
        end

        # conver to deterministic time series
        revisedts_deterministic = PSY.Deterministic(;
            name="requirement",
            data=revisedts,
            resolution=Dates.Hour(1),
            scaling_factor_multiplier=get_requirement
        )

        add_time_series!(sys_PRAS, service, revisedts_deterministic)
    end

    remove_time_series!(sys_PRAS, SingleTimeSeries)
    
    to_json(sys_PRAS, output_file, force=true)

    return
end