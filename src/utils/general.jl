
"""
This function returns all the subtypes at the leaf nodes (i.e. which do not have any further subtypes) of a given type.
"""
function leaftypes(t::Any, level = 1, leaves = Vector{Any}())
    for s in InteractiveUtils.subtypes(t)
        if length(InteractiveUtils.subtypes(s)) == 0
            push!(leaves, s)
        else
            leaftypes(s, level+1, leaves)
        end
    end
    return leaves
end

"""
This function gets the capacity market forward years for the simulation.
"""
function get_capacity_forward_years(sim::Union{AgentSimulation, AgentSimulationData})
    forward_years = 1
    if get_markets(sim)[:Capacity]
        capacity_mkt_params = DataFrames.DataFrame(
            CSV.File(joinpath(get_data_dir(get_case(sim)), "markets_data", "Capacity.csv")),
        )
        forward_years = capacity_mkt_params.forward_years[1]
    end
    return forward_years
end

"""
This function returns the size of the project (in MW)
based on the project type and size type.
"""
function size_in_MW(investor_dir::String,
    projecttype::String,
    sizetype::String)
    filename = joinpath(investor_dir, "sizedict.csv")
    sizedata = read_data(filename)

    size = sizedata[findfirst(x->x == projecttype, sizedata.type),
        findfirst(x -> x == sizetype, names(sizedata))]
    return size
end

"""
This function returns the size of the project (in MW)
if already specified in MW.
"""
function size_in_MW(investor_dir::String,
    projecttype::String,
    size::Float64)
    return size
end

"""
This function returns the size of the PSY Device.
"""
get_device_size(device::T) where {T <: PSY.ThermalGen} =
    PSY.get_active_power_limits(device)[:max]
get_device_size(device::PSY.RenewableDispatch) = PSY.get_rating(device)
get_device_size(device::Union{PSY.HydroTurbine, PSY.HydroDispatch}) =
    PSY.get_active_power_limits(device)[:max]
get_device_size(device::PSY.EnergyReservoirStorage) = PSY.get_rating(device)

"""
This function returns line rating for PSY Lines.
"""
get_line_rating(line::PSY.Line) = PSY.get_rating(line)
get_line_rating(line::PSY.TwoTerminalGenericHVDCLine) = line.active_power_limits_from[:max]

"""
This function removes leap days from the data if year is not leap year.
"""
function remove_leap_day!(df::DataFrames.DataFrame, start_year::Int64)
    #=
    if start_year % 4 != 0
        filter!(row -> row["Month"] != 2 || row["Day"] != 29, df)
    end
    =#
    #filter!(row -> row["Month"] != 2 || row["Day"] != 29, df)

    return
end

"""
This function finds the start and end years
for reading price data.
"""
function find_price_years(construction_year::Int64,
    end_life_year::Int64,
    iteration_year::Int64,
    yearly_horizon::Int64)
    price_years = (start_year = max((construction_year -
                       iteration_year + 1),
            1),
        end_year = min((end_life_year -
                        iteration_year + 1),
            (yearly_horizon)))

    return price_years
end

"""
This function finds the start and end years
for updating project profit and npv data.
"""
function find_update_years(construction_year::Int64,
    end_life_year::Int64,
    iteration_year::Int64,
    yearly_horizon::Int64)
    update_years = (start_year = max(construction_year,
            iteration_year),
        end_year = min(end_life_year,
            yearly_horizon + iteration_year - 1))

    return update_years
end

"""
This function returns the number of hours modeled in the market clearing problem.
"""
function get_num_hours(system::MarketClearingProblem{Z, T}) where {Z, T}
    num_hours = T

    return num_hours
end

"""
This function returns an O&M cost array filled with the fixed
annual O&M cost values from update year to the end life year.
"""
function create_OMcost_array(finance::Finance,
    scenario_name::String,
    project_life_end::Int64,
    update_start_year::Int64,
    iteration_year::Int64)
    OM_cost_array =
        zeros(size(get_scenario_profit(finance)[scenario_name][iteration_year], 2))

    for i in update_start_year:project_life_end
        OM_cost_array[i] = get_fixed_OM_cost(finance)
    end

    return OM_cost_array
end

"""
This function return a queue cost array filled with the queue cost values
corresponding to each year spent in the queue.
"""
function create_queue_cost_array(size_profit::Int64,
    decision_year::Int64,
    queue_time::Int64,
    queue_cost::Vector{Float64})
    queue_cost_array = zeros(size_profit)

    for i in decision_year:(decision_year + queue_time - 1)
        queue_cost_array[i] = queue_cost[i - decision_year + 1]
    end

    return queue_cost_array
end
"""
This function does nothing if the project is not an Option project.
"""
function update_lifecycle!(project::P,
    iteration_year::Int64,
    step_size::Int64,
    simulation_years::Int64) where {P <: Project{<: BuildPhase}}
    return
end

"""
This function updates the decision, construction and endlife years of Option projects.
"""
function update_lifecycle!(project::P,
    iteration_year::Int64,
    step_size::Int64,
    simulation_years::Int64) where {P <: Project{Option}}
    finance_data = get_finance_data(project)
    queue_time = length(get_queue_cost(finance_data))

    if queue_time + iteration_year < simulation_years
        set_decision_year!(project, (iteration_year + step_size))
        set_construction_year!(
            project,
            (get_decision_year(project) +
             queue_time +
             get_lag_time(finance_data)),
        )

        end_life_year = get_construction_year(project) + get_life_time(finance_data) - 1
        set_end_life_year!(project, end_life_year)

        @info "Updated lifecycle of project $(get_name(project)) to
              decision year: $(get_decision_year(project)),
              construction year: $(get_construction_year(project)),
             end life year: $(get_end_life_year(project)), 
             queue time: $(queue_time), lag time: $(get_lag_time(finance_data)),
             life time: $(get_life_time(finance_data))"

        set_effective_investment_cost!(
            project,
            get_investment_cost(finance_data)[queue_time + iteration_year + step_size],
        )

        products = get_products(project)
        finance_data.realized_profit =
            AxisArrays.AxisArray(zeros(length(products), end_life_year),
                AxisArrays.Axis{:prod}(get_name.(products)),
                AxisArrays.Axis{:year}(1:1:end_life_year))

        finance_data.annual_cashflow = zeros(end_life_year)

        scenario_profit = get_scenario_profit(finance_data)

        for scenario_name in keys(scenario_profit)
            rows = size(scenario_profit[scenario_name][iteration_year], 1)
            cols = get_end_life_year(project)

            set_expected_npv!(finance_data, zeros(cols))
            set_expected_utility!(finance_data, zeros(cols))

            finance_data.scenario_profit[scenario_name] = [
                AxisArrays.AxisArray(zeros(length(products), cols),
                    AxisArrays.Axis{:prod}(get_name.(products)),
                    AxisArrays.Axis{:year}(collect(1:1:cols)))
                for y in 1:cols
            ]
            set_scenario_npv!(finance_data, scenario_name, zeros(cols))
            set_scenario_utility!(finance_data, scenario_name, zeros(cols))
        end
    end

    return
end

"""
This function updates the total installed capacity each year.
"""
function update_installed_cap!(installed_capacity::Vector{Float64},
    announced_projects::Vector{<: Project{<: BuildPhase}},
    iteration_year::Int64,
    simulation_years::Int64)
    for year in iteration_year:simulation_years
        installed_capacity[year] = 0.0
        for project in announced_projects
            if (get_construction_year(project) <= year) &&
               (get_end_life_year(project) >= year)
                installed_capacity[year] += get_maxcap(project)
            end
        end
    end
    return installed_capacity
end

"""
This function does nothing if project is not in Existing phase.
"""
function extrapolate_profits!(project::Project{<:BuildPhase}, final_year::Int64)
    return
end

"""
This function extrapolates the profits of Existing projects beyond the simulation horizon.
"""
function extrapolate_profits!(project::Project{Existing}, final_year::Int64)
    end_life_year = get_end_life_year(project)
    finance_data = get_finance_data(project)
    if end_life_year > final_year
        for y in (final_year + 1):end_life_year
            set_annual_cashflow!(
                finance_data,
                y,
                get_annual_cashflow(finance_data)[final_year],
            )
            for product in get_products(project)
                name = get_name(product)
                set_realized_profit!(
                    finance_data,
                    name,
                    y,
                    get_realized_profit(finance_data)[name, final_year],
                )
            end
        end
    end
    return
end

function month_lookup(str::Union{String, SubString{String}})
    if str == "Jan" || str == "January"
        month = 1
    elseif str == "Feb" || str == "February"
        month = 2
    elseif str == "Mar" || str == "March"
        month = 3
    elseif str == "Apr" || str == "April"
        month = 4
    elseif str == "May"
        month = 5
    elseif str == "Jun" || str == "June"
        month = 6
    elseif str == "Jul" || str == "July"
        month = 7
    elseif str == "Aug" || str == "August"
        month = 8
    elseif str == "Sep" || str == "September"
        month = 9
    elseif str == "Oct" || str == "October"
        month = 10
    elseif str == "Nov" || str == "November"
        month = 11
    elseif str == "Dec" || str == "December"
        month = 12
    end
    return Int(month)
end

"""
This function reads the canonical season definition file (`capacity_seasons.csv`,
columns `name` and `months`, e.g. "summer","May-Sep") and returns a
Dict{String, Vector{Int64}} mapping each season name to its constituent calendar
months. Wrap-around ranges (e.g. "Oct-Apr") are supported using the same
start-month/end-month logic as the ORDC season parsing in ordc_construction.jl.
Returns Dict("annual" => collect(1:12)) when the file is absent, so cases that
don't define seasons fall back to a single annual capacity market.
"""
function load_season_months(seasons_file::String)
    if !isfile(seasons_file)
        return Dict{String, Vector{Int64}}("annual" => collect(1:12))
    end

    seasons_data = read_data(seasons_file)

    season_months = Dict{String, Vector{Int64}}()
    for row in DataFrames.eachrow(seasons_data)
        months = split(String(row["months"]), "-")
        start_month = month_lookup(String(strip(months[1])))
        end_month = month_lookup(String(strip(months[2])))
        season_name = String(row["name"])

        if start_month <= end_month
            season_months[season_name] = collect(start_month:end_month)
        else
            season_months[season_name] = collect(1:end_month)
            append!(season_months[season_name], collect(start_month:12))
        end
    end

    # Seasonal capacity CONE is scaled by each season's share of the year
    # (`season_share`), so the shares only sum to 1 when every month belongs to exactly
    # one season. Warn (not error) so a deliberately partial definition still runs.
    all_months = sort(vcat(collect(values(season_months))...))
    if all_months != collect(1:12)
        @warn "capacity_seasons.csv ($seasons_file) does not assign every month to " *
              "exactly one season (months covered: $(all_months)). Seasonal capacity " *
              "CONE shares will not sum to 1, so total capacity revenue will not " *
              "match an annual market." maxlog = 1
    end

    return season_months
end

"""
    season_months_for(season_months, season) -> Vector{Int64}

Returns the calendar months of `season`. The synthetic `"annual"` season always maps
to `1:12`, even when it is not a key of `season_months`.

This matters in annual mode: with `seasonal_capacity_market = false`,
`resolve_capacity_seasons` returns `["annual"]` even if a summer/winter
`capacity_seasons.csv` is on disk, so `season_months["annual"]` may not exist.
"""
function season_months_for(season_months::Dict{String, Vector{Int64}}, season::String)
    season == "annual" && return collect(1:12)
    return season_months[season]
end

"""
    season_share(months) -> Float64

Fraction of a non-leap year (365 days) covered by `months`, counted in days.
Used to scale a season's capacity-market prices (Net/Gross CONE and offers, both
quoted per year) to that season's part of the year.

`season_share(collect(1:12)) == 1.0` exactly, so annual mode is unchanged.
Example: May-Sep -> 153/365 ≈ 0.419; Oct-Apr -> 212/365 ≈ 0.581.

Non-leap days match the simulation's load data, which drops Feb 29
(`remove_leap_day!` in rts_reader.jl).
"""
function season_share(months::Vector{Int64})
    # 2001 is an arbitrary non-leap year, used only to get days per month.
    return sum(Dates.daysinmonth(2001, m) for m in months) / 365
end

"""
    season_hour_columns(season_months::Vector{Int64}, simulation_years::Int) -> Vector{Int}

Returns the ascending vector of hour-columns (1-indexed, 1:(DEFAULT_HOURS_PER_YEAR *
simulation_years)) whose calendar month falls in `season_months`. Ascending hour order
is the true chronological (month-year) stitch order required by the divide approach:
within each simulation year, hours are selected in calendar order, and simulation years
are concatenated in order (e.g. for a summer season across 2 years, all of year 1's
summer hours precede all of year 2's summer hours).

Month bucketing uses a fixed non-leap hour-of-year mapping: each hour is reduced to its
hour-of-year via `mod(h - 1, DEFAULT_HOURS_PER_YEAR)` and its month is taken within the
non-leap reference year `SIM_START_DATE`. This deliberately aligns with the underlying
load/availability/outage timeseries, which are assembled as fixed 8760-hour (no leap
day) blocks per simulation year. It intentionally does NOT use leap-aware wall-clock
arithmetic across the full horizon: doing so would drift by up to ~one day per month
boundary in every simulation year that crosses a calendar leap year (e.g. horizons ≥ 3
years from SIM_START_DATE=2018), mis-bucketing ~24 hours per boundary into the wrong
season relative to the data. The PRAS assessment ignores calendar semantics of the
timestamp axis, so diverging from the leap-aware timestamp labels here is harmless.
"""
function season_hour_columns(season_months::Vector{Int64}, simulation_years::Int)
    total_hours = DEFAULT_HOURS_PER_YEAR * simulation_years
    season_month_set = Set(season_months)
    # Precompute the constant hour-of-year → month table once (non-leap reference year),
    # matching the no-leap 8760-hour data blocks rather than the leap-aware timestamp axis.
    hoy_month = [Dates.month(SIM_START_DATE + Dates.Hour(hoy)) for hoy in 0:(DEFAULT_HOURS_PER_YEAR - 1)]
    cols = Int[]
    for h in 1:total_hours
        hoy = mod(h - 1, DEFAULT_HOURS_PER_YEAR)
        if hoy_month[hoy + 1] in season_month_set
            push!(cols, h)
        end
    end
    return cols
end

"""
Parses a month value from numeric, date/datetime, or string inputs.
Accepts month numbers, date-like strings, and month names/abbreviations.
"""
function parse_month_value(value)
    if value isa Integer
        return Int64(value)
    end

    if value isa Dates.Date || value isa Dates.DateTime
        return Int64(Dates.month(value))
    end

    if value isa AbstractString
        str = strip(String(value))
        if isempty(str)
            error("Encountered empty month string while parsing net-load data")
        end
        parsed_int = tryparse(Int64, str)
        if !isnothing(parsed_int)
            return parsed_int
        end

        parsed_dt = tryparse(Dates.DateTime, str)
        if !isnothing(parsed_dt)
            return Int64(Dates.month(parsed_dt))
        end
        parsed_date = tryparse(Dates.Date, str)
        if !isnothing(parsed_date)
            return Int64(Dates.month(parsed_date))
        end

        alpha_prefix = match(r"^[A-Za-z]+", str)
        if !isnothing(alpha_prefix)
            return month_lookup(alpha_prefix.match)
        end
        error("Unable to parse month value '$str' from net-load data")
    end

    error("Unsupported month value type $(typeof(value)) in net-load data")
end

"""
Builds an integer month vector from net-load data.
Uses the explicit `Month` column in the net-load CSV schema.
"""
function derive_month_vector(load_n_vg_data::DataFrames.DataFrame)
    month_col = findfirst(x -> lowercase(String(x)) == "month", names(load_n_vg_data))
    if !isnothing(month_col)
        return Int64[parse_month_value(v) for v in load_n_vg_data[!, month_col]]
    end

    error(
        "Unable to derive months from net-load data: expected a 'Month' column.",
    )
end

"""
    seasonal_peak_loads(load_data, season_months, seasons) -> Dict{String, Float64}

Returns the system-wide peak load (MW) of each capacity season.

`load_data` is the chronological hourly load table written by `rts_reader.jl` to
`timeseries_data_files/{scenario}/sim_year_{y}/Load/load.csv`: columns
`Year, Month, Day, Period` plus one column per zone. This is the same table the
annual `peak_load` is computed from (`rts_reader.jl`, `system_peak_load`). The peak
here is calculated the same way, but only over the hours in each season's months.
Zone columns are everything except `Year/Month/Day/Period`, the same convention as
the reserve scaling in `actual_market_simulation.jl` and `siip_psy_utils.jl`.

The `"annual"` season covers all months (`season_months_for`), so
`seasonal_peak_loads(load_data, season_months, ["annual"])["annual"]` equals the
annual peak. Errors if a season has no hours in `load_data`.
"""
function seasonal_peak_loads(load_data::DataFrames.DataFrame,
                             season_months::Dict{String, Vector{Int64}},
                             seasons::Vector{String})
    zone_data = load_data[:, DataFrames.Not(:Year, :Month, :Day, :Period)]
    system_load = vec(sum(Matrix{Float64}(zone_data); dims = 2))
    months = derive_month_vector(load_data)

    peaks = Dict{String, Float64}()
    for season in seasons
        month_set = Set(season_months_for(season_months, season))
        mask = map(m -> m in month_set, months)
        if !any(mask)
            error("No load hours found for capacity season '$season' " *
                  "(months $(sort(collect(month_set)))) in the load data.")
        end
        peaks[season] = maximum(system_load[mask])
    end
    return peaks
end

"""
    seasonal_peak_loads(timeseries_data_dir, scenario, sim_year, season_months, seasons)

File-reading wrapper: loads
`{timeseries_data_dir}/{scenario}/sim_year_{sim_year}/Load/load.csv` and returns
`seasonal_peak_loads(load_data, season_months, seasons)`.

Callers skip this in annual mode and use `peak_load` directly, so annual runs do no
extra file reads.
"""
function seasonal_peak_loads(timeseries_data_dir::String,
                             scenario::String,
                             sim_year::Int64,
                             season_months::Dict{String, Vector{Int64}},
                             seasons::Vector{String})
    load_data = read_data(joinpath(timeseries_data_dir, scenario,
                                   "sim_year_$(sim_year)", "Load", "load.csv"))
    return seasonal_peak_loads(load_data, season_months, seasons)
end

"""
    resolve_seasonal_peak_loads(annual_peak, timeseries_data_dir, scenario, sim_year,
                                season_months, seasons) -> Dict{String, Float64}

Peak load per capacity season, used for each season's reliability requirement.

- Annual mode (`seasons == ["annual"]`): returns `Dict("annual" => annual_peak)`
  without reading any file, so annual results are exactly unchanged.
- Seasonal mode: reads seasonal peaks from `load.csv` (`seasonal_peak_loads`). If the
  seasons cover all 12 months, checks that the largest seasonal peak equals
  `annual_peak`. A mismatch means `load.csv` and the `peak_load` stored on the
  simulation disagree (e.g. different zone columns or a regenerated file), and
  results would be silently inconsistent, so it errors.
"""
function resolve_seasonal_peak_loads(annual_peak::Float64,
                                     timeseries_data_dir::String,
                                     scenario::String,
                                     sim_year::Int64,
                                     season_months::Dict{String, Vector{Int64}},
                                     seasons::Vector{String})
    if seasons == ["annual"]
        return Dict{String, Float64}("annual" => annual_peak)
    end

    peaks = seasonal_peak_loads(timeseries_data_dir, scenario, sim_year,
                                season_months, seasons)

    covered = sort(vcat([season_months_for(season_months, s) for s in seasons]...))
    if covered == collect(1:12) &&
       !isapprox(maximum(values(peaks)), annual_peak; rtol = 1e-6)
        error("Seasonal peak loads $(peaks) for scenario '$scenario', sim_year " *
              "$sim_year: the largest seasonal peak does not equal the annual peak " *
              "$annual_peak used elsewhere. Check load.csv under $timeseries_data_dir.")
    end
    return peaks
end

function find_rt_periods(hours::Vector{Int64}, num_rt_intervals::Int64)
    rt_periods = [];
    for hour in hours
        append!(
            rt_periods,
            collect(((hour - 1) * num_rt_intervals + 1):(hour * num_rt_intervals)),
        )
    end
    return rt_periods
end

function parsebool(s)
    return lowercase(s) == "true" ? true : false
end

function parseint(s)
    return parse(Int64, s)
end

function parsefloat(s)
    return parse(Float64, s)
end
