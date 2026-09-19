const DEFAULT_PSY_CLASSIFICATION_MAPPING = joinpath(
    @__DIR__, "..", "..", "config", "psy5", "psy_classification_mapping.csv"
)
const DEFAULT_PROJECT_DEFAULTS = joinpath(
    @__DIR__, "..", "..", "config", "project_defaults.csv"
)

const PSY_CLASSIFICATION_COLUMNS = [
    :component_type,
    :prime_mover,
    :fuel,
    :unit_type,
]

const PROJECT_OPTION_INPUT_COLUMNS = [
    :Investor,
    :GEN_UID,
    Symbol("Unit Type"),
    :Size,
    :Zone,
    Symbol("Bus ID"),
]

const PROJECT_EXISTING_INPUT_COLUMNS = [:Investor, :GEN_UID]

const PROJECT_INTEGER_DEFAULT_FIELDS = Set([
    "Lagtime",
    "Online Year",
    "Capex Years",
    "Lifetime",
])

function _require_columns(df::DataFrames.DataFrame, columns, path::AbstractString)
    missing_columns = setdiff(columns, Symbol.(names(df)))
    isempty(missing_columns) || error(
        "$(path) is missing required columns: $(join(string.(missing_columns), ", "))"
    )
    return nothing
end

function _require_nonempty_column(df::DataFrames.DataFrame, column::Symbol, path::AbstractString)
    invalid_rows = findall(row -> ismissing(row[column]) || isempty(strip(string(row[column]))), eachrow(df))
    isempty(invalid_rows) || error(
        "$(path) has empty $(column) values at rows $(join(invalid_rows, ", "))"
    )
    return nothing
end

"""
    load_psy_classification_mapping(path=DEFAULT_PSY_CLASSIFICATION_MAPPING)

Load the user-maintained PSY component classification mapping. The mapping is
deliberately not version-tagged; users update it when their installed PSY types
or enum values change.
"""
function load_psy_classification_mapping(
    path::AbstractString=DEFAULT_PSY_CLASSIFICATION_MAPPING,
)
    isfile(path) || error("PSY classification mapping does not exist: $(path)")
    mapping = DataFrames.DataFrame(CSV.File(path))
    _require_columns(mapping, PSY_CLASSIFICATION_COLUMNS, path)

    for column in (:component_type, :unit_type)
        _require_nonempty_column(mapping, column, path)
    end

    keys = Tuple.(eachrow(mapping[:, PSY_CLASSIFICATION_COLUMNS[1:3]]))
    length(unique(keys)) == length(keys) || error(
        "$(path) contains duplicate component_type/prime_mover/fuel mappings"
    )
    return mapping
end

"""
    validate_psy_classification_mapping(mapping, technologies)

Validate mapping targets against the technology configuration.
"""
function validate_psy_classification_mapping(
    mapping::DataFrames.DataFrame,
    technologies::DataFrames.DataFrame,
)
    _require_columns(mapping, PSY_CLASSIFICATION_COLUMNS, "PSY classification mapping")
    _require_columns(technologies, [:unit_type], "technologies.csv")
    valid_unit_types = Set(string.(technologies.unit_type))
    invalid = sort!(unique([
        unit_type for unit_type in string.(mapping.unit_type) if
        !in(unit_type, valid_unit_types)
    ]))
    isempty(invalid) || error(
        "PSY classification mapping references unknown unit_type values: $(join(invalid, ", "))"
    )
    return true
end

function load_project_defaults(path::AbstractString=DEFAULT_PROJECT_DEFAULTS)
    isfile(path) || error("Project defaults do not exist: $(path)")
    defaults = DataFrames.DataFrame(CSV.File(path))
    _require_columns(defaults, [:unit_type, :field, :value], path)
    for column in [:unit_type, :field]
        _require_nonempty_column(defaults, column, path)
    end
    keys = Tuple.(eachrow(defaults[:, [:unit_type, :field]]))
    length(unique(keys)) == length(keys) || error(
        "$(path) contains duplicate unit_type/field defaults"
    )
    defaults.value = [
        _normalize_default_value(row.field, row.value) for row in eachrow(defaults)
    ]
    return defaults
end

function _normalize_default_value(field, value)
    value === missing && return value
    string(field) in PROJECT_INTEGER_DEFAULT_FIELDS || return value
    parsed = tryparse(Float64, strip(string(value)))
    parsed === nothing && return value
    return isinteger(parsed) ? string(Int(parsed)) : string(parsed)
end

function validate_project_option_cost_curve(df::DataFrames.DataFrame, path::AbstractString)
    if !("Unit Type" in names(df)) || !("Output_pct_0" in names(df))
        return nothing
    end
    thermal_types = Set(["ST", "CT", "CC", "GT", "RE_CT", "IC", "NU_ST"])
    for row in eachrow(df)
        unit_type = uppercase(strip(string(row[Symbol("Unit Type")])) )
        unit_type in thermal_types || continue
        values = Float64[]
        for idx in 0:4
            field = "Output_pct_$(idx)"
            heat_rate_field = idx == 0 ? "HR_avg_0" : "HR_incr_$(idx)"
            field in names(df) || continue
            heat_rate_field in names(df) || error("$(path) is missing $(heat_rate_field) for unit $(row[Symbol("Unit Type")])")
            value = row[Symbol(field)]
            heat_rate_value = row[Symbol(heat_rate_field)]
            text = strip(string(value))
            heat_rate_text = strip(string(heat_rate_value))
            output_is_na = uppercase(text) == "NA"
            heat_rate_is_na = uppercase(heat_rate_text) == "NA"
            output_is_na == heat_rate_is_na || error(
                "$(path) has an incomplete curve point $(field)/$(heat_rate_field) for unit $(row[Symbol("Unit Type")])"
            )
            output_is_na && continue
            isempty(text) && error("$(path) has an empty $(field) for unit $(row[Symbol("Unit Type")])")
            parsed = tryparse(Float64, text)
            parsed === nothing && error("$(path) has a non-numeric $(field) for unit $(row[Symbol("Unit Type")])")
            tryparse(Float64, heat_rate_text) === nothing && error(
                "$(path) has a non-numeric $(heat_rate_field) for unit $(row[Symbol("Unit Type")])"
            )
            0.0 <= parsed <= 1.0 || error("$(path) has an invalid $(field)=$(parsed) for unit $(row[Symbol("Unit Type")]); values must be in [0,1]")
            push!(values, parsed)
        end
        length(values) >= 2 || error("$(path) has fewer than two valid thermal cost-curve points for unit $(row[Symbol("Unit Type")])")
        all(diff(values) .> 0.0) || error(
            "$(path) has a non-ascending thermal cost curve for unit $(row[Symbol("Unit Type")]): $(values)"
        )
    end
    return nothing
end

"""Validate one of the minimal user-facing project input templates."""
function validate_project_input_template(
    path::AbstractString;
    kind::Symbol,
    investors::Union{Nothing, AbstractVector}=nothing,
)
    isfile(path) || error("Project input template does not exist: $(path)")
    input = DataFrames.DataFrame(CSV.File(path; stringtype=String))
    required = kind === :options ? PROJECT_OPTION_INPUT_COLUMNS :
               kind === :existing ? PROJECT_EXISTING_INPUT_COLUMNS :
               error("kind must be :options or :existing")
    _require_columns(input, required, path)

    for column in required[1:2]
        _require_nonempty_column(input, column, path)
    end
    if kind === :options
        _require_nonempty_column(input, Symbol("Unit Type"), path)
        _require_nonempty_column(input, :Size, path)
        for row in eachrow(input)
            has_zone = !(ismissing(row.Zone) || isempty(strip(string(row.Zone))))
            has_bus = !(ismissing(row[Symbol("Bus ID")]) || isempty(strip(string(row[Symbol("Bus ID")]))))
            has_zone || has_bus || error(
                "$(path) requires Zone or Bus ID for GEN_UID $(row.GEN_UID)"
            )
        end
    end

    if investors !== nothing
        valid_investors = Set(string.(investors))
        invalid = sort!(unique([
            investor for investor in string.(input.Investor) if
            !in(investor, valid_investors)
        ]))
        isempty(invalid) || error(
            "$(path) references investors not listed in project_spec.csv: $(join(invalid, ", "))"
        )
    end
    return input
end