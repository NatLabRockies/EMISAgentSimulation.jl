abstract type AbstractNode end

mutable struct Edge{T<:AbstractNode}

    nodefrom::T
    nodeto::T
    nextfrom::Union{Edge{T},Nothing}
    nextto::Union{Edge{T},Nothing}
    nextinactivefrom::Union{Edge{T},Nothing}
    previnactivefrom::Union{Edge{T},Nothing}
    nextinactiveto::Union{Edge{T},Nothing}
    previnactiveto::Union{Edge{T},Nothing}
    nextbalancedfrom::Union{Edge{T},Nothing}
    prevbalancedfrom::Union{Edge{T},Nothing}
    nextbalancedto::Union{Edge{T},Nothing}
    prevbalancedto::Union{Edge{T},Nothing}
    nextactivefrom::Union{Edge{T},Nothing}
    prevactivefrom::Union{Edge{T},Nothing}
    nextactiveto::Union{Edge{T},Nothing}
    prevactiveto::Union{Edge{T},Nothing}
    limit::Int
    cost::Int
    reducedcost::Int
    flow::Int

    function Edge{}(from::T, to::T, limit::Int, cost::Int) where {T <: AbstractNode}

        if cost < 0 # Edge will start active, flow must be maxed
            from.imbalance -= limit
            to.imbalance += limit
            flow = limit
        else # Edge will start balanced or inactive, flow can be zero
            flow = 0
        end

        new{T}(from, to, nothing, nothing, nothing, nothing, nothing, nothing,
               nothing, nothing, nothing, nothing,
               nothing, nothing, nothing, nothing, limit, cost, cost, flow)
    end

end

Base.show(io::IO, e::Edge) = print(io, "Edge")
Base.print(io::IO, e::Edge) = print(io, "Edge")

# TODO: These likely don't all need to be doubly-linked lists,
#       simplifying some might provide a small speedup?
#       (would need a workaround during initialization though)
mutable struct Node <: AbstractNode
    firstfrom::Union{Edge{Node},Nothing}
    firstto::Union{Edge{Node},Nothing}
    firstinactivefrom::Union{Edge{Node},Nothing}
    lastinactivefrom::Union{Edge{Node},Nothing}
    firstinactiveto::Union{Edge{Node},Nothing}
    lastinactiveto::Union{Edge{Node},Nothing}
    firstbalancedfrom::Union{Edge{Node},Nothing}
    lastbalancedfrom::Union{Edge{Node},Nothing}
    firstbalancedto::Union{Edge{Node},Nothing}
    lastbalancedto::Union{Edge{Node},Nothing}
    firstactivefrom::Union{Edge{Node},Nothing}
    lastactivefrom::Union{Edge{Node},Nothing}
    firstactiveto::Union{Edge{Node},Nothing}
    lastactiveto::Union{Edge{Node},Nothing}
    augpathprev::Union{Edge{Node},Nothing}
    nextS::Union{Node,Nothing}
    nextLnotS::Union{Node,Nothing}
    injection::Int
    price::Int
    imbalance::Int
    inS::Bool
    inL::Bool

    Node(injection::Int) = new(nothing, nothing, nothing, nothing,
                               nothing, nothing, nothing, nothing,
                               nothing, nothing, nothing, nothing,
                               nothing, nothing, nothing, nothing,
                               nothing, injection, 0, injection, false, false)

end

Base.show(io::IO, n::Node) = print(io, "Node")
Base.print(io::IO, n::Node) = print(io, "Node")

mutable struct FlowProblem
    nodes::Vector{Node}
    edges::Vector{Edge{Node}}
    firstS::Union{Node,Nothing}
    firstLnotS::Union{Node,Nothing}
    ascentgradient::Int
end

function FlowProblem(nodesfrom::Vector{Int}, nodesto::Vector{Int},
                     limits::Vector{Int}, costs::Vector{Int},
                     injections::Vector{Int})

    n = length(injections)
    e = length(nodesfrom)

    @assert length(nodesto) == e
    @assert length(limits) == e
    @assert length(costs) == e
    @assert sum(injections) == 0

    nodes = Node.(injections)
    edges = Edge.(nodes[nodesfrom], nodes[nodesto], limits, costs)

    # Initialize the to/from doubly-linked lists
    for edge in edges

        nodefrom = edge.nodefrom
        nodeto = edge.nodeto

        # Add edge to nodefrom's from adjacency list
        @addstart!(edge, :nextfrom, nodefrom, :firstfrom)

        # Add edge to nodeto's to adjacency list
        @addstart!(edge, :nextto, nodeto, :firstto)

        if edge.reducedcost === 0 # Balanced edge

            addbalanced!(edge)

        elseif edge.reducedcost > 0  # Inactive edge

            addinactive!(edge)

        else # Active edge

            addactive!(edge)

        end

    end

    return FlowProblem(nodes, edges, nothing, nothing, 0)

end

flows(fp::FlowProblem) = getproperty.(fp.edges, :flow)
costs(fp::FlowProblem) = getproperty.(fp.edges, :cost)
limits(fp::FlowProblem) = getproperty.(fp.edges, :limit)
injections(fp::FlowProblem) = getproperty.(fp.nodes, :injection)
prices(fp::FlowProblem) = getproperty.(fp.nodes, :price)

Base.show(io::IO, fp::FlowProblem) =
    print(io, "FlowProblem with $(length(fp.nodes)) nodes " *
              "and $(length(fp.edges)) edges")

Base.print(io::IO, fp::FlowProblem) =
    print(io, "FlowProblem with $(length(fp.nodes)) nodes " *
              "and $(length(fp.edges)) edges")
