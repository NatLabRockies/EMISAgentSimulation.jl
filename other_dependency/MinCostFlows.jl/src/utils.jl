function edgelist(fp::FlowProblem)

    edgelist = Vector{Pair{Int,Int}}(undef, length(fp.edges))
    for (e, edge) in enumerate(fp.edges)
        i = nodeidx(fp, edge.nodefrom)
        j = nodeidx(fp, edge.nodeto)
        edgelist[e] = Pair(i,j)
    end

    return edgelist

end

function showSL(fp::FlowProblem)

    S = Int[]
    L = Int[]
    print("L: [")

    node = fp.firstL
    while node !== nothing
        i = nodeidx(fp, node)
        print(i, ", ")
        i in L && error("Loop in L!!")
        push!(L, i)
        node.inS && push!(S, i)
        node = node.nextL
    end

    println("]")
    println("S: ", S)

end

nodeidx(fp::FlowProblem, node::Node) = findfirst(n -> n === node, fp.nodes)

function complementaryslackness(fp::FlowProblem)

    satisfied = true

    for edge in fp.edges

        # Check condition for active arcs
        if (edge.reducedcost < 0) && (edge.flow != edge.limit)
            i = findfirst(n -> n === edge.nodefrom, fp.nodes)
            j = findfirst(n -> n === edge.nodeto, fp.nodes)
            println("Active arc violation on edge $i => $j, reduced cost = $(edge.reducedcost)")
            satisfied = false

        # Check condition for balanced arcs
        elseif (edge.reducedcost == 0) && (edge.flow < 0 || edge.flow > edge.limit)
            i = findfirst(n -> n === edge.nodefrom, fp.nodes)
            j = findfirst(n -> n === edge.nodeto, fp.nodes)
            println("Balanced arc violation on edge $i => $j, reduced cost = $(edge.reducedcost)")
            satisfied = false

        # Check condition for inactive arcs
        elseif (edge.reducedcost > 0) && (edge.flow != 0)
            i = findfirst(n -> n === edge.nodefrom, fp.nodes)
            j = findfirst(n -> n === edge.nodeto, fp.nodes)
            println("Inactive arc violation on edge $i => $j, reduced cost = $(edge.reducedcost)")
            satisfied = false

        end

    end

    return satisfied

end

function flowbalance(fp::FlowProblem)

    for node in fp.nodes

        imbalance = node.injection

        edge = node.firstfrom
        while edge !== nothing
            imbalance -= edge.flow
            edge = edge.nextfrom
        end

        edge = node.firstto
        while edge !== nothing
            imbalance += edge.flow
            edge = edge.nextto
        end

        (node.imbalance != imbalance) && return false

    end

    return true

end

function removeinactive!(edge::Edge)

    fromnode = edge.nodefrom
    tonode = edge.nodeto

    @remove!(edge, :previnactivefrom, :nextinactivefrom,
             fromnode, :firstinactivefrom, :lastinactivefrom)
    @remove!(edge, :previnactiveto, :nextinactiveto,
             tonode, :firstinactiveto, :lastinactiveto)

    return

end

function removebalanced!(edge::Edge)

    fromnode = edge.nodefrom
    tonode = edge.nodeto

    @remove!(edge, :prevbalancedfrom, :nextbalancedfrom,
             fromnode, :firstbalancedfrom, :lastbalancedfrom)
    @remove!(edge, :prevbalancedto, :nextbalancedto,
             tonode, :firstbalancedto, :lastbalancedto)

    return

end

function removeactive!(edge::Edge)

    fromnode = edge.nodefrom
    tonode = edge.nodeto

    @remove!(edge, :prevactivefrom, :nextactivefrom,
             fromnode, :firstactivefrom, :lastactivefrom)
    @remove!(edge, :prevactiveto, :nextactiveto,
             tonode, :firstactiveto, :lastactiveto)

    return

end

function addinactive!(edge::Edge)

    fromnode = edge.nodefrom
    tonode = edge.nodeto

    @addend!(edge, :previnactivefrom, :nextinactivefrom,
             fromnode, :firstinactivefrom, :lastinactivefrom)

    @addend!(edge, :previnactiveto, :nextinactiveto,
             tonode, :firstinactiveto, :lastinactiveto)

    return

end

function addbalanced!(edge::Edge)

    fromnode = edge.nodefrom
    tonode = edge.nodeto

    @addend!(edge, :prevbalancedfrom, :nextbalancedfrom,
             fromnode, :firstbalancedfrom, :lastbalancedfrom)

    @addend!(edge, :prevbalancedto, :nextbalancedto,
             tonode, :firstbalancedto, :lastbalancedto)

    return

end

function addactive!(edge::Edge)

    fromnode = edge.nodefrom
    tonode = edge.nodeto

    @addend!(edge, :prevactivefrom, :nextactivefrom,
             fromnode, :firstactivefrom, :lastactivefrom)

    @addend!(edge, :prevactiveto, :nextactiveto,
             tonode, :firstactiveto, :lastactiveto)

    return

end
