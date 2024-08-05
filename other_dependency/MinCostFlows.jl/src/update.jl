"""
Change injections on `targetnode` to `newinj`, automatically adjusting
`slacknode` to compensate.

    updateinjection!(targetnode::Node, slacknode::Note, newinj::Int)

"""
function updateinjection!(targetnode::Node, slacknode::Node, newinj::Int)

    change = newinj - targetnode.injection

    targetnode.injection += change
    targetnode.imbalance += change
    slacknode.injection -= change
    slacknode.imbalance -= change

    return

end

"""
Change line limit on `edge` to `newlimit`.

    updateflowlimit!(edge::Edge, newlimit::Int)

"""
function updateflowlimit!(edge::Edge{Node}, newlimit::Int)

    newlimit < 0 && error("New limit must be non-negative, received $newlimit")

    if (newlimit < edge.flow) || (edge.reducedcost < 0)
        change = edge.flow - newlimit
        edge.flow = newlimit
        edge.nodefrom.imbalance += change
        edge.nodeto.imbalance -= change
    end

    edge.limit = newlimit

    return

end

"""
Charge flow cost on `edge` to `newcost`.

   updateflowcost!(edge::Edge, newcost::Int)

"""
function updateflowcost!(edge::Edge{Node}, newcost::Int)

    oldcost = edge.cost

    oldcost == newcost && return

    oldreducedcost = edge.reducedcost
    newreducedcost = oldreducedcost + newcost - oldcost

    edge.cost = newcost
    edge.reducedcost = newreducedcost

    if oldreducedcost > 0

        if newreducedcost == 0 # Inactive to balanced

            removeinactive!(edge)
            addbalanced!(edge)

        elseif newreducedcost < 0 # Inactive to active

            maxflow!(edge)
            removeinactive!(edge)
            addactive!(edge)

        end

    elseif oldreducedcost == 0

        if newreducedcost > 0 # Balanced to inactive

            minflow!(edge)
            removebalanced!(edge)
            addinactive!(edge)

        elseif newreducedcost < 0 # Balanced to active

            maxflow!(edge)
            removebalanced!(edge)
            addactive!(edge)

        end

    else # oldreducedcost < 0

        if newreducedcost > 0 # Active to inactive

            minflow!(edge)
            removeactive!(edge)
            addinactive!(edge)

        elseif newreducedcost == 0 # Active to balanced

            removeactive!(edge)
            addbalanced!(edge)

        end

    end

    return

end

"""Maximize flow on edge"""
function maxflow!(edge::Edge)

    flowincrease = edge.limit - edge.flow
    edge.flow = edge.limit
    edge.nodefrom.imbalance -= flowincrease
    edge.nodeto.imbalance += flowincrease

    return

end

"""Minimize flow on edge"""
function minflow!(edge::Edge)

    flowdecrease = edge.flow
    edge.flow = 0
    edge.nodefrom.imbalance += flowdecrease
    edge.nodeto.imbalance -= flowdecrease

    return

end
