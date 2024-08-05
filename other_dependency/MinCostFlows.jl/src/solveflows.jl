"""
Implements the Relaxation dual-ascent method for solving
min-cost flow problems, as described in Bertsekas (1998)
"""
function solveflows!(fp::FlowProblem; verbose::Bool=false)

    verbose && (starttime = time())

    majoriters = 0
    majoriters_multinode = 0
    minoriters = 0

    while true

        # Find a node with positive imbalance
        positiveimbalancenode = firstpositiveimbalance(fp)

        # If no starting node found, problem is either solved or infeasible
        # TODO: Need to throw an exception if infeasible
        positiveimbalancenode === nothing && break

        majoriters += 1

        # First try a single-node relaxation iteration,
        # moving to next iteration if it makes changes
        singlenodeupdate!(fp, positiveimbalancenode) && continue

        # If the single-node iteration doesn't change anything, 
        # run a multi-node relaxation iteration, updating either
        # flows and/or shadow prioces
        majoriters_multinode += 1
        minoriters += multinodeupdate!(fp, positiveimbalancenode)

    end

    if verbose
        endtime = time()
        elapsedtime = endtime - starttime
        majoriters_singlenode = majoriters - majoriters_multinode
        println(majoriters, " major iterations: ",
                majoriters_singlenode, " single-node major iterations, ",
                majoriters_multinode, " multi-node major iterations with ",
                minoriters, " multi-node minor iterations")
        println("Solved in ", elapsedtime, "s")
    end

    return fp

end

#TODO: A linked list that gets trimmed down on-the-fly might be faster here

"""
Finds first node in `nodes` with a positive imbalance
"""
function firstpositiveimbalance(fp::FlowProblem)
    for node in fp.nodes
        (node.imbalance > 0) && return node
    end
    return nothing
end

function decreasereducedcost!(ij::Edge{Node}, pricechange::Int)

    oldreducedcost = ij.reducedcost
    newreducedcost = oldreducedcost - pricechange # Price change assumed positive on node i
    ij.reducedcost = newreducedcost

    if oldreducedcost === 0 # ij moved from balanced -> active

        removebalanced!(ij)
        addactive!(ij)

    elseif newreducedcost === 0 # ij moved from inactive -> balanced

        removeinactive!(ij)
        addbalanced!(ij)

    end

end

function increasereducedcost!(ij::Edge{Node}, pricechange::Int)

    oldreducedcost = ij.reducedcost
    newreducedcost = oldreducedcost + pricechange # Price change assumed positive on node j
    ij.reducedcost = newreducedcost

    if oldreducedcost === 0 # ij moved from balanced -> inactive

        removebalanced!(ij)
        addinactive!(ij)

    elseif newreducedcost === 0 # ji moved from active -> balanced

        removeactive!(ij)
        addbalanced!(ij)

    end

end
