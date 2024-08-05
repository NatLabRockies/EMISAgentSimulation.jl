function multinodeupdate!(fp::FlowProblem, augmentingpathstart::Node)

    iters = 0

    # Reset the scan sets for this iteration
    resetSL!(fp, augmentingpathstart)

    while true

        iters += 1

        # Look for a candidate node i to scan and add to S
        i = augmentS!(fp)

        # Update prices if it will improve the dual solution
        # or if there are no nodes left to scan
        if i === nothing || (fp.ascentgradient > 0)
            updateprices!(fp)
            break
        end

        # Label neighbour nodes of i
        augmentingpathend = augmentL!(fp, i)

        # Didn't find an augmenting path, try adding a different node
        augmentingpathend === nothing && continue

        # Found an augmenting path, augment flows accordingly
        updateflows!(fp, augmentingpathstart, augmentingpathend)
        break

    end

    return iters

end

"""
Empties the set S and reduces the set L to a single element, `j`
"""
function resetSL!(fp::FlowProblem, j::Node)

    node = fp.firstS
    while node !== nothing
        node.inL = false
        node.inS = false
        node = node.nextS
    end

    node = fp.firstLnotS
    while node !== nothing
        node.inL = false
        node = node.nextLnotS
    end

    # Reset S to {}
    fp.firstS = nothing
    fp.ascentgradient = 0

    # Reset L to {j}
    fp.firstLnotS = j
    j.nextLnotS = nothing
    j.inL = true

    return nothing

end

"""
If S is a subset of L, return an element of L - S after adding that element
to S. If S == L, return `nothing` and make no changes.
"""
function augmentS!(fp::FlowProblem)

    i = fp.firstLnotS
    i === nothing && return nothing

    # i joins S and leaves LnotS
    i.inS = true
    add_S!(i, fp)
    removefirst_LnotS!(fp)

    # Update the ascent gradient with i included in S
    ij = i.firstbalancedfrom
    while ij !== nothing
        # TODO: Could factor out ij.flow here (save an Int allocation?)
        if ij.nodeto.inS
            fp.ascentgradient += ij.flow
        else
            fp.ascentgradient -= (ij.limit - ij.flow)
        end
        ij = ij.nextbalancedfrom
    end

    ji = i.firstbalancedto
    while ji !== nothing
        # TODO: Could factor out ji.flow here (save an Int allocation?)
        if ji.nodefrom.inS
            fp.ascentgradient += (ji.limit - ji.flow)
        else
            fp.ascentgradient -= ji.flow
        end
        ji = ji.nextbalancedto
    end

    return i

end

"""
Add nodes to L that are adjacent to `i` and satisfy inclusion criteria,
returning one such added node that has a positive imbalance
(or `nothing` if none exist). Subset of Step 2 in Bertsekas.
"""
function augmentL!(fp::FlowProblem, i::N)::Union{N,Nothing} where {N<:Node}

    negativeimbalancenode = nothing

    # Iterate through balanced edges ji connecting j to i
    ji = i.firstbalancedto
    while ji !== nothing

        j = ji.nodefrom # source node of edge ji

        if (!j.inL && ji.flow > 0) # flow enters L and exceeds lower bound

            # Add j to LnotS
            j.inL = true
            add_LnotS!(j, fp)

            # Save path back to i
            j.augpathprev = ji

            # Check for negative imbalance and save for returning
            j.imbalance < 0 && (negativeimbalancenode = j)

        end

        ji = ji.nextbalancedto # Get next edge

    end

    # Iterate through balanced edges ij connecting i to j
    ij = i.firstbalancedfrom
    while ij !== nothing

        j = ij.nodeto # sink node of edge ij

        if (!j.inL && ij.flow < ij.limit) # flow leaves L and is less than limit

            # Add j to LnotS
            j.inL = true
            add_LnotS!(j, fp)

            # Save path back to i
            j.augpathprev = ij

            # Check for negative imbalance and save for returning
            j.imbalance < 0 && (negativeimbalancenode = j)

        end

        ij = ij.nextbalancedfrom # Get next edge

    end

    return negativeimbalancenode

end

"""
Updates the flows on the augmenting path from `startnode` to `endnode`
(Step 3 in Bertsekas)
"""
function updateflows!(fp::FlowProblem, startnode::Node, endnode::Node)

    delta = min(startnode.imbalance, -endnode.imbalance)

    # First pass, determine value of delta
    # Traverse the augmenting path backwards to startnode
    currentnode = endnode
    while currentnode !== startnode

        # Determine edge direction and move to previous node
        ab = currentnode.augpathprev # Get edge
        a = ab.nodefrom # Get edge to/from nodes
        b = ab.nodeto

        if b === currentnode # ab is a forward edge in the path, move to node a
            delta = min(delta, ab.limit - ab.flow)
            currentnode = a
        else # ab is a backwards edge in the path, move to node b
            delta = min(delta, ab.flow)
            currentnode = b
        end

    end

    # Second pass, adjust flows by delta
    currentnode = endnode
    while currentnode !== startnode

        # Determine edge direction and move to previous node
        ab = currentnode.augpathprev # Get edge
        a = ab.nodefrom # Get edge to/from nodes
        b = ab.nodeto

        if b === currentnode # ab is a forward edge, move to node a
            ab.flow += delta
            a.imbalance -= delta
            b.imbalance += delta
            currentnode = a
        else # ab is a backward edge, move to node b
            ab.flow -= delta
            a.imbalance += delta
            b.imbalance -= delta
            currentnode = b
        end

    end

end

"""
Updates the shadow prices (and potentially flows) of the elements of S
(Step 4 in Bertsekas)
"""
function updateprices!(fp::FlowProblem)

    gamma = typemax(Int)

    # TODO: Search with S or notS, based on set sizes
    #       (but will managing the notS list pay for itself?)
    i = fp.firstS
    while i !== nothing


        # Adjust flows on balanced lines in to / out of S
        ij = i.firstbalancedfrom
        while ij !== nothing
            if !ij.nodeto.inS
                increase = ij.limit - ij.flow
                ij.flow = ij.limit
                i.imbalance -= increase
                ij.nodeto.imbalance += increase
            end
            ij = ij.nextbalancedfrom
        end

        ji = i.firstbalancedto
        while ji !== nothing
            if !ji.nodefrom.inS
                decrease = ji.flow
                ji.flow = 0
                ji.nodefrom.imbalance += decrease
                i.imbalance -= decrease
             end
            ji = ji.nextbalancedto
        end

        # Determine gamma from active / inactive lines in to / out of S
        ij = i.firstinactivefrom
        while ij !== nothing
            !ij.nodeto.inS && (gamma = min(gamma, ij.reducedcost)) # Promotion impacting performance here?
            ij = ij.nextinactivefrom
        end

        ji = i.firstactiveto
        while ji !== nothing
            !ji.nodefrom.inS && (gamma = min(gamma, -ji.reducedcost))
            ji = ji.nextactiveto
        end

        i = i.nextS

    end

    gamma === typemax(Int) && error("Problem is infeasible")

    # Adjust node prices and edge reduced costs
    i = fp.firstS
    while i !== nothing

        # Price is increase by gamma
        i.price += gamma

        # Edges from i have reduced cost decreased by gamma
        ij = i.firstfrom
        while ij !== nothing

            j = ij.nodeto

            # S->S edges don't change
            j.inS || decreasereducedcost!(ij, gamma)

            ij = ij.nextfrom

        end

        # Edges to i have reduced cost increased by gamma
        ji = i.firstto
        while ji !== nothing

            j = ji.nodefrom

            # S->S edges don't change
            j.inS || increasereducedcost!(ji, gamma)

            ji = ji.nextto

        end

        i = i.nextS

    end

end

add_LnotS!(node::Node, fp::FlowProblem) = (@addstart!(node, :nextLnotS, fp, :firstLnotS); nothing)
removefirst_LnotS!(fp::FlowProblem) = (@remove!(fp, :firstLnotS, :nextLnotS); nothing)
add_S!(node::Node, fp::FlowProblem) = (@addstart!(node, :nextS, fp, :firstS); nothing)
