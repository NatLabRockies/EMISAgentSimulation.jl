"""
Returns true if the process updated flows or prices, and false otherwise.
"""
function singlenodeupdate!(fp::FlowProblem, i::Node)

    statechanged = false
    x = imbalancecomparator(i)

    while true

        # Step 1: Compare i.imbalance with B+ and B- room
        if i.imbalance >= x

            # Step 4: Max/min out flows and increase i.price
            i.imbalance -= x
            maxminflows!(i)
            updateprice!(i)
            statechanged = true

            if i.imbalance > 0
                x = imbalancecomparator(i)
                continue
            else
                break
            end

        elseif i.imbalance > 0

            # Step 2 or 3: Flow adjustment
            if updateoutgoingflow!(i) || updateincomingflow!(i)
                statechanged = true
                continue
            else
                break
            end

        else # i.imbalance == 0

            break

        end

    end

    return statechanged

end

function imbalancecomparator(i::Node)

    x = 0

    ij = i.firstbalancedfrom
    while ij !== nothing
        if ij.flow < ij.limit
            x += ij.limit
            x -= ij.flow
        end
        ij = ij.nextbalancedfrom
    end

    ji = i.firstbalancedto
    while ji !== nothing
        if ji.flow > 0
            x += ji.flow
        end
        ji = ji.nextbalancedto
    end

    return x

end

function maxminflows!(i::Node)

    # i.imbalance already taken care of in main function

    ij = i.firstbalancedfrom
    while ij !== nothing
        j = ij.nodeto
        j.imbalance += ij.limit - ij.flow
        ij.flow = ij.limit
        ij = ij.nextbalancedfrom
    end

    ji = i.firstbalancedto
    while ji !== nothing
        j = ji.nodefrom
        j.imbalance += ji.flow
        ji.flow = 0
        ji = ji.nextbalancedto
    end

    return

end

function updateprice!(i::Node)

    newprice = typemax(Int)

    ij = i.firstinactivefrom
    while ij !== nothing
        j = ij.nodeto
        newprice = min(newprice, j.price + ij.cost)
        ij = ij.nextinactivefrom
    end

    ji = i.firstactiveto
    while ji !== nothing
        j = ji.nodefrom
        newprice = min(newprice, j.price - ji.cost)
        ji = ji.nextactiveto
    end

    if newprice === typemax(Int)
        i.imbalance > 0 && error("Problem is infeasible")
        return
    end

    pricechange = newprice - i.price
    i.price = newprice

    # Update reduced costs + edge categories

    # Edge out of i have reduced cost decreased by pricechange
    ij = i.firstfrom
    while ij !== nothing
        decreasereducedcost!(ij, pricechange)
        ij = ij.nextfrom
    end

    # Edge in to i have reduced cost increased by pricechange
    ji = i.firstto
    while ji !== nothing
        increasereducedcost!(ji, pricechange)
        ji = ji.nextto
    end

end

function updateoutgoingflow!(i::Node)

    # Choose j
    ij = i.firstbalancedfrom
    j = i # Initialize j
    while true
        ij === nothing && return false
        j = ij.nodeto
        j.imbalance < 0 && ij.flow < ij.limit && break
        ij = ij.nextbalancedfrom
    end

    delta = min(i.imbalance, -j.imbalance, ij.limit - ij.flow)
    ij.flow += delta
    i.imbalance -= delta
    j.imbalance += delta

    return true

end

function updateincomingflow!(i::Node)

    # Choose j
    ji = i.firstbalancedto
    j = i # Initialize j
    while true
        ji === nothing && return false
        j = ji.nodefrom
        j.imbalance < 0 && ji.flow > 0 && break
        ji = ji.nextbalancedto
    end

    delta = min(i.imbalance, -j.imbalance, ji.flow)
    ji.flow -= delta
    j.imbalance += delta
    i.imbalance -= delta

    return true

end
