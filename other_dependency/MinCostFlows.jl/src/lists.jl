# Singly-linked lists

macro addstart!(elem::Symbol, next::QuoteNode, context::Symbol, first::QuoteNode)

    return quote

        elem_val = $(esc(elem))
        context_val = $(esc(context))
        oldfirstelem = getproperty(context_val, $first)

        if oldfirstelem === nothing # list is empty

            # new element is last as well as first
            setproperty!(elem_val, $next, nothing)

        else # other elements exist

            # add elem before current first element
            setproperty!(elem_val, $next, oldfirstelem)

        end

        # elem is now first element
        setproperty!(context_val, $first, elem_val)

    end

end

macro remove!(context::Symbol, first::QuoteNode, next::QuoteNode)

    return quote

        context_val = $(esc(context))
        oldfirstelem = getproperty(context_val, $first)

        if oldfirstelem !== nothing # nothing to do for an empty list
            newfirstelem = getproperty(oldfirstelem, $next) # Type uncertainty here bottlenecks
            if newfirstelem === nothing
                setproperty!(context_val, $first, nothing)
            else
                setproperty!(context_val, $first, newfirstelem)
            end
        end

    end

end

# Doubly-linked lists

macro addstart!(elem::Symbol, prev::QuoteNode, next::QuoteNode, context::Symbol, first::QuoteNode, last::QuoteNode)

    return quote

        elem_val = $(esc(elem))
        context_val = $(esc(context))
        oldfirstelem = getproperty(context_val, $first)

        if oldfirstelem === nothing # list is empty

            # new element is last as well as first
            setproperty!(elem_val, $next, nothing)
            setproperty!(context_val, $last, elem_val)

        else # other elements exist

            # add elem before current first element
            setproperty!(elem_val, $next, oldfirstelem)
            setproperty!(oldfirstelem, $prev, elem_val)

        end

        # elem is now first element
        setproperty!(context_val, $first, elem_val)
        setproperty!(elem_val, $prev, nothing)

    end

end

macro addend!(elem::Symbol, prev::QuoteNode, next::QuoteNode, context::Symbol, first::QuoteNode, last::QuoteNode)

    return quote

        elem_val = $(esc(elem))
        context_val = $(esc(context))
        oldlastelem = getproperty(context_val, $last)

        if oldlastelem === nothing # list is empty

            # new element is first as well as last
            setproperty!(elem_val, $prev, nothing)
            setproperty!(context_val, $first, elem_val)

        else # other elements exist

            # add elem after current last element
            setproperty!(elem_val, $prev, oldlastelem)
            setproperty!(oldlastelem, $next, elem_val)

        end

        # elem is now last element
        setproperty!(context_val, $last, elem_val)
        setproperty!(elem_val, $next, nothing)

    end

end

macro remove!(elem::Symbol, prev::QuoteNode, next::QuoteNode, context::Symbol, first::QuoteNode, last::QuoteNode)

    return quote

        elem_val = $(esc(elem))
        context_val = $(esc(context))
        prevelem = getproperty(elem_val, $prev)
        nextelem = getproperty(elem_val, $next)

        if prevelem === nothing
            if nextelem === nothing
                # elem was only element in list
                setproperty!(context_val, $first, nothing)
                setproperty!(context_val, $last, nothing)
            else
                # elem was first in list
                setproperty!(context_val, $first, nextelem)
                setproperty!(nextelem, $prev, nothing)
            end
        else
            if nextelem === nothing
                # elem was last in list
                setproperty!(prevelem, $next, nothing)
                setproperty!(context_val, $last, prevelem)
            else
                # elem was neither first nor last in list
                setproperty!(prevelem, $next, nextelem)
                setproperty!(nextelem, $prev, prevelem)
            end
        end

    end

end

function printlist(ctx, first::Symbol, next::Symbol, f::Function)
    l = getproperty(ctx, first)
    print("[")
    while l !== nothing
        print(f(l), ", ")
        l = getproperty(l, next)
    end
    println("]")
end
