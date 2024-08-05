mutable struct Link
    id::Int
    prv::Union{Link,Nothing}
    nxt::Union{Link,Nothing}
    Link(id::Int) = new(id, nothing, nothing)
end

mutable struct Context
    fst::Union{Link,Nothing}
    lst::Union{Link,Nothing}
    Context() = new(nothing, nothing)
end

function firstfromlast(ctx::Context)

    l = ctx.lst
    l === nothing && return nothing

    while l !== nothing
        l.prv === nothing && return l
        l = l.prv
    end

end

function lastfromfirst(ctx::Context)

    l = ctx.fst
    l === nothing && return nothing

    while l !== nothing
        l.nxt === nothing && return l
        l = l.nxt
    end

end
