@testset "Link Operations" begin

    @testset "Singly-Linked Lists" begin

        ctx = Context()
        l1 = Link(1)
        l2 = Link(2)

        @test ctx.fst === nothing
        
        MinCostFlows.@addstart!(l2, :nxt, ctx, :fst)
        MinCostFlows.@addstart!(l1, :nxt, ctx, :fst)

        @test ctx.fst === l1
        @test l1.nxt === l2
        @test l2.nxt === nothing

        MinCostFlows.@remove!(ctx, :fst, :nxt)
        @test ctx.fst === l2
        @test l2.nxt === nothing

    end

    @testset "Doubly-Linked Lists" begin

        ctx = Context()
        l1 = Link(1)
        l2 = Link(2)
        l3 = Link(3)
        l4 = Link(4)
        l5 = Link(5)

        @test ctx.fst === nothing
        @test ctx.lst === nothing

        # Build list
        MinCostFlows.@addstart!(l3, :prv, :nxt, ctx, :fst, :lst)
        MinCostFlows.@addend!(l4, :prv, :nxt, ctx, :fst, :lst)
        MinCostFlows.@addstart!(l2, :prv, :nxt, ctx, :fst, :lst)

        @test firstfromlast(ctx) === ctx.fst
        @test lastfromfirst(ctx) === ctx.lst

        @test ctx.fst === l2
        @test l2.prv === nothing
        @test l2.nxt === l3
        @test l3.prv === l2
        @test l3.nxt === l4
        @test l4.prv === l3
        @test l4.nxt === nothing
        @test ctx.lst === l4

        # Modify list
        MinCostFlows.@addstart!(l1, :prv, :nxt, ctx, :fst, :lst)
        MinCostFlows.@addend!(l5, :prv, :nxt, ctx, :fst, :lst)
        MinCostFlows.@remove!(l3, :prv, :nxt, ctx, :fst, :lst)

        @test ctx.fst === l1
        @test l1.prv === nothing
        @test l1.nxt === l2
        @test l2.prv === l1
        @test l2.nxt === l4
        @test l4.prv === l2
        @test l4.nxt === l5
        @test l5.prv === l4
        @test l5.nxt === nothing
        @test ctx.lst === l5

        MinCostFlows.@remove!(l1, :prv, :nxt, ctx, :fst, :lst)

        @test ctx.fst === l2
        @test l2.prv === nothing
        @test l2.nxt === l4
        @test l4.prv === l2
        @test l4.nxt === l5
        @test l5.prv === l4
        @test l5.nxt === nothing
        @test ctx.lst === l5

        MinCostFlows.@remove!(l5, :prv, :nxt, ctx, :fst, :lst)

        @test ctx.fst === l2
        @test l2.prv === nothing
        @test l2.nxt === l4
        @test l4.prv === l2
        @test l4.nxt === nothing
        @test ctx.lst === l4

        MinCostFlows.@remove!(l2, :prv, :nxt, ctx, :fst, :lst)

        @test ctx.fst === l4
        @test l4.prv === nothing
        @test l4.nxt === nothing
        @test ctx.lst === l4

        MinCostFlows.@remove!(l4, :prv, :nxt, ctx, :fst, :lst)

        @test ctx.fst === nothing
        @test ctx.lst === nothing

    end

end
