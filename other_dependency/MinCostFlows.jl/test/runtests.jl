using MinCostFlows
using Test
using LinearAlgebra
using SparseArrays
using Profile
using Random

include("utils.jl")
include("listutils.jl")
verbose = false

function primalobjective(fp::FlowProblem)

    obj = 0

    for edge in fp.edges
        obj += edge.flow * edge.cost
    end

    return obj

end

function dualobjective(fp::FlowProblem)

    obj = 0

    for edge in fp.edges
        edge.reducedcost < 0 && (obj += edge.reducedcost * edge.limit)
    end

    for node in fp.nodes
        obj += node.price * node.injection
    end

    return obj

end

@testset "MinCostFlows" begin

    include("lists.jl")

    if true
    @testset "Example networks" begin

        @testset "Bertsekas page 220" begin

            fp = FlowProblem([1,2], [2,3], [5,5], [0,1], [1,0,-1])
            println(fp)
            println(fp.nodes, "\n", fp.edges)
            println(MinCostFlows.edgelist(fp))
            @test MinCostFlows.complementaryslackness(fp) # Initialization should satisfy CS

            lp = linprog(fp)
            solveflows!(fp, verbose=true)

            @test MinCostFlows.complementaryslackness(fp) # Solving should preserve CS
            @test flows(fp) == [1,1]
            @test prices(fp) == [1,1,0]
            @test flows(fp) == lp.flows

            @test primalobjective(fp) == dualobjective(fp)

        end

        @testset "Bertsekas page 237" begin

            fp = FlowProblem([1,1,2,3,2,3], [2,3,3,2,4,4], [2,2,3,2,1,5],
                              [5,1,4,3,2,0], [3,2,-1,-4])
            @test MinCostFlows.complementaryslackness(fp) # Initialization should satisfy CS

            lp = linprog(fp)
            solveflows!(fp)

            @test MinCostFlows.complementaryslackness(fp) # Solving should preserve CS
            @test flows(fp) == [1,2,2,0,1,3]
            @test prices(fp) == [9,4,0,0]
            @test flows(fp) == lp.flows

            @test primalobjective(fp) == dualobjective(fp)

        end

        @testset "Ahuja, Magnanti, and Orlin page 421" begin

            fp = FlowProblem([1,1,2,2,2,3,5,4,5], [2,3,3,4,5,5,4,6,6],
                             [8,3,3,7,2,3,4,5,6], [3,2,2,5,2,4,5,3,4],
                             [9,0,0,0,0,-9])
            @test MinCostFlows.complementaryslackness(fp) # Initialization should satisfy CS

            lp = linprog(fp)
            solveflows!(fp)

            @test MinCostFlows.complementaryslackness(fp) # Solving should preserve CS
            @test flows(fp) == [6,3,0,4,2,3,0,4,5]
            @test flows(fp) == lp.flows

            @test primalobjective(fp) == dualobjective(fp)

        end

    end

    # Note that degeneracy can potentially prevent MinCostFlow results from
    # matching LP results exactly. Instead, we just check that the minimized
    # costs from each method match and that the MinCostFlow solution is in
    # fact feasible.

    @testset "Previously problematic problems" begin

        fp = FlowProblem(
            [1, 1, 2, 2, 3, 3, 1, 2, 3, 4, 5, 6,
             4, 5, 6, 1, 2, 3, 10, 10, 10, 10, 10, 10],
            [2, 3, 3, 1, 1, 2, 10, 10, 10, 1, 2, 3,
             10, 10, 10, 7, 8, 9, 7, 8, 9, 1, 2, 3],
            [8, 8, 8, 8, 8, 8, 999999, 999999, 999999, 999999, 999999, 999999,
             999999, 999999, 999999, 999999, 999999, 999999,
             999999, 999999, 999999, 999999, 999999, 999999],
            [1, 1, 1, 1, 1, 1, 0, 0, 0, 10, 10, 10, 0, 0, 0, -9, -9, -9,
             0, 0, 0, 9999, 9999, 9999],
            [-9, 2, 19, 0, 0, 0, 0, 0, 0, -12]
        )

        @test MinCostFlows.complementaryslackness(fp) # Initialization should satisfy CS
        @test MinCostFlows.flowbalance(fp) # Flow accounting should be internally consistent

        lp = linprog(fp) # Will complain if infeasible
        solveflows!(fp)

        @test MinCostFlows.complementaryslackness(fp) # Solving should preserve CS
        @test MinCostFlows.flowbalance(fp) # Flow accounting should remain internally consistent
        @test primalobjective(fp) == dot(lp.flows, costs(fp))
        @test primalobjective(fp) == dualobjective(fp)
        @test buildAmatrix(fp) * flows(fp) == .-injections(fp)



        fp = FlowProblem(
            [32, 17, 37, 39, 20, 29, 3, 12, 1, 14, 33, 25, 12, 17, 1, 3, 18,
             38, 1, 32, 37, 15, 19, 39, 12, 1, 34, 28, 34, 5, 18, 15, 13, 9,
             12, 23, 32, 3, 16, 40, 17, 8, 27, 7, 9, 28, 18, 20, 4, 32, 8, 5,
             17, 21, 22, 4, 31, 11, 13, 4, 16, 9, 14, 4, 38, 22, 23, 40, 36,
             24, 22, 35, 12, 22, 38, 26, 38, 5, 13, 21, 3, 25, 18, 17, 1, 25,
             13, 38, 12, 12, 8, 34, 7, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
             13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28,
             29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 41, 41, 41, 41,
             41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41,
             41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41,
             41],
            [16, 1, 10, 35, 8, 32, 39, 5, 28, 34, 35, 29, 27, 35, 38, 28, 40,
             37, 14, 31, 11, 25, 33, 37, 34, 18, 6, 33, 19, 13, 27, 7, 21, 35,
             16, 20, 25, 38, 37, 15, 19, 40, 7, 33, 31, 35, 34, 3, 16, 39, 32,
             38, 6, 32, 39, 2, 21, 2, 32, 40, 14, 22, 16, 10, 15, 21, 10, 9, 18,
             14, 14, 14, 29, 10, 19, 39, 40, 10, 17, 22, 13, 40, 14, 29, 12, 9,
             40, 14, 1, 32, 33, 25, 14, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41,
             41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41,
             41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 1, 2, 3,
             4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
             22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37,
             38, 39, 40],
            [17, 11, 18, 10, 1, 7, 3, 13, 3, 3, 12, 11, 16, 5, 5, 9, 3, 1, 5,
             8, 3, 19, 17, 2, 16, 7, 16, 12, 18, 2, 11, 16, 11, 16, 7, 7, 15,
             7, 1, 8, 8, 4, 12, 17, 20, 2, 11, 20, 19, 8, 18, 7, 3, 16, 12, 9,
             10, 16, 18, 16, 11, 7, 8, 8, 1, 8, 20, 10, 6, 4, 1, 2, 5, 2, 13,
             15, 5, 1, 8, 4, 9, 14, 13, 15, 1, 4, 7, 13, 4, 17, 7, 11, 7, 9999,
             9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999,
             9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999,
             9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999,
             9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999,
             9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999,
             9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999,
             9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999,
             9999, 9999],
            [4, 4, 3, 2, 5, 5, 3, 1, 5, 5, 2, 2, 3, 4, 1, 3, 5, 2, 5, 5, 4, 5,
             1, 1, 5, 3, 4, 3, 5, 4, 5, 5, 4, 3, 3, 3, 3, 1, 5, 1, 2, 2, 3, 4,
             5, 4, 5, 2, 4, 2, 1, 3, 2, 2, 1, 1, 4, 5, 1, 2, 5, 2, 3, 1, 3, 5,
             3, 5, 2, 3, 5, 2, 4, 5, 3, 3, 3, 2, 5, 1, 3, 5, 5, 1, 5, 4, 5, 2,
             3, 2, 4, 5, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
             0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
             0, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999,
             9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999,
             9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999,
             9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999],
            [-12, 2, 4, 8, -11, 20, -9, -15, -5, -13, 17, -19, -7, 3, -6, 16,
             19, -4, -16, 2, 12, 5, 5, 9, 5, 3, -2, 14, 6, -10, -15, 3, -13,
             -13, 9, 10, -1, 17, 12, -4, -26]
        )

        @test MinCostFlows.complementaryslackness(fp) # Initialization should satisfy CS
        @test MinCostFlows.flowbalance(fp)

        lp = linprog(fp) # Will complain if infeasible
        solveflows!(fp)

        @test MinCostFlows.complementaryslackness(fp) # Solving should preserve CS
        @test MinCostFlows.flowbalance(fp)
        @test primalobjective(fp) == dot(lp.flows, costs(fp))
        @test primalobjective(fp) == dualobjective(fp)
        @test buildAmatrix(fp) * flows(fp) == .-injections(fp)



        fp = FlowProblem(
            [7, 5, 23, 36, 14, 2, 27, 6, 4, 1, 15, 27, 37, 11, 4, 21, 18, 37,
             30, 27, 5, 10, 10, 16, 20, 16, 24, 31, 8, 24, 35, 15, 20, 30, 35,
             29, 5, 18, 7, 23, 38, 22, 30, 36, 11, 3, 40, 36, 14, 39, 36, 24,
             19, 27, 3, 35, 30, 8, 36, 32, 7, 15, 9, 31, 7, 19, 28, 16, 22, 11,
             18, 2, 20, 3, 20, 1, 4, 25, 25, 3, 38, 18, 27, 24, 10, 19, 24, 23,
             12, 5, 16, 7, 4, 36, 38, 32, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
             12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27,
             28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 41, 41,
             41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41,
             41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41,
             41, 41, 41, 41, 41],
            [27, 1, 14, 6, 24, 20, 11, 18, 1, 8, 30, 5, 10, 13, 17, 15, 20, 36,
             7, 26, 29, 22, 36, 6, 31, 22, 17, 22, 37, 21, 2, 22, 24, 5, 30, 4,
             17, 6, 33, 27, 25, 14, 2, 26, 16, 30, 15, 35, 8, 34, 13, 36, 23,
             40, 14, 9, 15, 31, 37, 8, 1, 19, 38, 3, 34, 39, 2, 13, 38, 3, 16,
             6, 18, 32, 7, 38, 5, 10, 5, 31, 30, 7, 12, 3, 16, 5, 15, 4, 9, 36,
             33, 10, 13, 28, 39, 20, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41,
             41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41,
             41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 1, 2, 3,
             4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
             22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37,
             38, 39, 40],
            [8, 4, 13, 9, 1, 18, 9, 8, 16, 12, 3, 2, 18, 12, 18, 8, 11, 14, 4,
             16, 11, 19, 14, 20, 3, 19, 5, 14, 7, 11, 18, 9, 6, 2, 19, 5, 14,
             2, 3, 6, 15, 2, 11, 16, 15, 11, 7, 14, 6, 9, 2, 3, 5, 6, 17, 10,
             8, 8, 4, 7, 13, 10, 18, 17, 12, 10, 7, 14, 17, 7, 10, 16, 17, 3,
             18, 19, 4, 2, 13, 19, 5, 8, 5, 17, 15, 16, 12, 1, 1, 19, 12, 10,
             5, 20, 5, 3, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999,
             9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999,
             9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999,
             9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999,
             9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999,
             9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999,
             9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999,
             9999, 9999, 9999, 9999, 9999],
            [5, 4, 3, 4, 1, 4, 4, 4, 5, 5, 2, 3, 2, 5, 3, 5, 4, 3, 1, 4, 5, 4,
             4, 2, 2, 3, 2, 2, 3, 1, 3, 2, 3, 2, 3, 4, 1, 4, 4, 1, 1, 1, 5, 2,
             2, 4, 4, 2, 4, 2, 1, 3, 2, 3, 4, 2, 3, 3, 1, 5, 4, 1, 4, 1, 3, 3,
             4, 1, 4, 4, 3, 5, 3, 2, 5, 3, 5, 1, 1, 4, 3, 1, 3, 4, 3, 1, 2, 1,
             2, 5, 2, 4, 3, 4, 2, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
             0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
             0, 0, 0, 0, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999,
             9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999,
             9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999,
             9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999],
            [-3, 5, 13, -10, -19, -17, -11, 3, -13, 1, 3, -18, -6, -15, -19, 20,
             1, -15, 11, -10, 7, -1, -18, 15, 1, -14, -19, 10, -6, -8, 3, 16,
             19, -10, -15, -10, 15, 12, 16, -18, 104])

        @test MinCostFlows.complementaryslackness(fp) # Initialization should satisfy CS
        @test MinCostFlows.flowbalance(fp)

        lp = linprog(fp) # Will complain if infeasible
        solveflows!(fp)

        @test MinCostFlows.complementaryslackness(fp) # Solving should preserve CS
        @test MinCostFlows.flowbalance(fp)
        @test primalobjective(fp) == dot(lp.flows, costs(fp))
        @test primalobjective(fp) == dualobjective(fp)
        @test buildAmatrix(fp) * flows(fp) == .-injections(fp)



        fp = FlowProblem([4,3,4,1,1,2,3,4,5,5,5,5], [1,1,2,4,5,5,5,5,1,2,3,4],
                         [19,9,3,9,9999,9999,9999,9999,9999,9999,9999,9999],
                         [1,1,5,2,0,0,0,0,9999,9999,9999,9999], [-2,-7,4,-12,17])
        @test MinCostFlows.complementaryslackness(fp) # Initialization should satisfy CS
        @test MinCostFlows.flowbalance(fp)

        lp = linprog(fp) # Will complain if infeasible
        solveflows!(fp)

        @test MinCostFlows.complementaryslackness(fp) # Solving should preserve CS
        @test MinCostFlows.flowbalance(fp)
        @test primalobjective(fp) == dot(lp.flows, costs(fp))
            @test primalobjective(fp) == dualobjective(fp)
        @test buildAmatrix(fp) * flows(fp) == .-injections(fp)

    end

    @testset "Infeasible Problems" begin

        fp = FlowProblem([1,2,2,1,4,4], [4,1,4,3,3,1], [0,10,0,-9,0,9999],
                         [999999,999999,999999,999999,999999,999999],
                         [-11, 0, 1, 10])

        @test_throws ErrorException solveflows!(fp)

    end

    @testset "Random Networks" begin

        N, E = 200, 400

        for i in 1:1000

            fp = randomproblem(N, E)
            @test MinCostFlows.complementaryslackness(fp) # Initialization should satisfy CS
            @test MinCostFlows.flowbalance(fp)

            lp = linprog(fp) # Will complain if infeasible
            solveflows!(fp)

            @test MinCostFlows.complementaryslackness(fp) # Solving should preserve CS
            @test MinCostFlows.flowbalance(fp)
            @test primalobjective(fp) == dot(lp.flows, costs(fp))
            @test primalobjective(fp) == dualobjective(fp)
            @test buildAmatrix(fp) * flows(fp) == .-injections(fp)

        end

    end

    @testset "Random Hotstarts" begin

        N, E = 200, 400

        fp = randomproblem(N, E)
        @test MinCostFlows.complementaryslackness(fp) # Initialization should satisfy CS
        @test MinCostFlows.flowbalance(fp)

        lp = linprog(fp)
        solveflows!(fp, verbose=verbose)

        @test MinCostFlows.complementaryslackness(fp) # Solving should preserve CS
        @test MinCostFlows.flowbalance(fp)
        @test primalobjective(fp) == dot(lp.flows, costs(fp))
        @test primalobjective(fp) == dualobjective(fp)
        @test buildAmatrix(fp) * flows(fp) == .-injections(fp)

        # Randomly modify problem and re-solve
        for i in 1:1000

            # Update injections and rebalance at fallback node
            for n in 1:N
                updateinjection!(fp.nodes[n], fp.nodes[N+1],
                                 fp.nodes[n].injection + rand(-3:3))
            end
            @test MinCostFlows.complementaryslackness(fp)
            @test MinCostFlows.flowbalance(fp)

            # Update flow limits
            for e in 1:E
                updateflowlimit!(fp.edges[e], max(0, fp.edges[e].limit + rand(-3:3)))
            end
            @test MinCostFlows.complementaryslackness(fp)
            @test MinCostFlows.flowbalance(fp)

            # Update flow costs
            for e in 1:E
                updateflowcost!(fp.edges[e], fp.edges[e].cost + rand(-3:3))
            end
            @test MinCostFlows.complementaryslackness(fp)
            @test MinCostFlows.flowbalance(fp)

            # Re-solve
            lp = linprog(fp)
            solveflows!(fp, verbose=verbose)

            @test MinCostFlows.complementaryslackness(fp)
            @test MinCostFlows.flowbalance(fp)
            @test primalobjective(fp) == dot(lp.flows, costs(fp))
            @test primalobjective(fp) == dualobjective(fp)
            @test buildAmatrix(fp) * flows(fp) == .-injections(fp)

        end

    end

    if false

        Profile.init(delay=0.0001)
        Random.seed!(1234)
        @profile zeros(1)
        Profile.clear()
        N = 200; E = 400
        nresolves = 999
        println("n = $N, e = $E, $nresolves resolves")
        fp = randomproblem(N, E)
        @profile solveflows!(fp)

        for _ in 1:nresolves

            # Update injections and rebalance at fallback node
            for n in 1:N
                updateinjection!(fp.nodes[n], fp.nodes[N+1],
                                 fp.nodes[n].injection + rand(-3:3))
            end

            # Update flow limits
            for e in 1:E
                updateflowlimit!(fp.edges[e], max(0, fp.edges[e].limit + rand(-3:3)))
            end

            # Update flow costs
            for e in 1:E
                updateflowcost!(fp.edges[e], fp.edges[e].cost + rand(-3:3))
            end

            @profile solveflows!(fp)

        end

        Profile.print(maxdepth=14)

    end
    end
end

