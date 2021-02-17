using Test
using EmonCMS
using JuliaDB
using Dates
using Unitful

function maketestfeed(N, interval, t0)
  peak1 = 200.0
  peak2 = 300.0
  result = []
  Ni, Nj, Nk, Nl, Nm = sort(rand(1:N,5))
  t0 -= interval
  t0 *= 1000
  for (t,P) in zip(1:Ni, range(0,peak1;length=Ni))
    push!(result, Union{Int64,Float64}[t0+t*interval*1000,P])
  end
  for t in Nj:Nk
    push!(result, Union{Int64,Float64}[t0+t*interval*1000,peak2])
  end
  for t in Nl:Nm
    push!(result, Union{Int64,Float64}[t0+t*interval*1000,peak2])
  end

  Nmissing = Nj-Ni-1 + Nl-Nk-1

  return result, (Ni-1)*interval*peak1/2 + ((Nk-Nj) + (Nm-Nl))*interval*peak2, Nmissing
end

function getblock(::Any, feed, starttime, endtime, interval)
  starttime *= 1000
  endtime *= 1000
  tarray = getindex.(feed,1)
  startidx = searchsortedfirst(tarray,starttime)
  endidx = searchsortedlast(tarray,endtime)
  if endidx > length(tarray)
    return feed[startidx:end]
  end
  return feed[startidx:endidx]
end

# Set up a dummy database first
tdir = mktempdir()
EmonCMS.writeconnfile(EmonCMS.Connection("https://localhost", "dummykey"), EmonCMS.connectionfile(tdir))
emonds = EmonDataSet(tdir)

@testset "Feed setup" begin
  feedstuple = (id=[2,3], unit=["W", "W"], name=["TestFeed", "TotalPower"], starttime=[5000,5000], interval=[10,10])
  feedstable = table(feedstuple, pkey=[:id, :name])
  save(feedstable, EmonCMS.feedsfile(emonds.path))
end

@testset "Feed reading" begin
  N = 50000
  feeds = feedlist(emonds)
  interval = feeds[1][:interval]
  starttime = feeds[1][:starttime]
  feed, expectedenergy, expectednmissing = maketestfeed(N, interval, starttime)
  @test (feed[1][1] ÷ 1000) == starttime
  endtime = feed[end][1] ÷ 1000

  feedfile = joinpath(tdir, "TestFeed.juliadb")
  save(EmonCMS.updatefeedtable(getblock, nothing, EmonCMS.createfeedtable(), feed, starttime, endtime, interval, "TestFeed"), feedfile)
  feedtable = load(feedfile)
  save(feedtable, joinpath(tdir, "TotalPower.juliadb"))
  push!(feed, [(endtime+interval)*1000,10])
  feedtable2 = EmonCMS.updatefeedtable(getblock, nothing, feedtable, feed, starttime, endtime+interval, interval, "TestFeed")
  @test all(select(feedtable,:time) .== select(feedtable2,:time)[1:end-1])
  @test length(feedtable2) == length(feedtable) + 1
  testfeed = getfeed(emonds, "TestFeed")
  @test length(testfeed) == length(feed) + expectednmissing-1
  @test count(ismissing, select(testfeed,:value)) == expectednmissing
  energytable = energyperperiod(testfeed, Second(interval))
  energy = select(energytable, :energy)
  @test sum(skipmissing(energy)) ≈ (expectedenergy * u"J" |> u"kW*hr")
end

@testset "Postprocessing" begin
  names, energies, counts = energysummary(emonds; years=[1970], period=Day(1), totalpowerfeeds=["TotalPower", "TotalPower"])
  @test length(names) == 2
  @test names[2] == "TestFeed"
  @test names[1] == "Unknown"
  @test all(skipmissing(energies[:,1]) .== skipmissing(energies[:,2]))
end

@testset "Export" begin
  csvdir = mktempdir()
  exportcsv(emonds, csvdir)
  for tname in select(feedlist(emonds), :name)
    ref = EmonCMS.loadfeed(emonds,tname)
    imported = EmonCMS.importfeed(joinpath(csvdir, "$tname.csv"))
    @test all(select(ref,:time) .== select(imported,:time))
    @test all(skipmissing(select(ref,:value) .≈ select(imported,:value)))
    @test count(ismissing, select(ref,:value)) == count(ismissing, select(imported,:value))
  end
  @test feedlist(emonds) == loadtable(joinpath(csvdir,"feedlist.csv"))
end