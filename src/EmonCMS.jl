module EmonCMS

export EmonDataSet
export update
export energyperperiod
export feedlist
export getfeed
export yearlyaverage
export energysummary
export exportcsv

using Unitful
import HTTP
using URIs
import JSON3
using JuliaDB
using Dates
using ProgressMeter
using Trapz
using DelimitedFiles

struct Connection
  serveraddress::URI
  apikey::String
end
Connection(address::String, apikey::String) = Connection(URI(address), String(apikey))

JSON3.StructTypes.StructType(::Type{Connection}) = JSON3.StructTypes.Struct()
JSON3.StructTypes.StructType(::Type{URI}) = JSON3.StructTypes.Struct()

headers(c::Connection) = Dict("Authorization" => c.apikey)

function getjson(c::Connection, page, parameters=Dict())
  uri = URI(joinpath(c.serveraddress, page); query=parameters)
  response = HTTP.get(uri, headers(c))
  if isempty(response.body)
    return []
  end
  result_json = JSON3.read(response.body)
  if "success" ∈ keys(result_json) && !result_json["success"]
    throw(ErrorException("Request $uri failed with error: $(result_json["message"])"))
  end
  return result_json
end

list(c::Connection) = getjson(c, "list.json")
aget(c::Connection, id) = getjson(c, "aget.json", Dict("id" => id))
getmeta(c::Connection, id) = getjson(c, "getmeta.json", Dict("id" => id))

emontimemilli(dt::DateTime) = Int(floor(datetime2unix(dt)*1000))

const maxnbjsonentries = 8928

maxtimestep(interval) = maxnbjsonentries*1000*interval

"""
Get data from a feed. first and last are in seconds here, and converted to the required miliseconds. Interval is also in seconds
"""
function data(c::Connection, id, first, last, interval)
  # Times in seconds
  parameters = Dict("id" => id, "start" => first*1000, "end" => last*1000, "interval" => interval)
  return getjson(c, "data.json", parameters)
end

connectionfile(base) = joinpath(base,"connection.json")
feedsfile(base) = joinpath(base,"feeds"*dbextension)


"""
  EmonDataSet(path)

Load the dataset at the given path

  EmonDataSet(path, serveraddress, apikey)

Construct a new dataset in the existing directory at `path`, connecting to `serveraddress` using read-only EmonCMS API key `apikey`
"""
struct EmonDataSet
  path::String
  connection::Connection

  function EmonDataSet(path)
    if !isfile(connectionfile(path))
      throw(ErrorException("No existing database at \"$path\". Please create a new one using EmonDataSet(path, serveraddress, apikey)"))
    end
    connjson = JSON3.read(read(connectionfile(path), String))
    return new(path, Connection(connjson["serveraddress"]["uri"],connjson["apikey"]))
  end

  function EmonDataSet(path, serveraddress, apikey)
    if !isdir(path)
      throw(ErrorException("\"$path\" is not a directory, please create it first."))
    end
    return new(path, Connection(serveraddress, apikey))
  end
end

const dbextension = ".juliadb"

createfeedtable(t::AbstractVector{Int64}=Int64[], values::AbstractVector{Union{Missing,Float64}}=Vector{Union{Missing,Float64}}()) = table((time=t, value=values), pkey=:time)

toseconds(n)::Int64 = n ÷ 1000

function appendblock(feedtable, block, interval, blockstart, blockend)
  if isempty(block)
    return feedtable
  end

  blockfirst = toseconds(block[1][1])
  blocklast = toseconds(block[end][1])
  if (blocklast - blockfirst) % interval != 0
    throw(ErrorException("Block with starttime $starttime and endtime $endtime does not match interval $interval"))
  end
  if !isempty(feedtable) && ((blockfirst - feedtable[end][:time]) % interval) != 0
    throw(ErrorException("Feed timing does not match stored table"))
  end

  starttime = isempty(feedtable) ? blockfirst : feedtable[end][:time]+interval
  times = starttime:interval:blocklast
  values = Array{Union{Missing, Float64}}(missing, length(times))
  function time_to_index(t)
    @assert (t-starttime) % interval == 0
    return (t-starttime) ÷ interval + 1
  end
  for (t,v) in block
    t = toseconds(t)
    if t < blockstart || t > blockend || t < starttime
      continue
    end
    i = time_to_index(t)
    values[i] = v
  end

  return merge(feedtable, createfeedtable(times,values))
end

function updatefeedtable(getter::Function, conn, feedtable, feedid, feedstart, feedend, interval, name)
  starttime = (isempty(feedtable) ? feedstart : (feedtable[end][:time] + interval))::Int64
  nsteps = (feedend - (starttime-interval)) ÷ interval
  nblocks = Int(ceil(nsteps / maxnbjsonentries))
  @showprogress 0.5 "Appending $nblocks blocks to feed $name..." for blockidx in 1:nblocks
    blockstart = starttime + (blockidx - 1)*maxnbjsonentries*interval
    blockend = blockstart + (maxnbjsonentries-1)*interval
    try
      block = getter(conn, feedid, blockstart, blockend, interval)
      feedtable = appendblock(feedtable, block, interval, blockstart, blockend)
    catch e
      @warn "Aborting feed update with error $e"
      break
    end
  end
  @info "Appended $nsteps entries to feed $name"

  return feedtable
end

function writeconnfile(connection, filename)
  @info "Creating new database at \"$(dirname(filename))\""

  open(filename, "w") do filestream
    JSON3.pretty(filestream, JSON3.write(connection))
    println(filestream)
  end
end
"""
  update(ds)

Update the EmonDataSet `ds` with the latest available data on the emonPi.

  update(ds; feeds=[id1, id2, ...])

First call to update on EmonDataSet `ds` , with a list of integer feed IDs to store in the dataset.
"""
function update(ds::EmonDataSet; feeds=[])
  connfile = connectionfile(ds.path)
  if !isfile(connfile)
    writeconnfile(ds.connection, connfile)
  end

  conn = ds.connection

  feedlistfile = feedsfile(ds.path)
  if !isfile(feedlistfile)
    if isempty(feeds)
      throw(ErrorException("No existing feeds list found, and no feeds given. Call update with keyword argument \"feeds=[id1, if2, ...]\""))
    end
    @info "Creating new feeds list"

    nfeeds = length(feeds)
    feedstuple = (id=feeds, unit=fill("W", nfeeds), name=String[], starttime=Int64[], interval=Int64[])
    for id in feeds
      feedinfo = aget(conn, id)
      feedmeta = getmeta(conn, id)
      push!(feedstuple.name, feedinfo["name"])
      push!(feedstuple.starttime, feedmeta["start_time"]) # start_time in seconds
      push!(feedstuple.interval, feedmeta["interval"])
    end

    feedstable = table(feedstuple, pkey=[:id, :name])
    save(feedstable, feedlistfile)
  else
    if !isempty(feeds)
      throw(ErrorException("Feeds table exists, can't add new feeds. Call update without feed list or create a new database"))
    end
    feedstable = load(feedlistfile)
  end

  for fd in feedstable
    feedinfo = aget(conn, fd.id)
    feedfile = joinpath(ds.path, fd.name*dbextension)
    feedtable = isfile(feedfile) ? load(feedfile) : createfeedtable()
    updatedfeedtable = updatefeedtable(data, conn, feedtable, fd.id, fd.starttime, feedinfo["time"], fd.interval, fd.name)
    save(updatedfeedtable, feedfile)
  end
  return
end

"""
  feedlist(ds)

Get the table of stored feeds in `ds`.
"""
feedlist(ds) = load(feedsfile(ds.path))

loadfeed(ds,name) = load(joinpath(ds.path, name*dbextension))

"""
  getfeed(ds, name)

Get the values of a given feed. The result is a table with columns `time` and `value`.
"""
function getfeed(ds::EmonDataSet, name)
  feedtable = loadfeed(ds, name)
  unit = filter(i -> i.name == name, feedlist(ds))[1][:unit]
  return table((time= unix2datetime.(select(feedtable, :time)), value=select(feedtable, :value) .* uparse(unit)), pkey=:time)
end

function _replace_missing_limited(values, allowedmissing)
  nbmissing = count(ismissing, values)
  if nbmissing != 0 && nbmissing/length(values) < allowedmissing
    return replace(values, missing => zero(eltype(values)))
  end

  return values
end

"""
  energyperperiod(feedtable, period)

For the power data in `feedtable`, calculate the energy per given periods, returning the result as a table with columns `date` and `energy`

Example:

To get the monthly totals of feed `HeatPump` do:
```
feedtable = getfeed(ds, "HeatPump")
energyperperiod(feedtable, Month(1))
```

`Month` comes from the `Dates` package.

* The keyword argument `energyunit` allows you to set the unit of the output (`kW hr` by default)
* The keyword argument `allowedmissing` indicates the fraction of missing values that is allowed to be replaced by zero. If this fraction is exceeded for a given period, the period value will be `missing`.

"""
function energyperperiod(feedtable, period; energyunit = u"kW*hr", allowedmissing = 0.1)
  pertype = typeof(period)
  
  times = datetime2unix.(select(feedtable, :time))
  values = ustrip.(u"W", select(feedtable, :value))

  starttime = floor(unix2datetime(times[1]), pertype)
  endtime = floor(unix2datetime(times[end]), pertype)
  daterange = starttime:period:endtime
  energyvalues = zeros(Union{Float64,Missing}, length(daterange))
  for (i,p) in enumerate(daterange)
    periodstart = (datetime2unix(p) |> Int64)
    periodend = (datetime2unix(p + period) |> Int64)
    firstidx = max(searchsortedlast(times, periodstart),1)
    lastidx = min(searchsortedfirst(times, periodend), length(times))
    energyvalues[i] = trapz(times[firstidx:lastidx], _replace_missing_limited(values[firstidx:lastidx], allowedmissing))
  end

  return table((date=daterange, energy=((energyvalues .* u"J") .|> energyunit)), pkey=:date)
end

"""
  yearlyaverage(feed, period, years)

Return the average energy used for each period (e.g. monthly) over the given range of years, for the given feed.

The returned value is a tuple containing the averaged values and the number of times this period was counted.
"""
function yearlyaverage(feed, period, years=[2018,2019,2020])
	nbperiods = length(DateTime(2017):period:DateTime(2018))-1
	result = fill(0.0*u"kW*hr", nbperiods)
	counts = zeros(Int, nbperiods)
	for y in years
		yearintegral = energyperperiod(filter(f -> (year(f.time) == y && !(month(f.time) == 2 && day(f.time) == 29)), feed),period)
		values = select(yearintegral, :energy)
		for i in eachindex(skipmissing(values))
      if i > nbperiods
        break
      end
			counts[i] += 1
			result[i] += values[i]
		end
	end
	return replace(result ./ counts, NaN*u"kW*hr" => missing), counts
end

"""
  energysummary(ds)

Summarize energy usage over a number of years for a set of feeds, adding a feed named "Unknown" by subtracting all other feeds from a set of feeds that give the total power.

Example:

```
names, energies, counts = energysummary(ds; years=[2018,2019,2020], totalpowerfeeds = ["L1"])
```

The `years` argument lists the years over which to average, the `totalpowerfeeds` argument gives a list of feeds that when added together give the total power (e.g. measurements on each incoming phase).

This returns:
* `names`: The names of the considered feeds (all feeds by default, or the names in the array passed to the `feeds` keyword argument)
* `energies`: 2D array, with each column `i` the averaged energy for feed `i` in `names`. Rows are the period numbers (months by default, change with keyword argument `period`)
* `counts`: Number of times each period was non-missing for each feed.
"""
function energysummary(ds; period=Month(1), years=[2018,2019,2020], totalpowerfeeds = ["L1_in", "L2_in", "L3_in"], feeds=select(feedlist(ds),:name))
  # Collect averages per period for each feed
  energydict = Dict()
	for feedname in feeds
		energies,counts = yearlyaverage(getfeed(ds, feedname), period, years)
		energydict[feedname] = (energies=energies,counts=counts)
	end
	
  # Compute the total energy
	totalenergy = energydict[totalpowerfeeds[1]].energies
  for feedname in totalpowerfeeds[2:end]
    totalenergy .+=  energydict[feedname].energies
  end

  # Store the energy per period in a 2D array
	nbpers = length(totalenergy)
	wantedkeys = setdiff(feeds, totalpowerfeeds)
	energies = zeros(Union{Missing,typeof(0.0u"kW*hr")},nbpers,length(wantedkeys)+1)
	counts = zeros(Int, size(energies))
	for (i,name) in enumerate(wantedkeys)
		(h,c) = energydict[name]
		energies[:,i+1] .= h
		counts[:,i+1] .= c
	end
	energies[:,1] .= totalenergy .- reshape(sum(energies[:,2:end];dims=2),nbpers)
	counts[:,1] .= energydict[totalpowerfeeds[1]].counts
	pushfirst!(wantedkeys, "Unknown")
	return reshape(wantedkeys,1,length(wantedkeys)), energies, counts
end

function exporttable(filename, table)
  if isfile(filename)
    throw(ErrorException("Refusing to overwrite $filename"))
  end
  open(filename,"w") do f
    println(f,join(colnames(table),','))
    writedlm(f,table,',')
  end
end

function importfeed(filename)
  return loadtable(filename; nastrings=["missing"], indexcols=[:time])
end

"""
  exportcsv(ds, path)

Export all tables as CSV files in directory `path`
"""
function exportcsv(ds, path)
  if !isdir(path)
    throw(ErrorException("Export path $path must be an existing directory"))
  end
  exporttable(joinpath(path, "feedlist.csv"), feedlist(ds))
  for feedname in select(feedlist(ds),:name)
    exporttable(joinpath(path, "$feedname.csv"), loadfeed(ds, feedname))
  end
end

end # module
