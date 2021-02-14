module EmonCMS

export EmonDataSet
export update
export integrateperperiod
export feedlist
export getfeed

using Unitful
import HTTP
using URIs
import JSON3
using JuliaDB
using Dates
using ProgressMeter
using Trapz

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

function update(ds::EmonDataSet; endtime=now(), starttime=nothing, feeds=[])
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

feedlist(ds) = load(feedsfile(ds.path))
function getfeed(ds::EmonDataSet, name)
  feedtable = load(joinpath(ds.path, name*dbextension))
  unit = filter(i -> i.name == name, feedlist(ds))[1][:unit]
  return table((time=select(feedtable, :time) .* u"s", value=select(feedtable, :value) .* uparse(unit)), pkey=:time)
end

function integrateperperiod(feedtable, period, expectedunit = u"kW*hr")
  pertype = typeof(period)
  
  times = ustrip.(u"s", select(feedtable, :time))
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
    energyvalues[i] = trapz(times[firstidx:lastidx], values[firstidx:lastidx])
  end

  return table((dates=daterange, energy=((energyvalues .* u"J") .|> expectedunit)), pkey=:dates)
end

end # module
