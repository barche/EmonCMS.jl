# EmonCMS

Julia package to extract data from an [emonPi](https://guide.openenergymonitor.org/setup/install/) system and store it for offline processing.

## Setup

To set up a new database:

```julia
db = EmonDataSet("/path/to/existing/dir", "http://emonpi/emoncms/feed", "32-character-api-read-key")
update(db;feeds=[1,4,6,7,8,14,16,18,20,22,24,26])
```

Here, `emonpi` is the IP address of your emonpi, and the api key (read access is sufficient) is available at http://emonpi/emoncms/feed/api
The `feeds` argument to `update` is needed only during initial setup to specify the feed ids to download.

## Loading and updating

Once the database is initialized, it can be loaded using:

```julia
db = EmonDataSet("/path/to/existing/dir")
```

To append the latest data to this database, use `update(db)`.

Getting the daily energy usage of a feed named `HeatPump`:

```julia
heatpumpfeed = getfeed(db, "HeadPump")
energytable = energyperperiod(heatpumpfeed, Day(1))
```

Both `heatpumpfeed` and `energytable` are [JuliaDB](https://juliadb.juliadata.org) tables, available for further processing in e.g. a [Pluto](https://github.com/fonsp/Pluto.jl) notebook.
