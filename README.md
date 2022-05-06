# pg_happy

pg_happy is a simple test application for HA setup of PostgreSQL

## Purpose

When testing high availability tools for PostgreSQL, we need an application to
simulate client activity.

The idea is to send data to postgres a regular intervals, log this data locally
so that we can compare afterwards if there were some data lost while simulating
failovers.

## Install

The simplest way to install is to use go get:

```
go get github.com/orgrim/pg_happy
```

## Usage

There are multiple modes of operation:

* `init`: prepare the database to receive our test data
* `load`: send data to PostgreSQL regularly and log it locally
* `compare`: check if everything is in the database

To connect to PostgreSQL, use the `-d` option with an URL or DSN. The usual
`PG*` environment variables are recognized.

See the `--help` for more information.

### init

The `init` command connects to PostgreSQL and create the `happy` schema, the
`happy.stamps` table aimed at storing data, and the `happy.store` unlogged
table used for comparison with the local file.

### load

The `load` command inserts a line in `happy.stamps` and logs it in the local
file (in JSON format), at regular intervals.

### compare

The `compare` command loads the local file in `happy.store` and outputs the
differences with the `happy.stamps` table.

## Contributing

Feel free to send patches, PR, open issues.

## License

This tool is distributed under the BSD 2-Clause License, see the LICENSE file.
