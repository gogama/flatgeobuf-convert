# flatgeobuf-convert

Conversion library for [flatgeobuf](https://github.com/gogama/flatgeobuf) that
makes it easy to convert between FlatGeobuf's low-level Flatbuffer types and
usable high-level Go types like [Orb](https://github.com/paulmach/orb) features.

## Problem Solved 

The [FlatGeobuf](https://flatgeobuf.org/) file format is a powerful system for
storing, indexing, and searching huge amounts of geometric/geospatial data.

The problem is that FlatGeobuf is optimized for efficiency and most of its
core types (`Header`, `Feature`, `Geometry`, *etc.* and feature property buffers)
are complex and time-consuming to program against.

This library solves the problem by providing higher-level types and simple,
elegant, native Go conversions from the core types. Importantly, it provides
trivial conversions from FlatGeobuf features to the feature types of popular
geospatial Go libraries, *e.g.* Orb's `orb.Geometry` and `geojson.Feature`
types.

## Getting Started

todo.

## Compatibility

Works with all Go versions 1.20 and up.

## Package Map

todo.

## License

This project is licensed under the terms of the MIT License.

## Acknowledgements

todo.
