# Speedb: A drop in replacement embedded solution for RocksDB

## Checking out the source

	git clone https://github.com/speedb-io/speedb.git

## Dynamically linking Speedb
If speedb is in your default library path:

In your `CMakeLists.txt` add:

	target_link_libraries(${PROJECT_NAME} speedb)
where `PROJECT_NAME` is the name of your target application which uses speedb

Otherwise, you have to include the path to the folder the library is in like so:
	
	target_link_libraries(${PROJECT_NAME} /path/to/speedb/library/folder)

## Usage
Usage of the library in your code is the same, regardless of whether you statically linked the library or dynamically linked it, and examples can be found under the [examples](examples) directory.
The public interface is in [include](include/rocksdb). Callers should not include or rely on the details of any other header files in this package. Those internal APIs may be changed without warning.

## Build dependencies
Please refer to the file [INSTALL.md](INSTALL.md) for a list of all the dependencies and how to install them across different platforms.

## Building Speedb
Debug:

	mkdir build && cd build   
	cmake .. -DCMAKE_BUILD_TYPE=Debug [cmake options]
	make rocksdb

By default the build type is Debug.

Release:

	mkdir build && cd build   
	cmake .. -DCMAKE_BUILD_TYPE=Release [cmake options]
	make rocksdb

This will build the static library.
If you want to build the dynammic library, use:

	make rocksdb-shared

If you want `make` to increase the number of cores used for building, simply use the `-j` option.

If you want to build a specific target:

	make [target name]

For development and functional testing, go with the debug version which includes
more assertions and debug prints.
Otherwise, for production or performance testing, we recommend building a release version
which is more optimized.

## Running tests

	cd build   
	make rocksdb
	./[test name] --db=/path/to/db

for example, `db_blob_basic_test`:

	cd build   
	make rocksdb
	./db_blob_basic_test --db=/tmp/example_db

The test will generate a random DB at the specified path. This is also where speedb will store the LOG files.

## Contributing code
`TODO:` This section should point the reader to a dedicated file explaining how to contribute

## License
Speedb is licensed under Apache 2.0