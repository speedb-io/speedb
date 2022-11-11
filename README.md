<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset=".github/speedb-logo-dark.gif" width="480px" >
    <img src=".github/speedb-logo.gif" width="480px">
  </picture>
</div>

<div align="center">

![GitHub](https://img.shields.io/github/license/speedb-io/speedb)
![GitHub contributors](https://img.shields.io/github/contributors/speedb-io/speedb?color=blue)
![GitHub pull requests](https://img.shields.io/github/issues-pr/speedb-io/speedb)
![GitHub closed pull requests](https://img.shields.io/github/issues-pr-closed/speedb-io/speedb?color=green)
</div>

# Speedb
Speedb is a library that provides a high performance, persistent key-value store. 
This is a community led project that focuses on high performance, optimized for modern storage hardware, on-premise and in the cloud.
It is maintained by Speedb and is forked from Rocksdb, developed by Facebook.

**Join us to build the next generation data engine!**

<picture>
  <source media="(prefers-color-scheme: dark)" srcset=".github/new-bee-mascot-dark.gif" width="80px" >
  <img src=".github/new-bee-mascot.gif" width="80px">
</picture>


## Usage
* If speedb is in your default library path:


  In your `CMakeLists.txt` add:
  ```
  target_link_libraries(${PROJECT_NAME} speedb)
  ```
  where `PROJECT_NAME` is the name of your target application which uses speedb

* Otherwise, you have to include the path to the folder the library is in like so:
	
  ```
  target_link_libraries(${PROJECT_NAME} /path/to/speedb/library/folder)
  ```


Usage of the library in your code is the same regardless of whether you statically linked the library or dynamically linked it, and examples can be found under the [examples](examples) directory.
The public interface is in [include](include/rocksdb). Callers should not include or rely on the details of any other header files in this package. Those internal APIs may be changed without warning.


## Build dependencies

Please refer to the file [INSTALL.md](INSTALL.md) for a list of all the
dependencies and how to install them across different platforms.


## 🔨 Building Speedb 

Debug:

    mkdir build && cd build
    cmake .. -DCMAKE_BUILD_TYPE=Debug [cmake options]
    make speedb

By default the build type is Debug.

Release:

    mkdir build && cd build
    cmake .. -DCMAKE_BUILD_TYPE=Release [cmake options]
    make speedb

This will build the static library. If you want to build the dynamic library,
use:

    make speedb-shared

If you want `make` to increase the number of cores used for building, simply use
the `-j` option.

If you want to build a specific target:

    make [target name]

For development and functional testing, go with the debug version which includes
more assertions and debug prints. Otherwise, for production or performance
testing, we recommend building a release version which is more optimized.

## 📈 Performance 

We are using DBbench to test performance and progress between the versions. It is available under tools and also in the artifact for direct download.
In there you can also find a readme with the commands we are using to get you started. 
Here is a graph comparing Speedb's paired bloom filter with the default bloom filter. 

<img width="769" alt="Screen Shot 2022-10-31 at 15 15 42" src="https://user-images.githubusercontent.com/107058910/199016617-4ecbe279-b281-4a8c-b630-87a509bc09da.png">

For additional info see: [Paired Bloom Filter](https://docs.speedb.dev/documentation/speedb-features/paired-bloom-filter).

## Documentation

You can find a detailed description of all Speedb features [here](https://speedb.gitbook.io/documentation/).

[Speedb's documentation repository](https://github.com/speedb-io/book) allows you to enhance, add content and fix issues. 

## 🛣️ Roadmap

The [product roadmap](https://github.com/orgs/speedb-io/projects/4/views/1) provides a snapshot of the features we are currently developing, what we are planning for the future, and the items that have already been delivered.

We have added a column with items that are awaiting community feedback. We invite you to participate in our polls inside, share your thoughts about topics that are important to you, and let us know if there is anything else you would like to see on the list.

## ❔ Questions 

- For live discussion with the community you can use our official [Discord channel](https://discord.gg/5fVUUtM2cG). 
- For technical questions and discussions you can use our official [Discourse forum](https://forum.speedb.dev). 


## 🌎 Join us 

Speedb is committed to a welcoming and inclusive environment where everyone can
contribute.


## Contributing code

See the [contributing guide](CONTRIBUTING.md).


## License
Speedb is open-source and licensed under the [Apache 2.0 License](LICENSE.Apache).


<img src=".github/speedb-b.gif" width="200px">
