# PERF TRACING

Perf tracing is a method for high resolution profiling, debugging and analysing execution flows.

## Setup

The following scripts are a tailor-fit parsing for SpeeDB perf files which are ZLIB compressed files that by default will be written to ```/tmp/perfdata```. 
-------------------------------------------------------------------------------------------------------------------------------------------------------------------

## Usage - Chrome Tracing
```chrome_tracing.py``` - A script that given an array (-f ) of Perf files (space seperated) will produce Json files that could be loaded by ```chrome://tracing``` tool. (built in tool inside every chrome browser, just navigate to ```chrome://tracing``` in the chrome browser and load the Json file with the ```load``` button.
#### usage example: 
```bash
python3 chrome_tracing.py -f /tmp/perfdata/perf1 /tmp/perfdata/perf2
 ```
-------------------------------------------------------------------------------------------------------------------------------------------------------------------

 ## Perf Parser
PerfParser.py module is a helper module that parsing the compressed perf files produced by SpeeDB.
The usage is as easy as initiating the class with an Array of perf file paths and Calling ```getMarkers()``` method to receive a Marker generator, where Marker is an object representing a single function call.
#### usage example:
```python
from PerfParser import Marker,PerfParser

parser = PefParser([file1, file2])
markers = parser.getMarkers()
for marker in markers:
   # do something with the Marker

```

### Marker
 A class representing a single function execution on the execution timeline
        
        Attributes
        ----------
        tid : int
            Thread ID on which this function was executing
        start_ts : int
            Function start Timestamp 
        end_ts : int
            Function end Timestamp
        name : str
            Function name
        isFinished: boolean
            True - function Finished (had end time stamp)
            False - function did not finish or (end time stamp does not exist)
-------------------------------------------------------------------------------------------------------------------------------------------------------------------

 ## addMarkers
A Helper script that adds markers in the actual code (.cc files)
**This script is does not guarantee compilable code after usage, some manual editting might be required.**
usage:
```bash
python3 addMarkers.py file.cc
python3 addMarkers.py directory_containing_cc_files
```