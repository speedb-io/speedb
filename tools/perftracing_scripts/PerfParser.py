from array import array
import json
import zlib

class Marker:
    def __init__(self, raw_marker:str) -> None:
        self.isValid = True
        self.isFinished = False
        self.dic_ = {}
        splitted_ = raw_marker.split(',')
        if len(splitted_) < 4:
            self.isValid = False
        try:
            self.tid = splitted_.pop(0)
            self.end_ts = int(splitted_.pop(-1))
            self.start_ts = int(splitted_.pop(-1))

            self.name = ",".join(splitted_)
            self.isFinished = self.end_ts > 0
        except:
            print(f"exception in line: {raw_marker} length:{len(raw_marker)}")
        

        if self.isFinished:
            self.dic_={
                "name": self.name,
                "tid":self.tid,
                "ph": "X",
                "ts": self.start_ts,
                "dur" : self.end_ts - self.start_ts,
                "pid":100                # pid is not interesting at the moment, but required by chrome://tracing
            }
        else:
            self.dic_={
                "name": self.name,
                "tid":self.tid,
                "ph": "B",
                "ts": self.start_ts,
                "pid":100                # pid is not interesting at the moment, but required by chrome://tracing
            }
        
    
    def toChromeStr(self) -> str:
        ret_str = json.dumps(self.dic_)
        return ret_str

class PerfParser:
    def __init__(self, input_files:array):
        self.markers = []
        self.input_files = input_files
        self.currentFile = None
        self.CurrentDecompressionObject = zlib.decompressobj()
        self.NotCompressed = False


    def getMarkers(self) -> Marker:
        for file_ in self.input_files:
            self.currentFile = open(file_, "rb")
            list_of_data_strings = []
            #                                                        # bytes     # compressed bytes     # string         #list of strings
            try:
                list_of_data_strings =  self.CurrentDecompressionObject.decompress(self.currentFile.read()).decode("utf-8").split('\n')
            except Exception as exc:
                self.NotCompressed = True
                self.currentFile.close()
                self.currentFile = open(file_, "r")
                list_of_data_strings = self.currentFile.readlines()
                self.currentFile.close()
            for line in list_of_data_strings:
                if len(line) == 0:
                    continue
                yield Marker(line)
            while self.CurrentDecompressionObject.unused_data and not self.NotCompressed:
                unused_data = self.CurrentDecompressionObject.unused_data
                self.CurrentDecompressionObject =zlib.decompressobj()
                list_of_data_strings =  self.CurrentDecompressionObject.decompress(unused_data).decode("utf-8").split('\n')
                for line in list_of_data_strings:
                    if len(line) == 0:
                        continue
                    yield Marker(line)
            self.currentFile.close()    

