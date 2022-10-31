import os,sys
import argparse
from PerfParser import Marker, PerfParser


def export_to_chrome_tracing_file(perf_file):
    c_parser = PerfParser([perf_file])
    index = 0
    out_f = open(perf_file+"_"+str(index)+".json","wt")
    markers = c_parser.getMarkers()
    out_f.writelines("[\n")
    for marker in markers:
        out_f.writelines(marker.toChromeStr()+",")
        if out_f.tell() > 500e6: # max size that chrome://tracing will accept
            index += 1
            out_f.seek(0, os.SEEK_END)
            out_f.seek(out_f.tell()-1, os.SEEK_SET)
            out_f.truncate()
            out_f.writelines("]\n")
            out_f.close()
            out_f = open(perf_file+"_"+str(index)+".json", "wt")
            out_f.writelines("[\n")
    out_f.seek(0, os.SEEK_END)
    out_f.seek(out_f.tell()-1, os.SEEK_SET)
    out_f.truncate()
    out_f.writelines("]\n")
    out_f.close()


def main(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', nargs='+', help='input files', required=False)
    parser.add_argument('-o', help='outputfile', required=False)
    parser.add_argument('-t', help='threshold for minimum call duration in ms', required=False, default=0)
    parser.add_argument('-d', help='parse whole directory', required=False)
    args = parser.parse_args()

    print(f"files: {args.f}")

    for perf_f in args.f:
        export_to_chrome_tracing_file(perf_f)
    




if __name__ == "__main__":
    main(sys.argv)