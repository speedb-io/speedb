INT_NC = r'[\d]+'
INT = r"([\d]+)"
FLOAT = r"([\d]+\.[\d]+)"
UNIT = r'(KB|MB|GB|TB)'
EMPTY_LINE_REGEX = r'^\s*$'

TIMESTAMP_REGEX = r'\d{4}/\d{2}/\d{2}-\d{2}:\d{2}:\d{2}\.\d{6}'
FILE_PATH_REGEX = r"\/?.*?\.[\w:]+"

ORIG_TIME_REGEX = fr"\(Original Log Time ({TIMESTAMP_REGEX})\)"
CODE_POS_REGEX = fr"\[{FILE_PATH_REGEX}:\d+\]"

START_LINE_WITH_WARN_PARTS_REGEX = \
    fr"({TIMESTAMP_REGEX}) (\w+)\s*(?:{ORIG_TIME_REGEX})?\s*" \
    fr"\[(WARN|ERROR|FATAL)\]\s*({CODE_POS_REGEX})?(.*)"
START_LINE_PARTS_REGEX = \
    fr"({TIMESTAMP_REGEX}) (\w+)\s*" \
    fr"(?:{ORIG_TIME_REGEX})?\s*({CODE_POS_REGEX})?(.*)"

# <product name> version: <version number>
PRODUCT_AND_VERSION_REGEX = r"(\S+) version: ([0-9.]+)"

GIT_HASH_LINE_REGEX = r"Git sha \s*(\S+)"

JOB_REGEX = r"\[JOB [0-9]+\]"

OPTION_LINE_REGEX = r"\s*Options\.(\S+)\s*:\s*(.+)?"

# example:
# --------------- Options for column family [default]:
# In case of a match the result will be [<column-family name>] (List)
CF_OPTIONS_START_REGEX = \
    r"--------------- Options for column family \[(.*)\]:.*"

TABLE_OPTIONS_START_LINE_REGEX =\
    r"^\s*table_factory options:\s*(\S+)\s*:(.*)"
TABLE_OPTIONS_CONTINUATION_LINE_REGEX = r"^\s*(\S+)\s*:(.*)"

EVENT_REGEX = r"\s*EVENT_LOG_v1"

PREAMBLE_EVENT_REGEX = r"\[(.*?)\] \[JOB ([0-9]+)\]\s*(.*)"

STALLS_WARN_MSG_PREFIX = "Stalling writes"
STOPS_WARN_MSG_PREFIX = "Stopping writes"

#
# STATISTICS RELATED
#
DUMP_STATS_REGEX = r'------- DUMPING STATS -------'
DB_STATS_REGEX = r'^\s*\*\* DB Stats \*\*\s*$'
COMPACTION_STATS_REGEX = r'^\s*\*\* Compaction Stats\s*\[(.*)\]\s*\*\*\s*$'

FILE_READ_LATENCY_STATS_REGEX =\
    r'^\s*\*\* File Read Latency Histogram By Level\s*\[(.*)\]\s*\*\*\s*$'

LEVEL_READ_LATENCY_STATS_REGEX =\
    r'** Level ([0-9]+) read latency histogram (micros):'

STATS_COUNTERS_AND_HISTOGRAMS_REGEX = r'^\s*STATISTICS:\s*$'

UPTIME_STATS_LINE_REGEX =\
    r'^\s*Uptime\(secs\):\s*([0-9\.]+)\s*total,'\
    r'\s*([0-9\.]+)\s*interval\s*$'

BLOCK_CACHE_STATS_START_REGEX = \
    fr'Block cache (.*?) capacity: {FLOAT} {UNIT} ' \
    fr'collections: .* last_copies: .* last_secs: .* secs_since: 0'

BLOCK_CACHE_ENTRY_STATS_REGEX = \
    r"Block cache entry stats\(count,size,portion\): (.*)"

BLOCK_CACHE_ENTRY_ROLES_NAMES_REGEX = r"([A-Za-z]+)\("

BLOCK_CACHE_ENTRY_ROLES_STATS = r"\(([0-9]+,[0-9\.]+ [A-Z]+,[0-9\.]+%)+"

BLOCK_CACHE_ROLE_STATS_COMPONENTS =\
    r'([0-9]+),([0-9\.]+) ([A-Z]+),([0-9\.]+)%'

STATS_COUNTER_REGEX = r"([\w\.]+) COUNT : (\d+)\s*$"

STATS_HISTOGRAM_REGEX = \
    fr'([\w\.]+) P50 : {FLOAT} P95 : {FLOAT} P99 : {FLOAT} '\
    fr'P100 : {FLOAT} COUNT : {INT} SUM : {INT}'

BLOB_STATS_LINE_REGEX = \
    fr'Blob file count: ([\d]+), total size: {FLOAT} GB, '\
    fr'garbage size: {FLOAT} GB, space amp: {FLOAT}'

SUPPORT_INFO_START_LINE_REGEX = r'\s*Compression algorithms supported:\s*$'

VERSION_REGEX = r'(\d+)\.(\d+)\.?(\d+)?'
ROCKSDB_OPTIONS_FILE_REGEX = r"OPTIONS-rocksdb-(\d+\.\d+\.?\d*)"
SPEEDB_OPTIONS_FILE_REGEX = r"OPTIONS-speedb-(\d+\.\d+\.?\d*)"

DB_WIDE_INTERVAL_STALL_REGEX = \
    fr"Interval stall: (\d+):(\d+):(\d+)\.(\d+) H:M:S, {FLOAT} percent"

DB_WIDE_CUMULATIVE_STALL_REGEX = \
    fr"Cumulative stall: (\d+):(\d+):(\d+)\.(\d+) H:M:S, {FLOAT} percent"

CF_STALLS_LINE_START = "Stalls(count):"
CF_STALLS_COUNT_AND_REASON_REGEX = r"\b(\d+) (.*?),"
CF_STALLS_INTERVAL_COUNT_REGEX = r".*interval (\d+) total count$"

DB_SESSION_ID_REGEX = r"DB Session ID:\s*([0-9A-Z]+)"
