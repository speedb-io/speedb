# Speedb default options change log
## Speedb 2.0
* `avoid_unnecessary_blocking_io` changes from `false` to `true`. We have seen improvements in compaction latency when this option
was set to `true`, due to the fact that obsolete files were deleted in a separate background
job, instead of being deleted as part of the compaction job.
When we need to purge a large amount of files, or large files which don't have their blocks allocated contiguously, or on bitmap-based filesystems (even when we have a contiguous range of blocks for each file), we may incur a high load on the disk when purging obsolete files that would eat into the bandwidth available to user operations and compactions.
Therefore, this change also requires a change to the implementation of the default `Env`'s `DeleteFile()` function: we need to delete by truncating in modestly sized chunks (preliminary tests showed that around 500MiB might be a good place to start and tune from there), and allowing the disk to recover in between.
