import csv
import io


def get_counters_csv(counter_and_histograms_mngr):
    f = io.StringIO()
    writer = csv.writer(f)

    mngr = counter_and_histograms_mngr
    all_entries = mngr.get_all_counters_entries()
    counters_names = mngr.get_counters_names()
    times = mngr.get_counters_times()

    # csv header line (counter names)
    writer.writerow([""] + counters_names)

    # write one line per time
    # The assumption is that, every counter has entries for all
    # times
    for time_idx, time in enumerate(times):
        csv_line = list()
        csv_line.append(time)
        for counter_name in counters_names:
            assert counter_name in all_entries

            counter_entries = all_entries[counter_name]
            assert len(counter_entries) == len(times)
            assert all_entries[counter_name][time_idx]["time"] == time

            csv_line.append(all_entries[counter_name][time_idx]["value"])

        writer.writerow(csv_line)

    return f.getvalue()
