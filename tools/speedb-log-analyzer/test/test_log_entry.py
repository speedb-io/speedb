from log_entry import LogEntry


def test_is_entry_start():
    # Dummy text
    assert not LogEntry.is_entry_start(("XXXX"))

    # Invalid line - timestamp missing microseconds
    assert not LogEntry.is_entry_start("2022/11/24-15:58:04")

    # Invalid line - timestamp microseconds is cropped
    assert not LogEntry.is_entry_start("2022/11/24-15:58:04.758")

    # Valid line
    assert LogEntry.is_entry_start("2022/11/24-15:58:04.758352 32819 ")


if __name__ == '__main__':
    test_is_entry_start()
