from enum import Enum
import defs_and_utils

# the option is not prefixed by '<section_type>.' because it is
# not yet supported by the Rocksdb OPTIONS file; so it has to
# be fetched from the misc_options dictionary
# if option not in self.misc_options:
#     continue
# if option not in reqd_options_dict:
#     reqd_options_dict[option] = {}
# reqd_options_dict[option][DB_WIDE_CF_NAME] = (
#     self.misc_options[option]
# )

# Example: option = 'TableOptions.BlockBasedTable.block_align'
# then, section_type = 'TableOptions.BlockBasedTable'
# option_name = 'block_align'


DB_WIDE_CF_NAME = defs_and_utils.NO_COL_FAMILY


class SectionType(str, Enum):
    VERSION = "Version"
    DB_WIDE = "DBOptions"
    CF = "CFOptions"
    TABLE_OPTIONS = "TableOptions.BlockBasedTable"

    def __str__(self):
        return str(self.value)

    @staticmethod
    def extract_section_type(full_option_name):
        # Try to match a section type by combining dot-delimited words
        # in the full name. Longer combinations are tested before shorter
        # ones ("TableOptions.BlockBasedTable" will be tested before
        # "CFOptions")
        name_parts = full_option_name.split('.')
        for i in range(len(name_parts)):
            try:
                potential_section_type = '.'.join(name_parts[:-i])
                return SectionType(potential_section_type)
            except Exception: # noqa
                continue

        raise defs_and_utils.ParsingError(
            f"Invalid full option name ({full_option_name}")


def validate_section(section_type, expected_type=None):
    SectionType.extract_section_type(f"{section_type}.dummy")
    if expected_type is not None:
        if section_type != expected_type:
            raise defs_and_utils.ParsingError(
                f"Not DB Wide section name ({section_type}")


def parse_full_option_name(full_option_name):
    section_type = SectionType.extract_section_type(full_option_name)
    option_name = full_option_name.split('.')[-1]
    return section_type, option_name


def extract_option_name(full_option_name):
    section_type, option_name = parse_full_option_name(full_option_name)
    return option_name


def get_full_option_name(section_type, option_name):
    validate_section(section_type)
    return f"{section_type}.{option_name}"


def get_db_wide_full_option_name(option_name):
    return get_full_option_name(SectionType.DB_WIDE, option_name)


def extract_db_wide_option_name(full_option_name):
    section_type, option_name = parse_full_option_name(full_option_name)
    validate_section(section_type, SectionType.DB_WIDE)
    return option_name


def get_cf_full_option_name(option_name):
    return get_full_option_name(SectionType.CF, option_name)


def extract_cf_option_name(full_option_name):
    section_type, option_name = parse_full_option_name(full_option_name)
    validate_section(section_type, SectionType.CF)
    return option_name


def get_cf_table_full_option_name(option_name):
    return get_full_option_name(SectionType.TABLE_OPTIONS, option_name)


def extract_cf_table_option_name(full_option_name):
    section_type, option_name = parse_full_option_name(full_option_name)
    validate_section(section_type, SectionType.TABLE_OPTIONS)
    return option_name


class FullNamesOptionsDict:
    # {Full-option-name: {<cf>: <option-value>}}
    def __init__(self, options_dict=None):
        self.options_dict = {}
        if options_dict is not None:
            self.options_dict = options_dict

    def __eq__(self, other):
        if isinstance(other, FullNamesOptionsDict):
            return self.options_dict == other.options_dict
        elif isinstance(other, dict):
            return self.options_dict == other
        else:
            assert False, "Comparing to an invalid type " \
                          f"({type(other)})"

    def set_option(self, section_type, cf_name, option_name, option_value):
        if section_type == SectionType.DB_WIDE:
            assert cf_name == DB_WIDE_CF_NAME

        full_option_name = get_full_option_name(section_type, option_name)
        if full_option_name not in self.options_dict:
            self.options_dict[full_option_name] = {}

        self.options_dict[full_option_name].update({cf_name: option_value})

    def set_misc_option(self, option_name, option_value):
        if option_name not in self.options_dict:
            self.options_dict[option_name] = {}

        self.options_dict[option_name].update({DB_WIDE_CF_NAME: option_value})

    def get_option_by_full_name(self, full_option_name):
        if full_option_name not in self.options_dict:
            return None
        return self.options_dict[full_option_name]

    def get_option_by_parts(self, section_type, option_name, cf_name=None):
        value = self.get_option_by_full_name(get_full_option_name(
            section_type, option_name))
        if value is None or cf_name is None:
            return value
        if cf_name not in value:
            return None
        return value[cf_name]

    def set_db_wide_option(self, option_name, option_value):
        self.set_option(SectionType.DB_WIDE, DB_WIDE_CF_NAME, option_name,
                        option_value)

    def get_db_wide_option(self, option_name):
        return self.get_option_by_parts(SectionType.DB_WIDE, option_name,
                                        DB_WIDE_CF_NAME)

    def set_cf_option(self, cf_name, option_name, option_value):
        self.set_option(SectionType.CF, cf_name, option_name,
                        option_value)

    def get_cf_option(self, option_name, cf_name=None):
        return self.get_option_by_parts(SectionType.CF, option_name, cf_name)

    def set_cf_table_option(self, cf_name, option_name, option_value):
        self.set_option(SectionType.TABLE_OPTIONS, cf_name, option_name,
                        option_value)

    def get_cf_table_option(self, option_name, cf_name=None):
        return self.get_option_by_parts(SectionType.TABLE_OPTIONS,
                                        option_name, cf_name)

    def get_options_dict(self):
        return self.options_dict


class OptionsDiff:
    def __init__(self, baseline_options, new_options):
        self.baseline_options = baseline_options
        self.new_options = new_options
        self.diff_dict = {}

    def __eq__(self, other):
        if isinstance(other, OptionsDiff):
            return self.diff_dict == other.diff_dict
        elif isinstance(other, dict):
            return self.diff_dict == other
        else:
            assert False, "Comparing to an invalid type " \
                          f"({type(other)})"

    def diff_in_base(self, cf_name, full_option_name):
        self.add_option_if_necessary(full_option_name)
        self.diff_dict[full_option_name][cf_name] = \
            (self.baseline_options[full_option_name][cf_name], None)

    def diff_in_new(self, cf_name, full_option_name):
        self.add_option_if_necessary(full_option_name)
        self.diff_dict[full_option_name][cf_name] = \
            (None, self.new_options[full_option_name][cf_name])

    def diff_between(self, cf_name, full_option_name):
        self.add_option_if_necessary(full_option_name)
        self.diff_dict[full_option_name][cf_name] = \
            (self.baseline_options[full_option_name][cf_name],
             self.new_options[full_option_name][cf_name])

    def add_option_if_necessary(self, full_option_name):
        if full_option_name not in self.diff_dict:
            self.diff_dict[full_option_name] = {}

    def is_empty_diff(self):
        return self.diff_dict == {}

    def get_diff_dict(self):
        return self.diff_dict


class CfsOptionsDiff:
    CF_NAMES_KEY = "cf names"

    def __init__(self, baseline_options, baseline_cf_name,
                 new_options, new_cf_name, diff_dict=None):
        self.baseline_options = baseline_options
        self.baseline_cf_name = baseline_cf_name
        self.new_options = new_options
        self.new_cf_name = new_cf_name
        self.diff_dict = {}
        if diff_dict is not None:
            self.diff_dict = diff_dict

    def __eq__(self, other):
        new_dict = None
        if isinstance(other, CfsOptionsDiff):
            new_dict = other.get_diff_dict()
        elif isinstance(other, dict):
            new_dict = other
        else:
            assert False, "Comparing to an invalid type " \
                          f"({type(other)})"

        assert CfsOptionsDiff.CF_NAMES_KEY in new_dict, \
            f"{CfsOptionsDiff.CF_NAMES_KEY} key missing in other"

        baseline_dict = self.get_diff_dict()
        assert baseline_dict[CfsOptionsDiff.CF_NAMES_KEY] == \
               new_dict[CfsOptionsDiff.CF_NAMES_KEY], (
            "Comparing diff entities for mismatching cf-s: "
            f"baseline:{baseline_dict[CfsOptionsDiff.CF_NAMES_KEY]}, "
            f"new:{new_dict[CfsOptionsDiff.CF_NAMES_KEY]}")

        return self.diff_dict == new_dict

    def diff_in_base(self, full_option_name):
        self.add_option_if_necessary(full_option_name)
        self.diff_dict[full_option_name] = \
            (self.baseline_options[full_option_name][self.baseline_cf_name],
             None)

    def diff_in_new(self, full_option_name):
        self.add_option_if_necessary(full_option_name)
        self.diff_dict[full_option_name] = \
            (None, self.new_options[full_option_name][self.new_cf_name])

    def diff_between(self, full_option_name):
        self.add_option_if_necessary(full_option_name)
        self.diff_dict[full_option_name] = \
            (self.baseline_options[full_option_name][self.baseline_cf_name],
             self.new_options[full_option_name][self.new_cf_name])

    def add_option_if_necessary(self, full_option_name):
        if full_option_name not in self.diff_dict:
            self.diff_dict[full_option_name] = {}

    def is_empty_diff(self):
        return self.diff_dict == {}

    def get_diff_dict(self):
        if self.is_empty_diff():
            return {}

        result_dict = self.diff_dict
        result_dict.update({CfsOptionsDiff.CF_NAMES_KEY:
                            {"Base": self.baseline_cf_name,
                             "New": self.new_cf_name}})
        return result_dict


class DatabaseOptions:
    @staticmethod
    def is_misc_option(option_name):
        # these are miscellaneous options that are not yet supported by the
        # Rocksdb options file, hence they are not prefixed with any section
        # name
        return '.' not in option_name

    def __init__(self, section_based_options_dict=None, misc_options=None):
        # The options are stored in the following data structure:
        # {DBOptions: {DB_WIDE: {<option-name>: <option-value>, ...}},
        # {CFOptions: {<cf-name>: {<option-name>: <option-value>, ...}},
        # {TableOptions.BlockBasedTable:
        #   {<cf-name>: {<option-name>: <option-value>, ...}}}
        # If section_based_options_dict is specified, it must be in the
        # internal format (e.g., used when loading options from an options
        # file)
        self.options_dict = {}
        if section_based_options_dict:
            # TODO - Verify the format of the options_dict
            self.options_dict = section_based_options_dict

        self.misc_options = None
        self.column_families = None
        self.setup_column_families()

        # Setup the miscellaneous options expected to be List[str], where each
        # element in the List has the format "<option_name>=<option_value>"
        # These options are the ones that are not yet supported by the Rocksdb
        # OPTIONS file, so they are provided separately
        self.setup_misc_options(misc_options)

    def __str__(self):
        return "DatabaseOptions"

    def set_db_wide_options(self, options_dict):
        # The input dictionary is expected to be like this:
        # {<option name>: <option_value>}
        if self.are_db_wide_options_set():
            raise defs_and_utils.ParsingAssertion(
                "DB Wide Options Already Set")

        self.options_dict[SectionType.DB_WIDE] = {
            DB_WIDE_CF_NAME: options_dict}
        self.setup_column_families()

    def create_section_if_necessary(self, section_type):
        if section_type not in self.options_dict:
            self.options_dict[section_type] = dict()

    def create_cf_in_section_if_necessary(self, section_type, cf_name):
        if cf_name not in self.options_dict[section_type]:
            self.options_dict[section_type][cf_name] = dict()

    def create_section_and_cf_in_section_if_necessary(self, section_type,
                                                      cf_name):
        self.create_section_if_necessary(section_type)
        self.create_cf_in_section_if_necessary(section_type, cf_name)

    def validate_no_section(self, section_type):
        if section_type in self.options_dict:
            raise defs_and_utils.ParsingAssertion(
                f"{section_type} Already Set")

    def validate_no_cf_name_in_section(self, section_type, cf_name):
        if cf_name in self.options_dict[section_type]:
            raise defs_and_utils.ParsingAssertion(
                f"{section_type} Already Set for this CF ({cf_name})")

    def set_cf_options(self, cf_name, cf_non_table_options, table_options):
        # Both input dictionaries are expected to be like this:
        # {<option name>: <option_value>}
        self.create_section_if_necessary(SectionType.CF)
        self.validate_no_cf_name_in_section(SectionType.CF, cf_name)

        self.create_section_if_necessary(SectionType.TABLE_OPTIONS)
        self.validate_no_cf_name_in_section(SectionType.TABLE_OPTIONS,
                                            cf_name)

        self.options_dict[SectionType.CF][cf_name] = cf_non_table_options
        self.options_dict[SectionType.TABLE_OPTIONS][cf_name] = table_options

        self.setup_column_families()

    def setup_misc_options(self, misc_options):
        self.misc_options = {}
        if misc_options:
            for option_pair_str in misc_options:
                option_name = option_pair_str.split('=')[0].strip()
                option_value = option_pair_str.split('=')[1].strip()
                self.misc_options[option_name] = option_value

    def setup_column_families(self):
        self.column_families = []

        if SectionType.CF in self.options_dict:
            self.column_families =\
                list(self.options_dict[SectionType.CF].keys())

        if SectionType.DB_WIDE in self.options_dict:
            self.column_families.extend(
                list(self.options_dict[SectionType.DB_WIDE].keys()))

    def are_db_wide_options_set(self):
        return SectionType.DB_WIDE in self.options_dict

    def get_misc_options(self):
        # these are options that are not yet supported by the Rocksdb OPTIONS
        # file, hence they are provided and stored separately
        return self.misc_options

    def get_cfs_names(self):
        cf_names_no_db_wide = self.column_families
        if DB_WIDE_CF_NAME in cf_names_no_db_wide:
            cf_names_no_db_wide.remove(DB_WIDE_CF_NAME)
        return cf_names_no_db_wide

    def set_db_wide_option(self, option_name, option_value,
                           allow_new_option=False):
        if not allow_new_option:
            assert self.get_db_wide_option(option_name) is not None,\
                "Trying to update a non-existent DB Wide Option." \
                f"{option_name} = {option_value}"

        self.create_section_and_cf_in_section_if_necessary(
            SectionType.DB_WIDE, DB_WIDE_CF_NAME)
        self.options_dict[SectionType.DB_WIDE][
            DB_WIDE_CF_NAME][option_name] = option_value

    def get_all_options(self):
        # Returns all the options that as a FullNamesOptionsDict
        full_options_names = []
        for section_type in self.options_dict:
            for cf_name in self.options_dict[section_type]:
                for option_name in self.options_dict[section_type][cf_name]:
                    full_option_name = get_full_option_name(section_type,
                                                            option_name)
                    full_options_names.append(full_option_name)

        full_options_names.extend(list(self.misc_options.keys()))
        return self.get_options(full_options_names)

    def get_db_wide_options(self):
        # Returns the DB-Wide options as a FullNamesOptionsDict
        full_options_names = []
        section_type = SectionType.DB_WIDE
        if section_type in self.options_dict:
            sec_dict = self.options_dict[section_type]
            for option_name in sec_dict[DB_WIDE_CF_NAME]:
                full_option_name = get_full_option_name(section_type,
                                                        option_name)
                full_options_names.append(full_option_name)

        full_options_names.extend(list(self.misc_options.keys()))

        return self.get_options(full_options_names, DB_WIDE_CF_NAME)

    def get_db_wide_options_for_display(self):
        options_for_display = {}
        db_wide_options = self.get_db_wide_options().get_options_dict()
        for full_option_name, option_value_with_cf in db_wide_options.items():
            option_name = extract_db_wide_option_name(full_option_name)
            option_value = option_value_with_cf[DB_WIDE_CF_NAME]
            options_for_display[option_name] = option_value
        return options_for_display

    def get_db_wide_option(self, option_name):
        # Returns the raw value of a single DB-Wide option
        full_option_name = get_full_option_name(SectionType.DB_WIDE,
                                                option_name)
        option_value_dict = \
            self.get_options([full_option_name]).get_options_dict()
        if not option_value_dict:
            return None
        else:
            return option_value_dict[full_option_name][DB_WIDE_CF_NAME]

    def get_cf_options(self, cf_name):
        # Returns the options for cf-name as a FullNamesOptionsDict
        full_options_names = []
        for section_type in self.options_dict:
            if cf_name in self.options_dict[section_type]:
                for option_name in self.options_dict[section_type][cf_name]:
                    full_option_name = \
                        get_full_option_name(section_type, option_name)
                    full_options_names.append(full_option_name)

        full_options_names.extend(list(self.misc_options.keys()))

        return self.get_options(full_options_names, cf_name)

    def get_cf_options_for_display(self, cf_name):
        options_for_display = {}
        table_options_for_display = {}
        cf_options = self.get_cf_options(cf_name)
        for full_option_name, option_value_with_cf in \
                cf_options.get_options_dict().items():
            option_value = option_value_with_cf[cf_name]
            section_type, option_name =\
                parse_full_option_name(full_option_name)

            if section_type == SectionType.CF:
                options_for_display[option_name] = option_value
            else:
                assert section_type == SectionType.TABLE_OPTIONS
                table_options_for_display[option_name] = option_value

        return options_for_display, table_options_for_display

    def get_cf_option(self, cf_name, option_name):
        # Returns the raw value of a single CF option
        full_option_name = get_full_option_name(SectionType.CF, option_name)
        option_value_dict = self.get_options([full_option_name], cf_name). \
            get_options_dict()
        if not option_value_dict:
            return None
        else:
            return option_value_dict[full_option_name][cf_name]

    def set_cf_option(self, cf_name, option_name, option_value,
                      allow_new_option=False):
        if not allow_new_option:
            assert self.get_cf_option(cf_name, option_name) is not None,\
                "Trying to update a non-existent CF Option." \
                f"cf:{cf_name} - {option_name} = {option_value}"

        self.create_section_and_cf_in_section_if_necessary(SectionType.CF,
                                                           cf_name)
        self.options_dict[SectionType.CF][cf_name][option_name] = \
            option_value

    def get_cf_table_option(self, cf_name, option_name):
        # Returns the raw value of a single CF Table option
        full_option_name = get_full_option_name(SectionType.TABLE_OPTIONS,
                                                option_name)
        option_value_dict = self.get_options([full_option_name], cf_name). \
            get_options_dict()
        if not option_value_dict:
            return None
        else:
            return option_value_dict[full_option_name][cf_name]

    def set_cf_table_option(self, cf_name, option_name, option_value,
                            allow_new_option=False):
        if not allow_new_option:
            assert self.get_cf_table_option(cf_name, option_name) is not None,\
                "Trying to update a non-existent CF Table Option." \
                f"cf:{cf_name} - {option_name} = {option_value}"

        self.create_section_and_cf_in_section_if_necessary(
            SectionType.TABLE_OPTIONS, cf_name)
        self.options_dict[SectionType.TABLE_OPTIONS][cf_name][option_name] = \
            option_value

    def get_options(self, requested_full_options_names,
                    requested_cf_name=None):
        # The input is expected to be a set or a list of the format:
        # [<full option name>, ...]
        # Returns a FullNamesOptionsDict for the requested options
        assert isinstance(requested_full_options_names, list) or isinstance(
            requested_full_options_names, set),\
            f"Illegat requested_full_options_names type " \
            f"({type(requested_full_options_names)}"

        requested_full_options_names = set(requested_full_options_names)
        options = FullNamesOptionsDict()

        for full_option_name in requested_full_options_names:
            if DatabaseOptions.is_misc_option(full_option_name):
                if full_option_name not in self.misc_options:
                    continue
                options.set_misc_option(full_option_name,
                                        self.misc_options[full_option_name])
            else:
                section_type, option_name = \
                    parse_full_option_name(full_option_name)
                if section_type not in self.options_dict:
                    continue
                if requested_cf_name is None:
                    cf_names = self.options_dict[section_type].keys()
                else:
                    cf_names = [requested_cf_name]

                for cf_name in cf_names:
                    if cf_name not in self.options_dict[section_type]:
                        continue
                    if option_name in self.options_dict[section_type][cf_name]:
                        option_value = \
                            self.options_dict[section_type][cf_name][
                                option_name]

                        options.set_option(section_type, cf_name, option_name,
                                           option_value)
        return options

    @staticmethod
    def get_options_diff(baseline, new):
        # Receives 2 sets of options and returns the difference between them.
        # Three cases exist:
        # 1. An option exists in the old but not in the new
        # 2. An option exists in the new but not in the old
        # 3. The option exists in both but the values differ
        # The inputs must be FullNamesOptionsDict instances. These may be
        # obtained
        # The resulting diff is an OptionsDiff with only actual
        # differences
        assert isinstance(baseline, FullNamesOptionsDict)
        assert isinstance(new, FullNamesOptionsDict)

        baseline_options = baseline.get_options_dict()
        new_options = new.get_options_dict()

        diff = OptionsDiff(baseline_options, new_options)

        full_options_names_union =\
            set(baseline_options.keys()).union(set(new_options.keys()))
        for full_option_name in full_options_names_union:
            # if option in options_union, then it must be in one of the configs
            if full_option_name not in baseline_options:
                for cf_name in new_options[full_option_name]:
                    diff.diff_in_new(cf_name, full_option_name)
            elif full_option_name not in new_options:
                for cf_name in baseline_options[full_option_name]:
                    diff.diff_in_base(cf_name, full_option_name)
            else:
                for cf_name in baseline_options[full_option_name]:
                    if cf_name in new_options[full_option_name]:
                        if baseline_options[full_option_name][cf_name] != \
                                new_options[full_option_name][cf_name]:
                            diff.diff_between(cf_name, full_option_name)
                    else:
                        diff.diff_in_base(cf_name, full_option_name)

                for cf_name in new_options[full_option_name]:
                    if cf_name in baseline_options[full_option_name]:
                        if baseline_options[full_option_name][cf_name] != \
                                new_options[full_option_name][cf_name]:
                            diff.diff_between(cf_name, full_option_name)
                    else:
                        diff.diff_in_new(cf_name, full_option_name)

        return diff if not diff.is_empty_diff() else None

    def get_options_diff_relative_to_me(self, new):
        baseline = self.get_all_options()
        return DatabaseOptions.get_options_diff(baseline, new)

    @staticmethod
    def get_cfs_options_diff(baseline, baseline_cf_name, new, new_cf_name):
        assert isinstance(baseline, FullNamesOptionsDict)
        assert isinstance(new, FullNamesOptionsDict)

        # Same as get_options_diff, but for specific column families.
        # This is needed to compare a parsed log file with a baseline version.
        # The baseline version contains options only for the default column
        # family. So, there is a need to compare the options for all column
        # families in the parsed log with the same default column family in
        # the base version
        baseline_opts_dict = baseline.get_options_dict()
        new_opts_dict = new.get_options_dict()

        diff = CfsOptionsDiff(baseline_opts_dict, baseline_cf_name,
                              new_opts_dict, new_cf_name)

        options_union = set(baseline_opts_dict.keys()).union(
            set(new_opts_dict.keys()))

        for full_option_name in options_union:
            # if option in options_union, then it must be in one of the configs
            if full_option_name not in baseline_opts_dict:
                new_option_values = new_opts_dict[full_option_name]
                if new_cf_name in new_option_values:
                    diff.diff_in_new(full_option_name)
            elif full_option_name not in new_opts_dict:
                baseline_option_values = baseline_opts_dict[full_option_name]
                if baseline_cf_name in baseline_option_values:
                    diff.diff_in_base(full_option_name)
            else:
                baseline_option_values = baseline_opts_dict[full_option_name]
                new_option_values = new_opts_dict[full_option_name]

                if baseline_cf_name in baseline_option_values:
                    if new_cf_name in new_option_values:
                        if baseline_option_values[baseline_cf_name] !=\
                                new_option_values[new_cf_name]:
                            diff.diff_between(full_option_name)
                    else:
                        diff.diff_in_base(full_option_name)
                elif new_cf_name in new_option_values:
                    diff.diff_in_new(full_option_name)

        return diff if not diff.is_empty_diff() else None

    @staticmethod
    def get_db_wide_options_diff(opt_old, opt_new):
        return DatabaseOptions.get_cfs_options_diff(
            opt_old,
            DB_WIDE_CF_NAME,
            opt_new,
            DB_WIDE_CF_NAME)
