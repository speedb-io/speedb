import defs_and_utils


class DatabaseOptions:
    DB_WIDE_KEY = "DBOptions"
    CF_KEY = "CFOptions"
    TABLE_OPTIONS_KEY = "TableOptions.BlockBasedTable"

    @staticmethod
    def is_misc_option(option_name):
        # these are miscellaneous options that are not yet supported by the
        # Rocksdb options file, hence they are not prefixed with any section
        # name
        return '.' not in option_name

    @staticmethod
    def get_cfs_options_diff(opt_old, old_cf_name, opt_new, new_cf_name):
        # type: Dict(option, Tuple(old value, new value) -> # noqa
        # note: diff should contain a tuple of values only if they are
        # different from each other
        diff = {}

        options_union = set(opt_old.keys()).union(set(opt_new.keys()))
        for opt in options_union:
            # if option in options_union, then it must be in one of the configs
            if opt not in opt_old:
                if new_cf_name in opt_new[opt]:
                    diff[opt] = (None, opt_new[opt][new_cf_name])
            elif opt not in opt_new:
                if old_cf_name in opt_old[opt]:
                    diff[opt] = (opt_old[opt][old_cf_name], None)
            else:
                if old_cf_name in opt_old[opt]:
                    if new_cf_name in opt_new[opt]:
                        if opt_old[opt][old_cf_name] != \
                                opt_new[opt][new_cf_name]:
                            diff[opt] = (
                                opt_old[opt][old_cf_name],
                                opt_new[opt][new_cf_name]
                            )
                    else:
                        diff[opt] = (opt_old[opt][old_cf_name], None)
                elif new_cf_name in opt_new[opt]:
                    diff[opt] = (None, opt_new[opt][new_cf_name])

        if diff:
            diff["cf names"] = (old_cf_name, new_cf_name)

        return diff

    def __init__(self, options_dict=None, misc_options=None):
        # The options are stored in the following data structure:
        # Dict[section_type, Dict[section_name, Dict[option_name, value]]]
        self.misc_options = None
        self.options_dict = options_dict if options_dict else dict()
        self.column_families = None
        self.setup_column_families()

        # Setup the miscellaneous options expected to be List[str], where each
        # element in the List has the format "<option_name>=<option_value>"
        # These options are the ones that are not yet supported by the Rocksdb
        # OPTIONS file, so they are provided separately
        self.setup_misc_options(misc_options)

    def set_db_wide_options(self, db_wide_options_dict):
        assert not self.are_db_wide_options_set()

        # The input dictionary is expected to be like this:
        # Dict[<option_name>: <option_value>]
        self.options_dict[DatabaseOptions.DB_WIDE_KEY] = {
            defs_and_utils.NO_COL_FAMILY: db_wide_options_dict}
        self.setup_column_families()

    def set_cf_options(self, cf_name, non_table_options_dict,
                       table_options_dict):
        assert non_table_options_dict and\
               isinstance(non_table_options_dict, dict)
        assert table_options_dict and isinstance(table_options_dict, dict)

        if DatabaseOptions.CF_KEY in self.options_dict:
            assert cf_name not in self.options_dict[DatabaseOptions.CF_KEY]
        else:
            self.options_dict[DatabaseOptions.CF_KEY] = dict()

        if DatabaseOptions.TABLE_OPTIONS_KEY in self.options_dict:
            assert cf_name not in self.options_dict[DatabaseOptions.CF_KEY]
        else:
            self.options_dict[DatabaseOptions.TABLE_OPTIONS_KEY] = dict()

        # The input dictionaries is expected to be like this:
        # Dict[<option_name>: <option_value>]
        self.options_dict[DatabaseOptions.CF_KEY][cf_name] = \
            non_table_options_dict
        self.options_dict[DatabaseOptions.TABLE_OPTIONS_KEY][cf_name] = \
            table_options_dict

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

        if DatabaseOptions.CF_KEY in self.options_dict:
            self.column_families =\
                list(self.options_dict[DatabaseOptions.CF_KEY].keys())

        if DatabaseOptions.DB_WIDE_KEY in self.options_dict:
            self.column_families.extend(
                list(self.options_dict[DatabaseOptions.DB_WIDE_KEY].keys()))

    def are_db_wide_options_set(self):
        return DatabaseOptions.DB_WIDE_KEY in self.options_dict

    def get_misc_options(self):
        # these are options that are not yet supported by the Rocksdb OPTIONS
        # file, hence they are provided and stored separately
        return self.misc_options

    def get_column_families(self):
        cf_names_no_db_wide = self.column_families
        if defs_and_utils.NO_COL_FAMILY in cf_names_no_db_wide:
            cf_names_no_db_wide.remove(defs_and_utils.NO_COL_FAMILY)
        return cf_names_no_db_wide

    def get_all_options(self):
        # This method returns all the options that are stored in this object as
        # a: Dict[<sec_type>.<option_name>: Dict[col_fam, option_value]]
        all_options = []
        # Example: in the section header '[CFOptions "default"]' read from the
        # OPTIONS file, sec_type='CFOptions'
        for sec_type in self.options_dict:
            for col_fam in self.options_dict[sec_type]:
                for opt_name in self.options_dict[sec_type][col_fam]:
                    option = sec_type + '.' + opt_name
                    all_options.append(option)
        all_options.extend(list(self.misc_options.keys()))
        return self.get_options(all_options)

    def get_db_wide_options(self):
        db_wide_options = []

        sec_type = DatabaseOptions.DB_WIDE_KEY
        if sec_type in self.options_dict:
            sec_dict = self.options_dict[sec_type]
            for opt_name in sec_dict[defs_and_utils.NO_COL_FAMILY]:
                option = sec_type + '.' + opt_name
                db_wide_options.append(option)
        db_wide_options.extend(list(self.misc_options.keys()))
        return self.get_options(db_wide_options,
                                cf_name=defs_and_utils.NO_COL_FAMILY)

    def get_db_wide_options_for_display(self):
        options_for_display = {}
        db_wide_options = self.get_db_wide_options()
        for internal_option_name, option_value_with_cf in \
                db_wide_options.items():
            option_name =\
                internal_option_name[len(DatabaseOptions.DB_WIDE_KEY) + 1:]
            option_value = option_value_with_cf[defs_and_utils.NO_COL_FAMILY]
            options_for_display[option_name] = option_value
        return options_for_display

    def get_db_wide_option(self, option_name):
        db_wide = self.options_dict[DatabaseOptions.DB_WIDE_KEY]
        if option_name not in db_wide[defs_and_utils.NO_COL_FAMILY]:
            return None

        return db_wide[defs_and_utils.NO_COL_FAMILY][option_name]

    def set_db_wide_option(self, option_name, option_value,
                           allow_new_option=False):
        if not allow_new_option:
            assert self.get_db_wide_option(option_name) is not None

        self.options_dict[DatabaseOptions.DB_WIDE_KEY][
            defs_and_utils.NO_COL_FAMILY][option_name] = option_value

    def get_cf_options(self, cf_name):
        # This method returns all the options that are stored in this object as
        # a: Dict[<sec_type>.<option_name>: Dict[col_fam, option_value]]
        cf_options = []
        # Example: in the section header '[CFOptions "default"]' read from the
        # OPTIONS file, sec_type='CFOptions'
        for sec_type in self.options_dict:
            if cf_name in self.options_dict[sec_type]:
                for opt_name in self.options_dict[sec_type][cf_name]:
                    option = sec_type + '.' + opt_name
                    cf_options.append(option)
        cf_options.extend(list(self.misc_options.keys()))
        return self.get_options(cf_options, cf_name)

    def get_cf_options_for_display(self, cf_name):
        options_for_display = {}
        table_options_for_display = {}
        cf_options = self.get_cf_options(cf_name)
        for internal_option_name, option_value_with_cf in \
                cf_options.items():
            option_value = option_value_with_cf[cf_name]
            if internal_option_name.startswith(DatabaseOptions.CF_KEY):
                option_name = \
                    internal_option_name[len(DatabaseOptions.CF_KEY) + 1:]
                options_for_display[option_name] = option_value
            else:
                assert internal_option_name.startswith(
                    DatabaseOptions.TABLE_OPTIONS_KEY)
                option_name = \
                    internal_option_name[
                        len(DatabaseOptions.TABLE_OPTIONS_KEY) + 1:]
                table_options_for_display[option_name] = option_value
        return options_for_display, table_options_for_display

    def get_cf_option(self, cf_name, option_name):
        # This method returns the value of the option_name for cf_name
        if cf_name not in self.options_dict[DatabaseOptions.CF_KEY]:
            return None
        if option_name not in \
                self.options_dict[DatabaseOptions.CF_KEY][cf_name]:
            return None

        return self.options_dict[DatabaseOptions.CF_KEY][cf_name][option_name]

    def set_cf_option(self, cf_name, option_name, option_value,
                      allow_new_option=False):
        if not allow_new_option:
            assert self.get_cf_option(cf_name, option_name) is not None

        self.options_dict[DatabaseOptions.CF_KEY][cf_name][option_name] = \
            option_value

    def get_cf_table_option(self, cf_name, option_name):
        # This method returns the value of the option_name for cf_name table
        # options
        table_opts = self.options_dict[DatabaseOptions.TABLE_OPTIONS_KEY]
        if cf_name not in table_opts:
            return None
        if option_name not in table_opts[cf_name]:
            return None

        return table_opts[cf_name][option_name]

    def set_cf_table_option(self, cf_name, option_name, option_value,
                            allow_new_option=False):
        if not allow_new_option:
            assert self.get_cf_table_option(cf_name, option_name) is not None

        table_opts = self.options_dict[DatabaseOptions.TABLE_OPTIONS_KEY]
        table_opts[cf_name][option_name] = option_value

    def get_options(self, reqd_options, cf_name=None):
        # type: list[str] -> dict[str, dict[str, Any]] # noqa
        # List[option] -> Dict[option, Dict[col_fam, value]]
        reqd_options_dict = {}
        for option in reqd_options:
            if DatabaseOptions.is_misc_option(option):
                # the option is not prefixed by '<section_type>.' because it is
                # not yet supported by the Rocksdb OPTIONS file; so it has to
                # be fetched from the misc_options dictionary
                if option not in self.misc_options:
                    continue
                if option not in reqd_options_dict:
                    reqd_options_dict[option] = {}
                reqd_options_dict[option][defs_and_utils.NO_COL_FAMILY] = (
                    self.misc_options[option]
                )
            else:
                # Example: option = 'TableOptions.BlockBasedTable.block_align'
                # then, sec_type = 'TableOptions.BlockBasedTable'
                sec_type = '.'.join(option.split('.')[:-1])
                # opt_name = 'block_align'
                opt_name = option.split('.')[-1]
                if sec_type not in self.options_dict:
                    continue
                if cf_name is None:
                    cf_names = self.options_dict[sec_type]
                else:
                    cf_names = [cf_name]

                for col_fam in cf_names:
                    if opt_name in self.options_dict[sec_type][col_fam]:
                        if option not in reqd_options_dict:
                            reqd_options_dict[option] = {}
                        reqd_options_dict[option][col_fam] = (
                            self.options_dict[sec_type][col_fam][opt_name]
                        )
        return reqd_options_dict

    @staticmethod
    def get_options_diff(opt_old, opt_new):
        # type: Dict(option, Dict[col_fam, value]] X 2 -> # noqa
        # Dict[option, Dict[col_fam, Tuple(old_value, new_value)]]
        # note: diff should contain a tuple of values only if they are
        # different from each other
        options_union = set(opt_old.keys()).union(set(opt_new.keys()))
        diff = {}
        for opt in options_union:
            diff[opt] = {}
            # if option in options_union, then it must be in one of the configs
            if opt not in opt_old:
                for col_fam in opt_new[opt]:
                    diff[opt][col_fam] = (None, opt_new[opt][col_fam])
            elif opt not in opt_new:
                for col_fam in opt_old[opt]:
                    diff[opt][col_fam] = (opt_old[opt][col_fam], None)
            else:
                for col_fam in opt_old[opt]:
                    if col_fam in opt_new[opt]:
                        if opt_old[opt][col_fam] != opt_new[opt][col_fam]:
                            diff[opt][col_fam] = (
                                opt_old[opt][col_fam],
                                opt_new[opt][col_fam]
                            )
                    else:
                        diff[opt][col_fam] = (opt_old[opt][col_fam], None)

                for col_fam in opt_new[opt]:
                    if col_fam in opt_old[opt]:
                        if opt_old[opt][col_fam] != opt_new[opt][col_fam]:
                            diff[opt][col_fam] = (
                                opt_old[opt][col_fam],
                                opt_new[opt][col_fam]
                            )
                    else:
                        diff[opt][col_fam] = (None, opt_new[opt][col_fam])
            if not diff[opt]:
                diff.pop(opt)
        return diff

    @staticmethod
    def get_db_wide_options_diff(options_diff):
        db_wide_diff = {}
        for diff_key in options_diff.keys():
            if str(diff_key).startswith(DatabaseOptions.DB_WIDE_KEY):
                assert defs_and_utils.NO_COL_FAMILY in options_diff[diff_key]
                values_diff = options_diff[diff_key][
                               defs_and_utils.NO_COL_FAMILY]
                assert len(values_diff) == 2

                option_name = str(diff_key)[len(
                    DatabaseOptions.DB_WIDE_KEY)+1:]
                db_wide_diff[option_name] = {"Base": values_diff[0],
                                             "Log": values_diff[1]}

        return db_wide_diff

    @staticmethod
    def get_cf_options_diff(options_diff, cf_name):
        cf_diff = {}
        for diff_key in options_diff.keys():
            if cf_name not in options_diff[diff_key]:
                continue

            values_diff = options_diff[diff_key][cf_name]
            assert len(values_diff) == 2

            if str(diff_key).startswith(DatabaseOptions.CF_KEY):
                option_name = str(diff_key)[len(
                    DatabaseOptions.CF_KEY)+1:]
                cf_diff[option_name] = {"Base": values_diff[0],
                                        "Log": values_diff[1]}
            elif str(diff_key).startswith(DatabaseOptions.TABLE_OPTIONS_KEY):
                option_name = str(diff_key)[len(
                    DatabaseOptions.TABLE_OPTIONS_KEY)+1:]
                if "Table-Options" not in cf_diff:
                    cf_diff["Table-Options"] = {}
                cf_diff["Table-Options"][option_name] =\
                    {"Base": values_diff[0],
                     "Log": values_diff[1]}

        return cf_diff
