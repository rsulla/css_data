import csv
import os


class KeyValues:

    def __init__(self, index_name, table_name, ref_table, seq_num, col_name, ref_col_name):
        self.index_name = index_name
        self.table_name = table_name
        self.ref_table = ref_table
        self.key_cols = dict()

        self.key_cols[seq_num] = (col_name, ref_col_name)

    def add_col(self, seq_num, col_name, ref_col_name):
        self.key_cols[seq_num] = (col_name, ref_col_name)

    def __str__(self):
        ret_val = 'INDEX NAME: ' + self.index_name
        ret_val = '\nTABLE NAME: ' + self.table_name
        ret_val += '\nREF TABLE NAME: ' + self.ref_table

        for seq_num, cols in self.key_cols.items():
            ret_val += '\n  ' + seq_num + ' ' + str(cols)

        return ret_val

    def cols(self, ref_cols):
        ret_cols = ''
        sep_char = ''

        if ref_cols:
            offset = 1
        else:
            offset = 0

        for idx in range(1, len(self.key_cols)+1):

            ret_cols += sep_char + self.key_cols[str(idx)][offset].lower()
            sep_char = ', '

        return '(' + ret_cols + ')'


def process_ddl_files(in_path, out_path, table_prefix='', overwrite_fl=False):

    tables = dict()
    col_ids = dict()
    index_type = dict()
    key_def = dict()
    foreign_key_ref = dict()

    ddl_text = ''
    tbl_prefix_text = ''

    cols_file = 'DM_Columns.csv'
    indx_file = 'DM_Indexes.csv'
    constr_file = 'DM_Constr_Index_Columns.csv'
    foreign_key_file = 'DM_ForeignKeys.csv'

    if not isinstance(in_path, str):
        raise TypeError("in_path must be str type")

    if not isinstance(out_path, str):
        raise TypeError("out_path must be str type")

    if not isinstance(table_prefix, str):
        raise TypeError("table_prefix must be str type")

    if not isinstance(overwrite_fl, bool):
        raise TypeError("overwrite_fl must be bool type")

    if not os.path.isdir(in_path):
        raise ValueError("in_path value (" + in_path + ") must be valid path")

    if not in_path.endswith(os.path.sep):
        in_path += os.path.sep

    if not (os.path.isfile(in_path + cols_file) and
            os.path.isfile(in_path + indx_file) and
            os.path.isfile(in_path + constr_file) and
            os.path.isfile(in_path + foreign_key_file)):
        raise IOError("Missing 1 or more of the following files:" +
                      "\n\t" + in_path + cols_file +
                      "\n\t" + in_path + indx_file +
                      "\n\t" + in_path + constr_file +
                      "\n\t" + in_path + foreign_key_file)

    if not overwrite_fl and os.path.isfile(out_path):
        raise IOError(out_path + " output file already exists, must specify overwrite")

    if len(table_prefix) > 0:
        tbl_prefix_text = table_prefix + '.'

    with open(in_path + cols_file, newline='') as csv_file:
        csv_reader = csv.reader(csv_file)

        row_num = 0
        for row in csv_reader:
            if row_num > 0:
                if row[33] in tables:
                    cols = tables[row[33]]
                else:
                    tables[row[33]] = dict()
                    cols = tables[row[33]]

                cols[row[37]] = (row[0], row[36], row[41], row[42], row[43])

                col_ids[row[1]] = row[0]

            row_num += 1

    with open(in_path + indx_file, newline='') as csv_file:
        csv_reader = csv.reader(csv_file)

        row_num = 0
        for row in csv_reader:
            if row_num > 0:
                index_type[row[1]] = row[6]

            row_num += 1

    with open(in_path + foreign_key_file, newline='') as csv_file:
        csv_reader = csv.reader(csv_file)

        row_num = 0
        for row in csv_reader:
            if row_num > 0:
                if not row[0] in foreign_key_ref:
                    foreign_key_ref[row[0]] = row[7]

            row_num += 1

    with open(in_path + constr_file, newline='') as csv_file:
        csv_reader = csv.reader(csv_file)

        row_num = 0
        for row in csv_reader:
            if row_num > 0:
                if row[0] in key_def:
                    key_def[row[0]].add_col(row[9], col_ids[row[2]], row[8])
                else:
                    if row[6] in foreign_key_ref:
                        ref_tbl = foreign_key_ref[row[6]]
                    else:
                        ref_tbl = ''

                    new_entry = KeyValues(row[6], row[7], ref_tbl, row[9], col_ids[row[2]], row[8])
                    key_def[row[0]] = new_entry

            row_num += 1

    for tbl, cols in tables.items():
        ddl_text += 'drop table if exists ' + tbl_prefix_text + tbl.lower() + ' CASCADE;\n'

    for tbl, cols in tables.items():
        ddl_text += '\n\ncreate table ' + tbl_prefix_text + tbl.lower() + ' (\n'

        max_len = 0
        for col in cols.values():
            if len(col[0]) > max_len:
                max_len = len(col[0])

        sep_char = '   '
        for seq_num in range(1, len(cols)+1):
            if len(cols[str(seq_num)][2]) > 0:
                extra = '(' + cols[str(seq_num)][2] + ')'
            elif len(cols[str(seq_num)][3]) > 0 and len(cols[str(seq_num)][4]) > 0:
                extra = '(' + cols[str(seq_num)][3] + ',' + cols[str(seq_num)][4] + ')'
            else:
                extra = ''

            col_name = (cols[str(seq_num)][0].lower() + (' ' * max_len))[:max_len+1]

            ddl_text += sep_char + col_name + ' ' + cols[str(seq_num)][1] + extra + '\n'

            sep_char = '  ,'

        for idx_id, idx_value in key_def.items():
            if index_type[idx_id] == 'Primary Constraint' and idx_value.table_name == tbl:
                ddl_text += '  ,PRIMARY KEY ' + idx_value.cols(False)

        ddl_text += ')\nWITH (OIDS=FALSE);\n'

    ddl_text += '\n'

    for tbl in tables.keys():
        for idx_id, idx_value in key_def.items():
            if index_type[idx_id] == 'Foreign Key' and idx_value.table_name == tbl:
                ddl_text += 'ALTER TABLE ' + tbl_prefix_text + tbl + ' ADD CONSTRAINT ' + idx_value.index_name + \
                            ' FOREIGN KEY ' + idx_value.cols(False) + \
                            ' REFERENCES ' + tbl_prefix_text + idx_value.ref_table + ' ' + idx_value.cols(True) + ';\n'

    ddl_text += '\n'
    ddl_text += """insert into {TBL}sector (sector_dc, sector_ds) values 
                      (1,'Public, 4-year or above'),
                      (2,'Private nonprofit, 4-year or above'),
                      (4,'Public, 2-year'),
                      (3,'Private for-profit, 4-year or above'),
                      (9,'Private for-profit, less-than 2-year'),
                      (6,'Private for-profit, 2-year'),
                      (5,'Private nonprofit, 2-year'),
                      (7,'Public, less-than 2-year'),
                      (8,'Private nonprofit, less-than 2-year'),
                      (0,'Administrative Unit Only');
    """.replace('{TBL}',tbl_prefix_text)

    ddl_text += '\n'
    ddl_text += """insert into {TBL}sub_type (sub_typ_cd, sub_typ_ds) values
                      (1,'arrest'),
                      (2,'discipline');
    """.replace('{TBL}',tbl_prefix_text)

    ddl_text += '\n'
    ddl_text += """insert into {TBL}rpt_loc (loc_typ_cd, loc_typ_ds) values
                      (1,'noncampus'),
                      (2,'oncampus'),
                      (3,'publicproperty'),
                      (4,'reported'),
                      (5,'residencehall'),
                      (6,'unfounded');
    """.replace('{TBL}',tbl_prefix_text)

    ddl_text += '\n'
    ddl_text += """insert into {TBL}fire_cat (fire_cat_cd, fire_cat_ds) values
                      (1,'Intentional'),
                      (2,'Unintentional'),
                      (3,'Undetermined');
    """.replace('{TBL}',tbl_prefix_text)

    ddl_text += '\n'
    ddl_text += """insert into {TBL}fire_damage (fire_damage_cd, fire_damage_ds, fire_damage_min_am, fire_damage_max_am) values
                      (1,'$0-$99',0,99),
                      (2,'$100-$999',100,999),
                      (3,'$1,000-$9,999',1000,9999),
                      (4,'$10,000-$24,999',10000,24999),
                      (5,'$25,000-$49,999',25000,49999),
                      (6,'$50,000-$99,999',50000,99999),
                      (7,'$100,000-$249,999',100000,249999),
                      (8,'$250,000-$499,999',250000,499999),
                      (9,'$500,000-$999,999',500000,999999),
                      (10,'>$1,000,000',1000000,2147483647);
    """.replace('{TBL}',tbl_prefix_text)

    try:
        with open(out_path, 'w') as ddl_out_file:
            ddl_out_file.write(ddl_text)
    except IOError:
        print("IO Error occurred writing to file " + out_path)


def main():
    in_file_path = 'C:\\Users\\bob\\Documents\\CSS\\export'
    out_file_path = 'C:\\Users\\bob\\Documents\\CSS\\export\\ddl.sql'

    process_ddl_files(in_file_path, out_file_path, table_prefix='css_tbl', overwrite_fl=True)


if __name__ == "__main__":
    main()
