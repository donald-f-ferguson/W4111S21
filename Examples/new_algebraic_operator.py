import pymysql

conn = pymysql.connect(
    host="localhost",
    user="dbuser",
    password="dbuserdbuser",
    db="imdbf20raw",
    cursorclass=pymysql.cursors.DictCursor
)

cur = conn.cursor()
res = cur.execute("select * from name_basics limit 10")
res = cur.fetchall()

#print(res)


def cool_new_operator(in_df, key_column, keep_colums, split_column):
    """

    :param in_df: Input dataframe to convert.
    :param key_column: Primary key of in_df.
    :param keep_colums: Columns to keep in the original dataframe.
    :param split_column: Column to split.
    :return: Three dataframes -
                    in_df with columns to keep.
                    new_df with unique values from split column.
                    assoc_entity that links dataframe values.
    """
    changed_in_df = []
    new_df = {}
    assoc_entity = []

    for r in in_df:
        new_r = {k:r[k] for k in keep_colums}
        split_c = r[split_column].split(',')

        for c in split_c:
            a_idx = new_df.get(c, None)
            if a_idx is None:
                new_idx = len(new_df.keys())+1
                new_df[c] = new_idx
                a_idx = new_idx
            new_d = dict()
            new_d[key_column] = c
            new_d[split_column] = a_idx
            assoc_entity.append(new_d)

        changed_in_df.append(new_r)

    return changed_in_df, assoc_entity, new_df


df1, df2, df3  = cool_new_operator(res,
                                   'nconst',
                                   ['nconst', 'primary_name', 'birth_year', 'death_year'],
                                   'primary_profession')

print(df1),
print(df2)
print(df3)

