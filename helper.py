from functools import lru_cache

import numpy as np
#from pandarallel import pandarallel

#pandarallel.initialize()

import pandas as pd


def convert_date_to_quarter(date: int):
    quarters = {3: 1, 6: 2, 9: 3, 12: 4}
    date = str(date)
    year = int(date[:4])
    month = int(date[4:6])

    for i, quarter in quarters:
        if month <= i:
            return year, quarter

    assert False


def extract_year(date):
    return int(str(date)[:4])


def get_firm_name_cik(sub_df, adsh):
    sub_df = sub_df[sub_df['adsh'] == adsh]
    return sub_df['name'].values[0], sub_df['cik'].values[0]


def update_df(df: pd.DataFrame, sub_df, row):
    firm_name = get_firm_name(sub_df, row['adsh'])
    year = extract_year(row['ddate'])
    # see if firm and year exist
    if df[(df['firm_name'] == firm_name) & (df['year'] == year)].empty:
        df = pd.concat([df, pd.DataFrame({'firm_name': [firm_name], 'year': [year]})], copy=False)
    # if column doesnt exist
    if row['tag'] not in df.columns:
        df[row['tag']] = None
    df.loc[(df['firm_name'] == firm_name) & (df['year'] == year), row['tag']] = row['value']
    return df


conditions = [
    #lambda row: row['uom'] == "USD",
]

def get_name_func(sub, adsh_dict=None):
    if adsh_dict is None:
        adsh_dict = {}
    def get_name(row):
        try:
            if row['adsh'] not in adsh_dict:
                adsh_dict[row['adsh']] = get_firm_name_cik(sub, row['adsh'])
            return adsh_dict[row['adsh']]
        except Exception as e:
            return None
    return get_name

def get_skeleton_set(df: pd.DataFrame, sub: pd.DataFrame):
    adsh_dict = {}
    firm_year_set = set()
    tag_set = df['tag'].unique()
    def get_name_cik(row):
        try:
            if row['adsh'] not in adsh_dict:
                adsh_dict[row['adsh']] = get_firm_name_cik(sub, row['adsh'])
            return adsh_dict[row['adsh']]
        except Exception as e:
            return None
    df = df.copy() # do not modify original df
    df.loc[:, 'name'] = df.apply(lambda row: get_name_cik(row)[0], axis=1)
    df.loc[:, 'cik'] = df.apply(lambda row: get_name_cik(row)[1], axis=1)
    df = df.dropna(subset=['name'])
    df.loc[:, 'name-cik-year'] = df.apply(lambda row: row['name'] + "_-_" + str(row['cik']) + "_-_" + str(extract_year(row['ddate'])), axis=1)
    df = df.replace('', None)
    df = df.dropna(subset=['name-cik-year'])
    #for index, row in df.iterrows():
    #    adsh_dict[row['adsh']] = adsh_dict.get(row['adsh'], get_firm_name(sub, row['adsh']))
    #    passes_condition_test = True
    #    for condition in conditions:
    #        if not condition(row):
    #            passes_condition_test = False
    #            break
    #    if not passes_condition_test:
    #        continue
    #    firm_year_set.add((adsh_dict[row['adsh']], extract_year(row['ddate'])))
    firm_year_set = df['name-cik-year'].unique()
    firm_year_set = {tuple(fy.split("_-_")) for fy in firm_year_set}

    return df, firm_year_set, tag_set


def get_10k_df(num_df: pd.DataFrame, sub_df: pd.DataFrame):
    sub_df = sub_df[(sub_df['form'] == "10-K") | (sub_df['form'] == "10-K/A")]
    num_df = num_df[num_df['adsh'].isin(set(sub_df['adsh'].values))]
    num_df = num_df[(num_df['uom'] == "USD") & ~np.isnan(num_df['value'])]  # make sure that value is filled in and uom is USD
    return num_df, sub_df


def extract_data(out: pd.DataFrame, num: pd.DataFrame, sub: pd.DataFrame, tag_set, firm_year_index_cache):
    if firm_year_index_cache is None:
        firm_year_index_cache = {}

    get_name = get_name_func(sub)

    def extract_row(row):
        if row["tag"] not in tag_set:
            return
        if row["value"] is None:
            return
        (firm, _), year = get_name(row), extract_year(row["ddate"])
        index = firm_year_index_cache.get((firm, year))
        if not index:
            return

        # if cell is not filled in
        if pd.isna(out.at[index, row["tag"]]):
            out.at[index, row["tag"]] = row["value"]

    num.apply(extract_row, axis=1)



if __name__ == "__main__":
    extract_data(pd.DataFrame({'test': [1]}), pd.DataFrame(), pd.DataFrame())
