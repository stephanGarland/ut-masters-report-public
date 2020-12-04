#!/usr/bin/env python

import fuzzy_pandas as fpd

def run_stuff(df_input, df_names):
    """
    Attempts to determine a user's gender by comparing their username to tradtionally gendered names.
    Utilizes Levenshtein distance. Assumes two Pandas Dataframes with columns "author_login" and "name".
    The threshold can be varied as needed; I empirically determined 0.72 to be the most accurate.

    Args:
        df_input: A DataFrame with unknown usernames to check.
        df_names: A DataFrame with traditionally gendered names, i.e. only names traditionally associated with men or women.

    Returns:
        A Fuzzy Pandas DataFrame consisting of names which match up.
    """
    results = fpd.fuzzy_merge(df_input, df_names,
                left_on="author_login",
                right_on="name",
                method="levenshtein",
                threshold=0.72,
                keep="match")
    return results

results = run_stuff(df_input, df_names)
