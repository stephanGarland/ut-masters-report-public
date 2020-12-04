#!/usr/bin/env python

import glob
from io import BytesIO
import os
from pathlib import Path
import requests

import numpy as np
import pandas as pd
from PIL import Image

ava_dict = {}
save_path = "$YOUR_PATH"

def get_names():
    """
    This presupposes that you have extracted the author_login column and are globbing only those files.
    It will work on the entire files, but unless you have >= 32 GB of RAM and a fast SSD,
    you're unlikely to be pleased with the results.

    Args:
        None.

    Returns:
        A Pandas DataFrame.
    """
    df_names = pd.concat(
        map(
            pd.read_csv, glob.glob(
                os.path.join(r"$YOUR_CSV_PATH", "$YOUR_PREFIX*.csv")
            )
        )
    )
    df_names = df_names.drop_duplicates()
    df_names.to_csv(r"$YOUR_CSV_PATH", index=False)
    return df_names

def resume(df_names):
    """
    In the event of an interruption, the following code can be used to quickly determine
    what avatars exist vs. what remain to be scraped. Note that np.setdiff1d() 
    returns values (ar1 not in ar2), and is not symmetric, so comment out whichever line
    you don't need. 

    Args:
        df_names: A DataFrame returned from get_names().

    Returns:
        A list containing files that have not yet been downloaded.
    """
    avatars = glob.glob(save_path + "*.png")
    # This will return a list of all filenames you have saved.
    existing_names = [Path(x).name.split(".")[0] for x in avatars]
    # This will return a list of names that exist in df_names but are not yet saved to disk.
    existing_diff = np.setdiff1d(df_names["author_login"].tolist(), existing_names)

    return existing_diff.tolist()

def scrape(ava_dict, df_names):
    """
    This scrapes avatars from githubusercontent.

    Args:
        ava_dict: A dictionary consisting of filename: file_content.
        df_names: A DataFrame with filenames to be downloaded.

    Returns:
        Nothing.

    Raises:
        TypeError: Ignores.
    """
    # Replace the iterable with one generated above if needing to resume scraping.
    for i in df_names:
        if not ava_dict.get(i):
            try:
                r = requests.get("https://avatars.githubusercontent.com/" + i)
                ava_dict[i] = Image.open(BytesIO(r.content))
                # Optional, but provides some measure of progress.
                if not len(ava_dict) % 500:
                    print("Current length: {} out of {}".format(len(ava_dict), len(df_names)))
            # Some avatars resulted in Image.open() parsing errors; they were dropped.
            except TypeError:
                pass
    print("All done")

def save_and_dedupe(ava_dict):
    """
    Saves the dictionary's files to disk, ignoring duplicates for faster I/O.

    Args:
        ava_dict: A dictionary consisting of filename: file_content.

    Returns:
        Nothing.
    """
    dupes = []
    for i in ava_dict:
            file_exists = Path(save_path + i + ".png")
            if not file_exists.is_file():
                ava_dict[i].save(save_path + i + ".png")
            else:
                dupes.append(i)
    print("Saved {} files, ignored {} duplicates".format(
        len(ava_dict) - len(dupes), len(dupes)))

def main():
    df_names = get_names()
    scrape(ava_dict, df_names)
    save_and_dedupe(ava_dict)

