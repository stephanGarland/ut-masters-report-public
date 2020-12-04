#!/usr/bin/env python

import csv
import os
import re

import nltk
from nltk.stem import WordNetLemmatizer
import pandas as pd

nltk.download('punkt')
nltk.download('wordnet')
stemmer = WordNetLemmatizer()

output_headers = ["idx", "score"]

def reset_df():
    """
    A small helper function to reset the DataFrame during testing.
    
    Args:
        None.
    
    Returns:
        Nothing.
    """
    global sentiment_df
    sentiment_df = pd.DataFrame(columns=output_headers)

def reset_csv():
    """
    A small helper function to reset the outputted CSV and current index during testing.
    
    Args:
        None.
    
    Returns:
        Nothing, but writes to a file.
    """
    global sentiment_path
    with open(sentiment_path, "w") as f:
        f.write("idx, score\n")
    with open(csv_path + "last_idx.txt", "w") as f:
        f.write("0")

def check_sentiment(df, output_file, output_headers=output_headers, start_iloc=0):
    """
    Writes sentiment values for use by supervised learning.
    
    Args:
        df: A Pandas DataFrame consisting of comments to be printed.
        output_file: A CSV file path to be appended to.
        output_headers: A list containing headers for the DataFrame. Defined above.
        start_iloc: The iloc in the DF to start at. Defaults to 0.
    
    Returns:
        Nothing, but writes to a file.
    """
    output_df = pd.DataFrame(columns=["idx", "score"])
    print("\n0:\tharsh")
    print("1:\tnegative")
    print("2:\tneutral")
    print("3:\tpositive")
    print("h:\thelpful")
    print("q:\tsave and quit")
    print("s:\tskip")
    print("t:\tterse")
    for idx, row in df.iloc[start_iloc:].iterrows():
        print("")
        score = input(row[0] + " ")
        while score not in ["0", "1", "2", "3", "h", "q", "s", "t"]:
            print("Error, please select from [0, 1, 2, 3, h, q, s, t]")
            score = input(row[0] + " ")
        if score == "s":
            continue
        if score == "q":
            output_df.to_csv(sentiment_path, mode="a", header=False, index=False)
            with open(csv_path + "last_idx.txt", "w") as last_idx:
                last_idx.write(str(idx - 1))
                print("Index: {}".format(str(idx - 1)))
                break
        else:
            output_df = output_df.append({"idx": idx, "score": score}, ignore_index=True)
    return output_df

def run(sentiment_df):
    """
    Runs the analysis by calling check_sentiment().
    
    Args:
        sentiment_df: A Pandas DataFrame, empty or containing existing sentiments.
        
    Returns:
        A Pandas DataFrame containing the sentiments.
    """
    if sentiment_df.empty:
        ignore_empty = input("The dataframe is empty. Would you like to continue anyway [yn]? ")
        if ignore_empty in "nN":
            print("Exiting - recover the dataframe with pd.read_csv().")
            return
    try:
        with open(csv_path + "last_idx.txt", "r") as f:
            cur_idx = f.read()
    except OSError:
        reset_csv()
        with open(csv_path + "last_idx.txt", "r") as f:
            cur_idx = f.read()

    print("Last index saved: {}".format(cur_idx))
    start = int(input("Start index: "))
    
    return sentiment_df.append(check_sentiment(input_df, output_headers, sentiment_path, start), ignore_index=True)

def preprocess_text(document):
    """
    This was taken from https://stackabuse.com/python-for-nlp-working-with-facebook-fasttext-library/
    and modified for use. The emoticon regex was taken from https://stackoverflow.com/a/59890719/4221094
    
    Specifically, I want to retain emoticons at the expense of stripping out special characters, 
    as they often carry enormous weight in the tone of a PR. Likewise, I am ignoring /?+/ as it 
    is often a comment on its own, albeit a terse one.
    
    Args:
        A string to process.
    
    Returns:
        A processed string.
    """
    # Check for emoticons
    emoticons = re.search(r"(>?[\:\;X][\-=]*[3\)D\(>sp}])", document, re.I)
    
    # Remove all the special characters, excepting "?", if no emoticons present
    if not emoticons:
        document = re.sub(r"(?!\?)\W", " ", str(document))

    # remove all single characters
    document = re.sub(r"\s+[a-zA-Z]\s+", " ", document)

    # Remove single characters from the start
    document = re.sub(r"\^[a-zA-Z]\s+", " ", document)

    # Substituting multiple spaces with single space
    document = re.sub(r"\s+", " ", document, flags=re.I)

    # Removing prefixed "b"
    document = re.sub(r"^b\s+", "", document)

    # Converting to Lowercase
    document = document.lower()

    # Lemmatization
    tokens = document.split()
    tokens = [stemmer.lemmatize(word) for word in tokens]
    #tokens = [word for word in tokens if word not in en_stop]
    tokens = [word for word in tokens if len(word) > 3]

    preprocessed_text = " ".join(tokens)

    return preprocessed_text

def upsampling(input_file, output_file, ratio_upsampling=1):
    """
    This was taken from 
    https://towardsdatascience.com/fasttext-sentiment-analysis-for-tweets-a-straightforward-guide-9a8c070449a2
    without further modification, save for this docstring.
    
    Args:
        input_file: an input CSV containing sentiments.
        output_file: an output CSV containing sentiments, upsampled - can be the same as input_file.
        ratio_upsampling: the ratio of each minority class:majority class.
    
    Returns:
        None, but writes to a file.
    """
    i=0
    counts = {}
    dict_data_by_label = {}
    i=0
    counts = {}
    dict_data_by_label = {}
# GET LABEL LIST AND GET DATA PER LABEL
    with open(input_file, 'r', newline='') as csvinfile:
        csv_reader = csv.reader(csvinfile, delimiter=',', quotechar='"')
        for row in csv_reader:
            counts[row[0].split()[0]] = counts.get(row[0].split()[0], 0) + 1
            if not row[0].split()[0] in dict_data_by_label:
                dict_data_by_label[row[0].split()[0]]=[row[0]]
            else:
                dict_data_by_label[row[0].split()[0]].append(row[0])
            i=i+1
            if i%10000 ==0:
                print("read" + str(i))
# FIND MAJORITY CLASS
    majority_class=""
    count_majority_class=0
    for item in dict_data_by_label:
        if len(dict_data_by_label[item])>count_majority_class:
            majority_class= item
            count_majority_class=len(dict_data_by_label[item])  
    
    # UPSAMPLE MINORITY CLASS
    data_upsampled=[]
    for item in dict_data_by_label:
        data_upsampled.extend(dict_data_by_label[item])
        if item != majority_class:
            items_added=0
            items_to_add = count_majority_class - len(dict_data_by_label[item])
            while items_added<items_to_add:
                data_upsampled.extend(
                    dict_data_by_label[item][:max(
                        0,min(items_to_add-items_added,len(dict_data_by_label[item])))
                        ]
                )
                items_added = items_added + max(0,min(
                    items_to_add-items_added,len(dict_data_by_label[item]))
                    )
# WRITE ALL
    i=0
    with open(output_file, 'w') as txtoutfile:
            for row in data_upsampled:
                txtoutfile.write(row+ '\n' )
                i=i+1
                if i%10000 ==0:
                    print("writer" + str(i))

def post_process(sentiment_path,
                 df_comments,
                 upsample=False,
                 preprocess=True,
                 custom=False,
                 simple=False,
                 concat=False
                ):
    """
    Runs post-processing (and calls preprocess()) to generate labeled comments,
    and optionally upsamples the data to have more equal sentiments.
    
    This should ideally be split into separate functions.
    
    Args:
        sentiment_path: The path to the sentiment file.
        df_comments: The main Pandas DataFrame containing comments.
        upsample: If True, upsamples the data. Defaults to False.
        preprocess: If True, preprocesses the data. Defaults to True.
        custom: If True, appends custom comments for processing. Expects a list of lists, defaults to False.
        simple: If True, uses a simplified classifier. Defaults to False.
        concat: If True, appends to a shared file. Defaults to False.
    
    Returns:
        Nothing, but writes to files.
    """
    append_or_overwrite = "a" if concat else "w"
    sentiments = pd.read_csv(sentiment_path)
    training_path = csv_path + "training.csv"
    test_path = csv_path + "test.csv"
    df_merged = df_comments.merge(sentiments, left_index=True, right_on="idx")
    df_merged = df_merged.rename(columns=
                                 {
                                     0: "comment",
                                     "idx": "idx",
                                     "score": "score"
                                 }
                                )
    
    def pre_process(input_df, preprocess):
        """
        Appends necessary columns to a list, optionally running pre-processing.

        Args:
            input_df: An input Pandas DataFrame, or a list of custom comments.
            preprocess: If True, preprocesses the data. Called from parent function post_process().

        Returns:
            A list.
        """
        processed_list = []
        if type(input_df) == pd.core.frame.DataFrame:
            for x in input_df.iloc[:,[0,2]].values.tolist():
                if preprocess:
                    processed_list.append([preprocess_text(x[0]), str(x[1])])
                else:
                    processed_list.append([x[0], x[1]])
        else:
            for x in input_df:
                if preprocess:
                    processed_list.append([preprocess_text(x[0]), str(x[1])])
                else:
                    processed_list.append([x[0], x[1]])
        return processed_list
    
   
    def list_labeler(input_list, simple):
        """
        Converts added labels to fastText format.

        Args:
            input_list: The input list containing coded labels.
            simple: Boolean that determines the complexity of the classier. Obtained from parent function.

        Returns:
            A list containing fastText labels and comments.

        """
        labeled_list = []
        for x in range(len(input_list)):
            if not simple:
                if input_list[x][1] == "0":
                    labeled_list.append("__label__harsh " + input_list[x][0])
                elif input_list[x][1] == "1":
                    labeled_list.append("__label__negative " + input_list[x][0])
                elif input_list[x][1] == "2":
                    labeled_list.append("__label__neutral " + input_list[x][0])
                elif input_list[x][1] == "3":
                    labeled_list.append("__label__positive " + input_list[x][0])
                elif input_list[x][1] == "h":
                    labeled_list.append("__label__helpful " + input_list[x][0])
                elif input_list[x][1] == "t":
                    labeled_list.append("__label__terse " + input_list[x][0])
            else:
                if input_list[x][1] in "01t":
                    labeled_list.append("__label__negative " + input_list[x][0])
                elif input_list[x][1] == "2":
                    labeled_list.append("__label__neutral " + input_list[x][0])
                elif input_list[x][1] in "3h":
                    labeled_list.append("__label__positive " + input_list[x][0])
            
        return labeled_list
    
    if preprocess:
        processed_list = pre_process(df_merged, True)
    else:
        processed_list = pre_process(df_merged, False)
    labeled_list = list_labeler(processed_list, simple)

    if custom:
        if preprocess:
            custom_list = pre_process(custom, True)
        else:
            custom_list = pre_process(custom, False)
        custom_list = list_labeler(custom, simple)
        for x in custom_list:
            labeled_list.append(x)

    pd.DataFrame(labeled_list).to_csv(training_path, header=False, index=False, mode=append_or_overwrite)
    
    # You should change this to a different range if you go this far into the dataset.
    df_comments[10000:12000].to_csv(test_path, header=False, index=False)
    if upsample:
        upsampling(training_path, training_path)

def make_custom():
    """
    Makes a custom set of comments to be trained on.
    
    Args:
        None.
    
    Returns:
        A formatted list for use by post_process()
    """
    
    custom_list = []
    print("\n0:\tharsh")
    print("1:\tnegative")
    print("2:\tneutral")
    print("3:\tpositive")
    print("h:\thelpful")
    print("q:\tsave and quit")
    print("t:\tterse")
    
    while True:
        comment = input("\nPlease enter a comment; case is insensitive: ")
        if comment == "q":
            break
        score = input("Please score that comment per the above key: ")
        while score not in ["0", "1", "2", "3", "h", "q", "t"]:
            print("Error, please select from [0, 1, 2, 3, h, q, t]")
            score = input("Please score that comment per the above key: ")
        custom_list.append([comment, score])
    # Save it as a CSV as a backup
    pd.DataFrame(custom_list).to_csv(csv_path + "custom_list.csv", header=False, index=False)
    
    return custom_list

# Anything you put in here will be evaluated literally, so be careful.
csv_path = input("Path to CSVs (hint: can type os.getcwd(): ")
csv_path = eval(csv_path) if csv_path == "os.getcwd()" else csv_path
csv_path = csv_path + "/"
input_csv_file = input("CSV filename without extension: ")
input_csv_file_path = csv_path + input_csv_file + ".csv"
sentiment_path = csv_path + input_csv_file + "_sentiments.csv"

try:
    input_df = pd.read_csv(input_csv_file_path, header=None)
except FileNotFoundError:
    reset_csv()
    input_df = pd.read_csv(input_csv_file_path, header=None)

custom_list = make_custom()

try:
    sentiment_df = run(sentiment_df)
except NameError:
    reset_df()
    sentiment_df = run(sentiment_df)

# This is used to load in previously scored sentiments to process them and merge.
# It will first overwrite any existing files; use append in the second run of the current data.
# It is far more useful if you're running this in a Jupyter Notebook.
#month_to_load = "jan"
#past_sentiment_path = csv_path + "sentiments_" + month_to_load + ".csv"
#past_month_df = pd.read_csv(csv_path + month_to_load + "_comments.csv.gz", header=None)
#post_process(past_sentiment_path,
#             past_month_df,
#             upsample=True,
#             preprocess=True,
#             custom=custom_list,
#             simple=False,
#             concat=False)

# As above, these are more useful if running this in a Jupyter Notebook.
#reset_csv()
#reset_df()
