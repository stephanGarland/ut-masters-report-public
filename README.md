# Examining Gender Bias of GitHub Pull Request Comments with Machine Learning

This is the public version of my repo, containing only the report and the scripts I used to create it.
I have made the dataset available (sans gender - I'll leave it as an exercise for the reader) on [Kaggle.](https://www.kaggle.com/stephangarland/ghtorrent-pull-requests)

The scripts are licensed under MPL-2.0. If you want to use information in the report, please cite it accordingly:

> Garland, Stephan. “Examining Gender Bias of GitHub Pull Request
> Comments with Machine Learning,” 2020.

## Errata
In section 3.5.1, I erroneously stated that images returned from Rekognition were passed back to Rekognition. The avatars themselves are never used again, but the filenames and respective gender are passed to BigQuery.
