#!/usr/bin/env bash

cd ../BQ\ Results/comments

mv training.csv ../../ml/training.txt
mv test.csv ../../ml/test.txt

cd ../../ml

for i in training.txt test.txt; do
    sed -i 's/^"//' $i
    sed -i 's/"$//' $i
done

rm  model*
shuf training.txt > temp.txt
head -$(echo "$(echo $(wc -l training.txt) | cut -d" " -f1) / 5" | bc) temp.txt > validate.txt
tail -$(echo "$(echo $(wc -l training.txt) | cut -d" " -f1) / 1.25" | bc) temp.txt > training.txt

rm temp.txt

./fasttext supervised -input training.txt -output model -autotune-validation validate.txt -autotune-duration 300
./fasttext predict model.bin test.txt > predictions.txt