#! /bin/bash
source /Users/ZhenxinLei/riverrocktech/bin/activate

export PYTHONPATH=$PYTHONPATH:/Users/ZhenxinLei/MyWork/quantamental:

python3.6 /Users/ZhenxinLei/MyWork/quantamental/spac/SpacWatcher.py -m news
python3.6 /Users/ZhenxinLei/MyWork/quantamental/spac/SpacWatcher.py -m 1h