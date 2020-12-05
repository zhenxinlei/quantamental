#! /bin/bash
source /Users/ZhenxinLei/riverrocktech/bin/activate

export PYTHONPATH=$PYTHONPATH:/Users/ZhenxinLei/MyWork/quantamental:

python3.6 /Users/ZhenxinLei/MyWork/quantamental/spac/spac_screener.py

python3.6 /Users/ZhenxinLei/MyWork/quantamental/spac/SpacWatcher.py

python3.6 /Users/ZhenxinLei/MyWork/quantamental/cronjob/factors_monitor.py