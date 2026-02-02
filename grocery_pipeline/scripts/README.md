## Scripts README


### eval_silver_matching.py

#### Standard Execution:

```
cd /home/ubuntu/pipelines/grocery_pipeline/grocery_pipeline/scripts
python eval_silver_false_positives.py --out-dir ./eval_outputs
```

#### Knobs:

```
python eval_silver_false_positives.py \
  --batch-size 20 \
  --limit-per-term 200 \
  --model gpt-4.1-mini \
  --out-dir ./eval_outputs
```