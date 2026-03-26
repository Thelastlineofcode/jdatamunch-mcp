# jdatamunch-mcp — Token Efficiency Benchmark

**Tokenizer:** `cl100k_base` (tiktoken)  
**Workflow:** `describe_dataset` + `describe_column` (per task)  
**Baseline:** full raw source file tokenized (minimum for "read everything" agent)  
**AI summaries:** disabled (clean retrieval-only measurement)  

## crime.csv ([source](https://catalog-beta.data.gov/dataset/crime-data-from-2020-to-present))

| Metric | Value |
|--------|-------|
| Rows | **1,004,894** |
| Columns | **28** |
| File size | **255.5 MB** |
| Baseline tokens (full file) | **111,028,360** |

| Task | Baseline&nbsp;tokens | jDataMunch&nbsp;tokens | Reduction | Ratio |
|------|---------------------:|-----------------------:|----------:|------:|
| `schema overview` | 111,028,360 | 3,849 | **100.0%** | 28846.0x |
| `crime type distribution` | 111,028,360 | 4,630 | **100.0%** | 23980.2x |
| `temporal range` | 111,028,360 | 4,736 | **100.0%** | 23443.5x |
| `victim demographics` | 111,028,360 | 4,442 | **100.0%** | 24995.1x |
| `geographic coverage` | 111,028,360 | 4,371 | **100.0%** | 25401.1x |
| **Average** | — | — | **100.0%** | **25333.2x** |

<details><summary>Token breakdown by tool call + latency</summary>

| Task | describe_dataset | describe_column | Column | Latency&nbsp;ms |
|------|----------------:|----------------:|--------|----------------:|
| `schema overview` | 3,849 | 0 | — | 35 |
| `crime type distribution` | 3,847 | 783 | Crm Cd Desc | 22 |
| `temporal range` | 3,849 | 887 | DATE OCC | 24 |
| `victim demographics` | 3,848 | 594 | Vict Age | 33 |
| `geographic coverage` | 3,849 | 522 | AREA NAME | 24 |

</details>

---

## Grand Summary

| | Tokens |
|--|-------:|
| Baseline total (5 task-runs) | 555,141,800 |
| jDataMunch total | 22,028 |
| **Reduction** | **100.0%** |
| **Ratio** | **25201.6x** |

> Measured with tiktoken `cl100k_base`. Baseline = full raw file tokenized. jDataMunch = describe_dataset + describe_column per task. AI summaries disabled.