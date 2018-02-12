## bdh

Assumptions:
- All revision entries have a `REVISION`, `MAIN` and `TALK` section.

Design decision:
- Program is split into three jobs:
  1. Clean output into a line format: `article_title 1.0 outlink outlink outlink ...`
  2. Iterate to update score
  3. Save output in format: `article_id score`
- Only the most recent revision for an article is kept.
