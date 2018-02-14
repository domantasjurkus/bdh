## bdh

Assumptions:
- All revision entries have a `REVISION`, `MAIN` and `TALK` section.

Design decision:
- Program is split into three jobs:
  - Clean output into a line format: `article_title 1.0 outlink outlink outlink ...`
  - Iterate to update score
  - Save output in format: `article_id score`
- Only the most recent revision for an article is kept.
