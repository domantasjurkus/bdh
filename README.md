## bdh

### Assumptions:
- All revision entries have a `REVISION`, `MAIN` and `TALK` section.
- Assumed that at least iteration of PageRank is run (n >= 1) to form complete set of outlinks, otherwise the initial job will not generate all of the outlinks.
- Duplicates are assumed to be ordered (duplicates are in sequence).
- Most recent revision has the highest revision number.

### Design decisions:
- Intermediate folders are not deleted
- Program is split into three jobs:
  - Clean output into a line format: `article_title 1.0 outlink outlink outlink ...`  
  - Iterate to update score
  - Save output in format: `article_id score`
- Only the most recent revision for an article is kept.

### Design decisions
Initial Job (PageRankInit):
- Input is split by a double new line.
- The outlink list has duplicate outlinks removed. 

Main Job (PageRank):
- Input is in the specified format: `article_title 1.0 outlink outlink outlink ...`
- Mapper emits two types of key:value pairs: 
  - Contribution entry: `<article: contribution>`
  - Out-link entry: `<article: 0.0 DELIMITER [out_links]>`
