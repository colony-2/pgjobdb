`format9.sql` is the unchanged scheduler schema from checkout base
`d03bb35dc0ca3b127ca8d587286b9abf80bf3367`, path
`pkg/pgjobdb/pgjobdb.sql`. `TestLeaseEventsUpgrade` installs it, creates a job and
live lease using the native API, and applies the current installer. This frozen
fixture exercises the real immediate-prior schema, rather than changing a version
number on a current-format installation.

`format10.sql` is the unchanged scheduler schema at execution base
`852d9fc7a77ea105d3fa217bbfdb4dc74bb10f98`, path
`pkg/pgjobdb/pgjobdb.sql`. It retains ordinary preparation and Restart fields
before selected-goal submission adds explicit selection and retained receipts.

`format12.sql` is the unchanged scheduler SQL from
`a13f5e36df9da7efe5083b0b3bffeef22ec949b9`, path
`pkg/pgjobdb/pgjobdb.sql`. `TestSubmissionFormat12Upgrade` creates valid work
with those definitions, reproduces the historical non-UTC read failure, applies
and reapplies the current SQL, then reads and acquires that work through a new
connection. The schema format stays 12.
