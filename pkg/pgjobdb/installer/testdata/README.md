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
connection. Explicit migration composes the retained-goal phase and advances it to format 14.


`format11.sql` is the unchanged scheduler SQL from
`5acfe712ea0591400e6406d465b1d75ea3809207`, path
`pkg/pgjobdb/pgjobdb.sql`. SHA256:
`33cc0fd5302843060e2cf5d86c0ecdfae67cb49234d4cede52082cf405a6b885`.
It includes genuine selected-goal preparations and accepted receipts before the
format-12 scheduler retirement. `TestGoalConversionHistoricalFormats` populates
all four authentic formats through their original APIs and converts them to 14.
These fixtures are frozen inputs; changing only a current schema marker is not a
historical upgrade test.

`format13.sql` is the assembled format-13 program from
`15ae3bf2330bcc9bf3964999b0e3415d9ad393b9`: `pgjobdb.sql` through its final
marker update, with `retained_conversion.sql` and `goal_discovery.sql` inserted in
the original `pgjobdb_embed.go` order before that update. SHA256:
`6e5ac8b4141111b83405b156d72a691b0232190ceb6e0834bd8fb01d5c436cd5`.
The installer uses these same frozen bytes for the historical intermediate phase.
Tests install the genuine program and populate it through the original APIs;
no current-format marker is relabeled as an older fixture.
