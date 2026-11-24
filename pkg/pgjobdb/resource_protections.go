package pgjobdb

// Inspect physical storage as well as function bodies. A marker and a method
// cannot establish retained uniqueness, cleanup ownership or archive preservation.
// Attribute/key checks avoid dependence on the caller's search_path.
const resourceProtectionCatalog = `WITH resource_columns(table_name,column_name,type_name,required) AS (VALUES
 ('resource_configuration','singleton','bool',true),
 ('resource_configuration','protocol_version','int4',true),
 ('resource_configuration','crash_allowance','int8',true),
 ('resource_sequences','tenant_id','text',true),
 ('resource_sequences','job_id','text',true),
 ('resource_sequences','invocation_digest','bytea',true),
 ('resource_sequences','invocation','text',true),
 ('resource_sequences','attempt','int4',true),
 ('resource_sequences','reason','text',true),
 ('resource_sequences','count','int8',true),
 ('resource_sequences','policy','bytea',true),
 ('resource_sequences','allowance','int8',true),
 ('resource_receipts','tenant_id','text',true),
 ('resource_receipts','job_id','text',true),
 ('resource_receipts','lease_id','text',true),
 ('resource_receipts','request_id','text',true),
 ('resource_receipts','worker_id','text',true),
 ('resource_receipts','request','bytea',true),
 ('resource_receipts','receipt','bytea',true),
 ('jobs','crash_budget_remaining','int8',true),
 ('jobs','resource_not_before_ns','int8',false),
 ('jobs','resource_not_before','timestamptz',false),
 ('jobs','resource_deadline','timestamptz',false),
 ('jobs','resource_resume','json',false),
 ('jobs_archive','crash_budget_remaining','int8',true),
 ('jobs_archive','resource_not_before_ns','int8',false),
 ('jobs_archive','resource_not_before','timestamptz',false),
 ('jobs_archive','resource_deadline','timestamptz',false),
 ('jobs_archive','resource_resume','json',false)
), resource_keys(table_name,columns) AS (VALUES
 ('resource_configuration',ARRAY['singleton']),
 ('resource_sequences',ARRAY['tenant_id','job_id','invocation_digest','attempt','reason']),
 ('resource_receipts',ARRAY['tenant_id','job_id','lease_id','request_id'])
), resource_foreign_keys(table_name) AS (VALUES ('resource_sequences'),('resource_receipts')),
resource_checks(table_name,expression) AS (VALUES
 ('resource_configuration','singleton'),
 ('resource_configuration','(protocol_version = 1)'),
 ('resource_configuration','((crash_allowance >= 1) AND (crash_allowance <= 2147483647))'),
 ('resource_sequences','(count > 0)'),
 ('resource_sequences','(allowance > 0)'),
 ('jobs','(crash_budget_remaining >= 0)'),
 ('jobs_archive','(crash_budget_remaining >= 0)')
)`
