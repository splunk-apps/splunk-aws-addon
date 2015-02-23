[aws_cloudtrail://<name>]
aws_account = AWS account used to connect to AWS
aws_region = AWS region of log notification SQS queue
sqs_queue = log notification SQS queue
exclude_describe_events = boolean indicating whether to exclude read-only events from indexing. defaults to true
remove_files_when_done = boolean indicating whether to remove s3 files after reading defaults to false
blacklist = override regex for the "exclude_describe_events" setting. default regex applied is ^(?:Describe|List|Get)
excluded_events_index = name of index to put excluded events into. default is empty, which discards the events

[aws_cloudwatch://<name>]
aws_account = AWS account used to connect to AWS
aws_region = AWS region of CloudWatch metrics
metric_namespace = CloudWatch metric namespace
metric_names = CloudWatch metric names
metric_dimensions = CloudWatch metric dimensions
statistics = CloudWatch metric statistics being requested
period = CloudWatch metric granularity
polling_interval = Polling interval for statistics

[aws_s3://<name>]
is_secure = whether use secure connection to AWS
host_name = the host name of the S3 service
aws_account = AWS account used to connect to AWS
bucket_name = S3 bucket
key_name = S3 key
recursion_depth = For folder keys, -1 == unconstrained
initial_scan_datetime = Splunk relative time
max_items = Max trackable items.
max_retries = Max number of retry attempts to stream incomplete items.
whitelist = Override regex for blacklist when using a folder key.
blacklist = Keys to ignore when using a folder key.
character_set = The encoding used in your S3 files. Default to 'auto' meaning that file encoding will be detected automatically amoung UTF-8, UTF8 without BOM, UTF-16BE, UTF-16LE, UTF32BE and UTF32LE. Notice that once one specified encoding is set, data input will only handle that encoding.

[aws_billing://<name>]
aws_account = AWS account used to connect to fetch the billing report
bucket_name = S3 bucket
report_file_match_reg = CSV report file in regex, it will override below report type options instead
monthly_report_type = report type for monthly report. options: None, Monthly report, Monthly cost allocation report
detail_report_type = report type for detail report. options: None, Detailed billing report, Detailed billing report with resources and tags

# below items are internally used only
recursion_depth = recursion depth when iterate files
initial_scan_datetime = this option is deprecated
monthly_timestamp_select_column_list = fields of timestamp extracted from monthly report, seperated by '|'
detail_timestamp_select_column_list = fields of timestamp extracted from detail report, seperated by '|'
time_format_list = time format extraction from existing. e.g. "%Y-%m-%d %H:%M:%S" seperated by '|'
max_file_size_csv_in_bytes = max file size in csv file format, default: 50MB
max_file_size_csv_zip_in_bytes = max file size in csv zip format, default: 1GB
header_look_up_max_lines = maximum lines to look up header of billing report
header_magic_regex = regex of header to look up
monthly_real_timestamp_extraction = for monthly report, regex to extract real timestamp in the montlh report, must contains "(%TIME_FORMAT_REGEX%)", which will be replaced with one value defined in "monthly_real_timestamp_format_reg_list"
monthly_real_timestamp_format_reg_list = for monthly report, regex to match the format of real time string. seperated by '|'

[aws_config://<name>]
aws_account = AWS account used to connect to AWS
aws_region = AWS region of log notification SQS queue
sqs_queue = Starling Notification SQS queue
enable_additional_notifications = Enable collection of additional helper notifications