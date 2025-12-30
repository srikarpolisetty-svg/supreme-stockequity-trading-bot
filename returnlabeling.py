from return_labeling_functions import (
    fill_return_label_stock_shortdb,
    fill_return_label_stock_long,
    fill_return_label_stock_execution_short,
    fill_return_label_stock_execution_long,
)





fill_return_label_stock_long(
    "opt_ret_10m",
    "f.timestamp >= base.timestamp + INTERVAL '10 minutes'"
)


fill_return_label_stock_long(
    "opt_ret_1h",
    "f.timestamp >= base.timestamp + INTERVAL '1 hour'"
)


fill_return_label_stock_long(
    "opt_ret_eod",
    "DATE(f.timestamp) = DATE(base.timestamp)",
    order_dir="DESC"
)

fill_return_label_stock_long(
    "opt_ret_next_open",
    "DATE(f.timestamp) > DATE(base.timestamp)",
    order_dir="ASC"
)


fill_return_label_stock_long(
    "opt_ret_1d",
    "f.timestamp >= base.timestamp + INTERVAL '1 day'"
)


fill_return_label_stock_long(
    "opt_ret_2d",
    "f.timestamp >= base.timestamp + INTERVAL '2 days'"
)

fill_return_label_stock_long(
    "opt_ret_3d",
    "f.timestamp >= base.timestamp + INTERVAL '3 days'"
)



fill_return_label_stock_shortdb(
    "opt_ret_10m",
    "f.timestamp >= base.timestamp + INTERVAL '10 minutes'"
)

fill_return_label_stock_shortdb(
    "opt_ret_1h",
    "f.timestamp >= base.timestamp + INTERVAL '1 hour'"
)

fill_return_label_stock_shortdb(
    "opt_ret_eod",
    "DATE(f.timestamp) = DATE(base.timestamp)",
    order_dir="DESC"
)

fill_return_label_stock_shortdb(
    "opt_ret_next_open",
    "DATE(f.timestamp) > DATE(base.timestamp)",
    order_dir="ASC"
)

fill_return_label_stock_shortdb(
    "opt_ret_1d",
    "f.timestamp >= base.timestamp + INTERVAL '1 day'"
)

fill_return_label_stock_shortdb(
    "opt_ret_2d",
    "f.timestamp >= base.timestamp + INTERVAL '2 days'"
)

fill_return_label_stock_shortdb(
    "opt_ret_3d",
    "f.timestamp >= base.timestamp + INTERVAL '3 days'"
)





fill_return_label_stock_execution_long(
    "opt_ret_10m",
    "f.timestamp >= base.timestamp + INTERVAL '10 minutes'"
)

fill_return_label_stock_execution_long(
    "opt_ret_1h",
    "f.timestamp >= base.timestamp + INTERVAL '1 hour'"
)

fill_return_label_stock_execution_long(
    "opt_ret_eod",
    "DATE(f.timestamp) = DATE(base.timestamp)",
    order_dir="DESC"
)

fill_return_label_stock_execution_long(
    "opt_ret_next_open",
    "DATE(f.timestamp) > DATE(base.timestamp)",
    order_dir="ASC"
)

fill_return_label_stock_execution_long(
    "opt_ret_1d",
    "f.timestamp >= base.timestamp + INTERVAL '1 day'"
)

fill_return_label_stock_execution_long(
    "opt_ret_2d",
    "f.timestamp >= base.timestamp + INTERVAL '2 days'"
)

fill_return_label_stock_execution_long(
    "opt_ret_3d",
    "f.timestamp >= base.timestamp + INTERVAL '3 days'"
)



fill_return_label_stock_execution_short(
    "opt_ret_10m",
    "f.timestamp >= base.timestamp + INTERVAL '10 minutes'"
)

fill_return_label_stock_execution_short(
    "opt_ret_1h",
    "f.timestamp >= base.timestamp + INTERVAL '1 hour'"
)

fill_return_label_stock_execution_short(
    "opt_ret_eod",
    "DATE(f.timestamp) = DATE(base.timestamp)",
    order_dir="DESC"
)

fill_return_label_stock_execution_short(
    "opt_ret_next_open",
    "DATE(f.timestamp) > DATE(base.timestamp)",
    order_dir="ASC"
)

fill_return_label_stock_execution_short(
    "opt_ret_1d",
    "f.timestamp >= base.timestamp + INTERVAL '1 day'"
)

fill_return_label_stock_execution_short(
    "opt_ret_2d",
    "f.timestamp >= base.timestamp + INTERVAL '2 days'"
)

fill_return_label_stock_execution_short(
    "opt_ret_3d",
    "f.timestamp >= base.timestamp + INTERVAL '3 days'"
)
