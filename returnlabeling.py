from return_labeling_functions import (
    fill_return_label_stock_shortdb,
    fill_return_label_stock_long,
    fill_return_label_stock_execution_short,
    fill_return_label_stock_execution_long,
)


# -------------------------
# LONG (stock_bars_enriched_5m)
# -------------------------
fill_return_label_stock_long("opt_ret_10m", 10, "ASC")
fill_return_label_stock_long("opt_ret_1h",  60, "ASC")

fill_return_label_stock_long("opt_ret_eod", 0, "DESC")
fill_return_label_stock_long("opt_ret_next_open", 1440, "ASC")

fill_return_label_stock_long("opt_ret_1d", 1440, "ASC")
fill_return_label_stock_long("opt_ret_2d", 2880, "ASC")
fill_return_label_stock_long("opt_ret_3d", 4320, "ASC")


# -------------------------
# SHORTDB (stock_bars_enriched_5m_3d)
# -------------------------
fill_return_label_stock_shortdb("opt_ret_10m", 10, "ASC")
fill_return_label_stock_shortdb("opt_ret_1h",  60, "ASC")

fill_return_label_stock_shortdb("opt_ret_eod", 0, "DESC")
fill_return_label_stock_shortdb("opt_ret_next_open", 1440, "ASC")

fill_return_label_stock_shortdb("opt_ret_1d", 1440, "ASC")
fill_return_label_stock_shortdb("opt_ret_2d", 2880, "ASC")
fill_return_label_stock_shortdb("opt_ret_3d", 4320, "ASC")


# -------------------------
# EXECUTION LONG (stock_execution_signals_5m)
# -------------------------
fill_return_label_stock_execution_long("opt_ret_10m", 10, "ASC")
fill_return_label_stock_execution_long("opt_ret_1h",  60, "ASC")

fill_return_label_stock_execution_long("opt_ret_eod", 0, "DESC")
fill_return_label_stock_execution_long("opt_ret_next_open", 1440, "ASC")

fill_return_label_stock_execution_long("opt_ret_1d", 1440, "ASC")
fill_return_label_stock_execution_long("opt_ret_2d", 2880, "ASC")
fill_return_label_stock_execution_long("opt_ret_3d", 4320, "ASC")


# -------------------------
# EXECUTION SHORT (stock_execution_signals_5m_3d)
# -------------------------
fill_return_label_stock_execution_short("opt_ret_10m", 10, "ASC")
fill_return_label_stock_execution_short("opt_ret_1h",  60, "ASC")

fill_return_label_stock_execution_short("opt_ret_eod", 0, "DESC")
fill_return_label_stock_execution_short("opt_ret_next_open", 1440, "ASC")

fill_return_label_stock_execution_short("opt_ret_1d", 1440, "ASC")
fill_return_label_stock_execution_short("opt_ret_2d", 2880, "ASC")
fill_return_label_stock_execution_short("opt_ret_3d", 4320, "ASC")



import datetime

print(datetime.datetime.now())
