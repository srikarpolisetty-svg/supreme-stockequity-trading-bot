from return_labeling_functions import fill_return_label_stock
from return_labeling_functions import fill_return_label_stock_execution

# -------------------------
# LONG (stock_bars_enriched_5m)
# -------------------------
fill_return_label_stock("opt_ret_10m", 10, "ASC")
fill_return_label_stock("opt_ret_1h",  60, "ASC")

fill_return_label_stock("opt_ret_eod", 0, "DESC")
fill_return_label_stock("opt_ret_next_open", 1440, "ASC")

fill_return_label_stock("opt_ret_1d", 1440, "ASC")
fill_return_label_stock("opt_ret_2d", 2880, "ASC")
fill_return_label_stock("opt_ret_3d", 4320, "ASC")



# -------------------------
# EXECUTION LONG (stock_execution_signals_5m)
# -------------------------
fill_return_label_stock_execution("opt_ret_10m", 10, "ASC")
fill_return_label_stock_execution("opt_ret_1h",  60, "ASC")

fill_return_label_stock_execution("opt_ret_eod", 0, "DESC")
fill_return_label_stock_execution("opt_ret_next_open", 1440, "ASC")

fill_return_label_stock_execution("opt_ret_1d", 1440, "ASC")
fill_return_label_stock_execution("opt_ret_2d", 2880, "ASC")
fill_return_label_stock_execution("opt_ret_3d", 4320, "ASC")



import datetime

print(datetime.datetime.now())
