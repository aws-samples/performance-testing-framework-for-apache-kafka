# Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
# the Software, and to permit persons to whom the Software is furnished to do so.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
# FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
# IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import re
import pprint
import itertools
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import warnings

key_by_fn = lambda key_by: lambda stat: tuple(stat['test_params'][key] for key in key_by)
map_to_throughput_fn = lambda x: x['test_params']['cluster_throughput_mb_per_sec']


colors = list(mcolors.TABLEAU_COLORS.values())
markers = ['o', 'x', '.', '.']
# linestyles = ['-', '--', '-.', ':']


def plot_measurements(data, metric_line_style_names, ylabel, 
                      xlogscale=False, ylogscale=False, ymin=0, ymax=None, xmin=0, xmax=None, exclude=[], indicate_max=False,
                      ignore_keys = [ 'topic_id', 'cluster_id', 'test_id', 'cluster_name', 'num_producers', ],
                      title_keys = [ 'brokers', 'producers', 'at_rest', 'in_transit', 'duration_sec' ],
                      row_keys = [ 'replication_factor', 'num_partitions' ],
                      column_keys = [ 'num_consumer_groups', 'consumer_props', 'num_producers', 'producer_props' ],
                      metric_color_keys = ['message_size_byte'],
                      legend_replace_str = r"(buffer.memory=\d*|_mean)",
                      linestyles = ['-', '--', ':', '-.']):

    rows_key_by = key_by_fn(row_keys)
    columns_key_by = key_by_fn(column_keys)
    metric_color_key_by = key_by_fn(metric_color_keys)

    num_rows = len(set(map(rows_key_by, data)))
    num_columns = len(set(map(columns_key_by, data)))

    fig, ax = plt.subplots(num_rows, num_columns, figsize=(num_columns*10, num_rows*7), sharex='all', sharey='all', squeeze=False)
    fig.subplots_adjust(hspace=.4)
    
    row_index = 0
    for row_key,row_group in itertools.groupby(sorted(data, key=rows_key_by), key=rows_key_by):
        column_index = 0
        for column_key,column_group in itertools.groupby(sorted(row_group, key=columns_key_by), key=columns_key_by):
            max_throughput_values = set()
            metric_color_index = 0
            for metric_color_key,metric_color_group in itertools.groupby(sorted(column_group, key=metric_color_key_by), key=metric_color_key_by):
                metric_line_style_index = 0

                subplot = ax[row_index][column_index]

                results = sorted(metric_color_group, key=map_to_throughput_fn)
                filtered_results = [result for result in results if result['test_params'] not in exclude]
                                
                test_params = next(map(lambda x: x['test_params'], filtered_results), None)
                
                if not test_params:
                    continue

                for metric_name in metric_line_style_names:
                    data_x = np.array(list(map(map_to_throughput_fn, filtered_results)))
                    data_y = np.array(list(map(lambda x: x['test_results'][metric_name], filtered_results)))
                    
                    if len(np.unique(data_x)) != len(data_x):
                        warnings.warn("Datapoints with identical throughput detected: check the partitions parameter")

                    label = str(dict((k,v) for k,v in zip(metric_color_keys, metric_color_key) if k not in ignore_keys))
                    simplified_label =  re.sub(legend_replace_str, "", "{} {}".format(label, metric_name))
                    
                    
                    subplot.plot(data_x, data_y, label=simplified_label, marker=markers[metric_line_style_index], linestyle=linestyles[metric_line_style_index], color=colors[metric_color_index % len(colors)])                 

                    max_x = max(data_x)
                    metric_line_style_index += 1

                subplot.grid(True)
                subplot.tick_params(labelleft=True, labelbottom=True)
#                subplot.ticklabel_format(useOffset=False)
                subplot.set_xlabel("requested aggregated producer throughput (mb/sec)")
                subplot.set_ylabel(ylabel)
                subplot.legend()
            
                
                title = "{}\n{}\n{}\n".format(
                    str(dict((k, test_params[k]) for k in title_keys if k in test_params and k not in ignore_keys)),
                    str(dict((k,v) for k,v in zip(row_keys, row_key) if k not in ignore_keys)),
                    str(dict((k,v) for k,v in zip(column_keys, column_key) if k not in ignore_keys))
                )
                subplot.set_title(re.sub(legend_replace_str, "_", title))

                if xmax or xmin:
                    subplot.set_xlim(left=xmin, right=xmax)

                if ymax or ymin:
                    subplot.set_ylim(bottom=ymin, top=ymax)

                if xlogscale:
                    subplot.set_xscale('log')

                if ylogscale:
                    subplot.set_yscale('log')
                        
                if indicate_max:
                    max_throughput_values.add(max_x)
                    subplot.axvline(x=max_x, linestyle='-.', color=colors[metric_color_index% len(colors)], alpha=.5)
                    
                metric_color_index += 1
                
            if num_rows<=1 or num_columns<=1:
                subplot.legend(loc='upper center', bbox_to_anchor=(0.5, -0.1))

            column_index += 1
        row_index += 1

    if indicate_max and len(ax)>0:
        subplot = ax[0][0]
        ticks = list(subplot.get_xticks())
                        
        for max_throughput_value in max_throughput_values:
            closest_tick = min(ticks, key=lambda x:abs(x-max_throughput_value))
            ticks = [max_throughput_value if x==closest_tick else x for x in ticks]
        
        subplot.set_xticks(ticks)

        for plots in ax:
            for plot in plots:
                plot.axvline(x=indicate_max, linestyle=':', color=mcolors.CSS4_COLORS['dimgray'])