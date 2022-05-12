#!/usr/bin/env python

import pandas as pd
import matplotlib.pyplot as plt

def calculate_correlation_stats(x,y):

    df = pd.DataFrame({'x':x,'y':y})

    # coefficient of correlation
    pearsons_r = df.corr()['x'][1]

    # other stats building blocks
    df['error'] = df['y'] - df['x']
    df['error_sq'] = df['error'] ** 2
    df['abs_error'] = abs(df['error'])
    df['abs_percent_error'] = 100 * abs(df['error'] / df['x'])

    # error and bias stats
    RMSE = df['error_sq'].mean() ** 0.5
    
    MAE = df['abs_error'].mean()
    DAE = df['abs_error'].median()

    MAPE = df['abs_percent_error'].mean()
    DAPE = df['abs_percent_error'].median()

    mean_bias = df['error'].mean()
    median_bias = df['error'].median()
    
    # create text block
    txt = ('Pearson\'s r: ' + str(round(pearsons_r,3)) + '\n' +
           'RMSE: ' + str(round(RMSE,3))  + '\n\n' +
           'MAE: ' + str(round(MAE,3)) + '\n' +
           'MAPE: ' + str(round(MAPE,3))  + '\n' +
           'mean bias: ' + str(round(mean_bias,3)) + '\n\n' +
           'DAE: ' + str(round(DAE,3)) + '\n' +
           'DAPE: ' + str(round(DAPE,3)) + '\n' +
           'median bias: ' + str(round(median_bias,3)) 
          )
    return txt

def correlation_scatter(x,y,
                        group=pd.Series(),
                        marker_size = 8,
                        xlab='please add x-axis label',
                        ylab='please add y-axis label',
                        title='',
                        stats=False):
    
    # plot groups as different colors if there are between 2 and 20 groups
    use_groups = False
    num_grps = len(group.unique()) 
    if (num_grps > 1 & num_grps < 21):
        use_groups = True
    
    if use_groups:
        grps = sorted(group.unique())
        cmap=plt.get_cmap("tab20")
        colmap = {}
        for col,grp in zip(cmap.colors,grps):
            colmap[grp] = col   

        df = pd.DataFrame({'x':x,'y':y,'group':group})

        for grp in grps:
            subdf = df[df['group']==grp]
            plt.scatter(subdf['x'],subdf['y'],
                        s=marker_size,color=colmap[grp],label=grp)

    # don't use groups; just plot them the same color without a legend
    else:
        plt.scatter(x,y,s=marker_size)
        group = False
    
    # figure out the axis limits
    smallest = min(min(x),min(y))
    largest = max(max(x),max(y))
    margin = (largest-smallest)*0.05
    low = smallest-margin
    high = largest+margin
    
    # one-to-one line
    plt.plot([low,high],[low,high], color='k', linestyle='-', linewidth=1)
    
    # axes config and title
    plt.xlim(low,high)
    plt.ylim(low,high)
    plt.xlabel(xlab)
    plt.ylabel(ylab)
    plt.title(title)
    
    # legend outside
    if use_groups:
        plt.legend(loc='upper left',bbox_to_anchor=(1.6, 1))
        #plt.legend(loc='upper left',bbox_to_anchor=(2.6, 1))
        #plt.legend(loc='lower right',bbox_to_anchor=(1, 1))
    
    # statistics
    if stats:
        plt.figtext(0.8,0.85,
                    calculate_correlation_stats(x,y),
                    va='top')

    # make it a square plot
    plt.gca().set_aspect('equal', adjustable='box')
    plt.show()    

def correlation_scatter_with_stats(x,y,
                                   group=pd.Series(),
                                   marker_size = 8,
                                   xlab='please add x-axis label',
                                   ylab='please add y-axis label',
                                   title=''):
    correlation_scatter(x,y,group,marker_size,xlab,ylab,title,True)

