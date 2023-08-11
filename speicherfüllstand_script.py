import pandas as pd
import numpy as np
import datetime as datetime
from datetime import timedelta


# Shared function um datarange und min max mean zu definieren
def TimeseriesToMinMaxMeanViz(data, date, category, value, values_absolute, time_resolution, trend_breaks, trend, trendRating):
# data: input dataframe
# date: name of attribute with date in %Y-%m-%d
# category: name of attribute with category
# value: name of attribute with value to take min max mean from, numeric
# values_absolute: list of absolut values which should also be in output. use [] if it should be ignored
# time_resolution: options: daily, weekly, monthly. weekly will be extended automatically to daily. 
# trend_breaks: borders of bins, ex: [-100, -1.5, -0.3, 0.3, 1.5, 100]
# trend: bins for trend as list, ex: ['down_strong', 'down_mild', 'neutral', 'up_mild', 'up_strong']
# trendRating : rating of bins as list, ex: ['negativ', 'negativ', 'neutral', 'positiv', 'positiv']

    # Vorbereitungen
    df = data.copy()
    df.Datum = pd.to_datetime(df.Datum, format='%Y-%m-%d')
    df[value] = pd.to_numeric(df[value])
    if len(values_absolute) > 0:
        attributes = [date, category, value] + values_absolute
        df = df[attributes]
    else:
        df = df[[date, category, value]]        

    # most recent date in data
    most_recent_date = df[date].max()

    #today = pd.Timestamp.today()
    yearMin5 = most_recent_date.year-5    
    
    # Nur Daten der letzten 6 Jahre
    df = df[df[date] >= pd.to_datetime(str(yearMin5) + "-01-01", format='%Y-%m-%d')]
    
    if time_resolution == "weekly":
        
        list = df[category].unique()

        for entry in list:

            # Timerange with all days
            df_timerange = pd.date_range(start=str(yearMin5) + "-01-01",end=most_recent_date).to_frame()
            df_timerange.reset_index(inplace=True)
            df_timerange = df_timerange.drop(columns=['index'])
            df_timerange = df_timerange.rename(columns={0: date})
            df_timerange[category] = entry

            # merge
            df = df_timerange.merge(df, how='outer', on=[date, category])

        df.sort_values(by=[category, date], inplace=True)

        # interpolate NA (linear)
        df[value] = df.groupby(category, group_keys=False)[value].apply(lambda group: group.interpolate(method="linear"))
        df[value] = df.groupby(category, group_keys=False)[value].apply(lambda group: group.fillna(method="backfill"))
        
        if len(values_absolute) > 0:
            for value_absolute in values_absolute:
                df[value_absolute] = df.groupby(category, group_keys=False)[value_absolute].apply(lambda group: group.interpolate(method="linear"))
                df[value_absolute] = df.groupby(category, group_keys=False)[value_absolute].apply(lambda group: group.fillna(method="backfill"))
    
    
    # *****************
    # MIN MAX MEAN der letzten fuenf Jahre berechnen
    # *****************    
    
    df_minmax = df[[date, category, value]]
    
    # aktuelles Jahr ausschliessen
    df_minmax = df_minmax[df_minmax[date] < pd.to_datetime(str(most_recent_date.year) + "-01-01", format='%Y-%m-%d')]

    # Min Max der letzten fuenf Jahre berechnen
    df_minmax["Monat_Tag"] = df_minmax[date].dt.strftime('%m%d')

    df_minmax = df_minmax.groupby([category,"Monat_Tag"]).agg({value: ['min', 'max', 'mean']})
    df_minmax.reset_index(inplace=True)    

    # Min Max Ausdehnen aus ingesamt drei Jahre (aktuel, -1, +1), so dass wir kein JahresendeProblem haben
    jetzt = pd.Timestamp.today()
    Jahr_aktuell = jetzt.strftime("%Y")
    Jahr_aktuell_int = int(Jahr_aktuell)
    Jahr_aktuell_minus1 = str(Jahr_aktuell_int-1)
    Jahr_aktuell_plus1 = str(Jahr_aktuell_int+1)
    
    df_minmax_minus1 = df_minmax.copy()
    df_minmax_minus1[date] = Jahr_aktuell_minus1 + df_minmax["Monat_Tag"]
    df_minmax_plus1 = df_minmax.copy()
    df_minmax_plus1[date] = Jahr_aktuell_plus1 + df_minmax["Monat_Tag"]

    df_minmax[date] = Jahr_aktuell + df_minmax["Monat_Tag"]

    df_minmax_add = pd.concat([df_minmax_minus1, df_minmax_plus1])
    df_minmax = pd.concat([df_minmax_add, df_minmax])

    df_minmax = df_minmax[df_minmax["Monat_Tag"] != "0229"]
    df_minmax.Datum = pd.to_datetime(df_minmax.Datum, format='%Y%m%d')
    df_minmax.columns = df_minmax.columns.droplevel(1)
    df_minmax.drop(columns=['Monat_Tag'], inplace=True)
    df_minmax.columns.values[1] = "5y_Min"
    df_minmax.columns.values[2] = "5y_Max"
    df_minmax.columns.values[3] = "5y_Mittelwert"
    df_minmax.sort_values(by=[category, date], inplace=True)    
    df_minmax["5y_Min"] = round(df_minmax["5y_Min"],1)
    df_minmax["5y_Max"] = round(df_minmax["5y_Max"],1)
    df_minmax["5y_Mittelwert"] = round(df_minmax["5y_Mittelwert"],1)
 

    # *****************
    # Wir zeigen auch drei Monate in der Zukunft, damit Linie nicht ganz rechts aufhoert.
    # Wir muessen daher die Fuellungsgrade ergaenzen
    # *****************
    
    # heute vor einem Jahr / soweit zurueck zeigen wir die Fuellungsgrade
    dt = pd.Timestamp.today() # Heute
    dt = dt - timedelta(days=365) # Heute -1  Jahr
    dt_3months = pd.Timestamp.today() + pd.offsets.DateOffset(months=3) # Heute + 3 Monate

    # Nur Daten des letzten Jahres
    df_kpi = df[[date, category, value]]
    df_kpi = df[df['Datum'] >= dt]

    # *****************
    # merge 3 Jahre Min Max und 1 Jahr aktuelle Daten
    df_aktuell = df_minmax.merge(df_kpi, how='outer', on=[date, category])    

    # 15 Monate: 12 past + 3 future
    df_aktuell = df_aktuell[(df_aktuell[date] < dt_3months) & (df_aktuell[date] >= dt)]    

    # runden ordnen
    df_aktuell[value] = round(df_aktuell[value],1)
    df_aktuell = df_aktuell[[category, date, value, "5y_Min", "5y_Max", "5y_Mittelwert"]]

    # Differenzen
    df_aktuell["Differenz_Mittelwert"] = round(df_aktuell[value]-df_aktuell["5y_Mittelwert"],1)
    df_aktuell["Differenz_Min"] = round(df_aktuell[value]-df_aktuell["5y_Min"],1)
    df_aktuell["Differenz_Max"] = round(df_aktuell[value]-df_aktuell["5y_Max"],1)

    # preparation for Trend
    df_aktuell.reset_index(inplace=True)
    df_aktuell = df_aktuell.drop(columns=['index'])

    # Rolling Mean
    rolling_mean = df_aktuell.groupby(category, group_keys=True)[value].apply(lambda x: x.rolling(10).mean()).reset_index()[value].to_frame()
    rolling_mean.columns.values[0] = "Rolling_Mean"
    df_aktuell = pd.concat([df_aktuell, rolling_mean], axis=1)
    
    # Trend_pp
    if time_resolution == "weekly":
        shift_distance = 7
    else:
        shift_distance = 1
    df_aktuell['Trend_pp'] = round(df_aktuell['Rolling_Mean']-df_aktuell.groupby([category])['Rolling_Mean'].shift(shift_distance),1)
    
    # Trend
    s=pd.Series(trend_breaks)
    trendNumber = len(trend_breaks)
    df_aktuell['Trend'] = pd.cut(df_aktuell['Trend_pp'], s, trendNumber, labels=trend)
    df_aktuell['TrendRating'] = pd.cut(df_aktuell['Trend_pp'], s, trendNumber, labels=trendRating, ordered=False)
    
    # clean
    df_aktuell = df_aktuell.drop(columns=['Rolling_Mean', 'Trend_pp'])

    # add absolut value
    if len(values_absolute) > 0:
        attributes = [date, category] + values_absolute
        df_absolute = df[attributes]
        df_aktuell = df_aktuell.merge(df_absolute, on=[date, category], how="left")
        for value_absolute in values_absolute:
            df_aktuell[value_absolute] = round(df_aktuell[value_absolute],0)

    # weekly data is interpolated. no values for newer dates than the last observation should be in data
    if time_resolution == "weekly":
        attributes = values_absolute + [value, "Differenz_Mittelwert", "Differenz_Min", "Differenz_Max", "Trend", "TrendRating"]
        df_aktuell.loc[df_aktuell[date] > most_recent_date, attributes] = np.nan
         
    # sort
    df_aktuell.sort_values(by=[category, date], inplace=True)

    return df_aktuell

#############################
##### Eigentliches Script
#############################

file_input = "https://www.uvek-gis.admin.ch/BFE/ogd/17/ogd17_fuellungsgrad_speicherseen.csv"
file_output = "<file_output>"

# read data
df = pd.read_csv(file_input, sep=",")

# unpivot    
df1 = df[['Datum', 'Wallis_speicherinhalt_gwh', 'Graubuenden_speicherinhalt_gwh',
       'Tessin_speicherinhalt_gwh', 'UebrigCH_speicherinhalt_gwh',
       'TotalCH_speicherinhalt_gwh']]
df1 = pd.melt(df1, id_vars=['Datum'], value_vars=['Wallis_speicherinhalt_gwh', 'Graubuenden_speicherinhalt_gwh',
       'Tessin_speicherinhalt_gwh', 'UebrigCH_speicherinhalt_gwh',
       'TotalCH_speicherinhalt_gwh'])

df1["Region"] = df1["variable"].str.split('_', n=1, expand=True)[0]
df1 = df1.drop(columns=['variable'])

df2 = df[['Datum', 'Wallis_max_speicherinhalt_gwh',
       'Graubuenden_max_speicherinhalt_gwh', 'Tessin_max_speicherinhalt_gwh',
       'UebrigCH_max_speicherinhalt_gwh', 'TotalCH_max_speicherinhalt_gwh']]
df2 = pd.melt(df2, id_vars=['Datum'], value_vars=['Wallis_max_speicherinhalt_gwh',
       'Graubuenden_max_speicherinhalt_gwh', 'Tessin_max_speicherinhalt_gwh',
       'UebrigCH_max_speicherinhalt_gwh', 'TotalCH_max_speicherinhalt_gwh'])     

df2["Region"] = df2["variable"].str.split('_', n=1, expand=True)[0]
df2 = df2.drop(columns=['variable'])

# merge
df_clean = df1.merge(df2, how='outer', on=['Datum','Region'])

# clean
df_clean = df_clean.rename(columns={
    "Datum": "Datum",
    "value_x": "Speicherinhalt_GWh",
    "Region": "Region",
    "value_y": "Speicherinhalt_100prozent_GWh"})

# Prozent berechnen
df_clean["Speicherstand_prozent"] = round(df_clean["Speicherinhalt_GWh"]/df_clean["Speicherinhalt_100prozent_GWh"]*100,1)

# Get 15 month MinMaxMean from function
df_result = TimeseriesToMinMaxMeanViz(df_clean, # data: input dataframe
                                      "Datum", # date: name of attribute with date in %Y-%m-%d
                                      "Region", # category: name of attribute with category
                                      "Speicherstand_prozent", # value: name of attribute with value to take min max mean from, numeric
                                      ["Speicherinhalt_GWh", "Speicherinhalt_100prozent_GWh"], # values_absolute: absolut values which should also be in output
                                      "weekly", # time_resolution: options: daily, notdaily
                                      [-100, -3.5, -1.5, 1.5, 3.5, 100], # trend_breaks
                                      ['down_strong', 'down_mild', 'neutral', 'up_mild', 'up_strong'], # trend
                                      ['negativ', 'negativ', 'neutral', 'positiv', 'positiv']) # trendRating


df_result['hist_min'] = pd.Series(np.nan, index=df_result.index).mask(df_result['Region']=="TotalCH", 9)
df_result['hist_min_und_Speicherreserve'] = pd.Series(np.nan, index=df_result.index).mask(df_result['Region']=="TotalCH", 14)

# Plot
#df_result[df_result["Region"] == "TotalCH"].plot(x="Datum", y=["5y_Min","5y_Max","5y_Mittelwert","Speicherstand_prozent"])

# write csv
df_result.to_csv(file_output, index=False)




