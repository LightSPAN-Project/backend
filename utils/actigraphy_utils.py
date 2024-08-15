import numpy as np
import pandas as pd
import plotly.graph_objs as go


def create_dataframe(act_data: dict):
    df = pd.DataFrame.from_dict(act_data)

    df["log_time"] = pd.to_datetime(df["log_time"], format="%Y-%m-%d %H:%M:%S")

    df.sort_values(by=["log_time"], inplace=True)

    df.set_index(df["log_time"], inplace=True)

    return df


def actigraphy_load_data(filename):
    f = open(filename, "r")
    count = 0
    while count < 50:
        count += 1

        # Get next line from file
        line = f.readline()
        if "+-------------------------------------------------------+" in line:
            break
    df = pd.read_csv(filename, delimiter=";", header=count, parse_dates=False)
    df["DATE/TIME"] = pd.to_datetime(df["DATE/TIME"], format="%d/%m/%Y %H:%M:%S")
    df.set_index(df["DATE/TIME"], inplace=True)
    return df


def actigraphy_select_period(
    df: pd.DataFrame, start_date: pd.Timestamp, end_date: pd.Timestamp
):
    index = np.logical_and(df.index >= start_date, df.index < end_date)

    filtered_df = df[index]

    return filtered_df


def actigraphy_split_by_day(df, start_hour=0):
    ldays = []
    ldays_ref = []

    # First day is the start of the day of the first Epoch
    sdate = pd.Timestamp(
        year=df.index[0].year, month=df.index[0].month, day=df.index[0].day
    )
    if df.index[0].hour <= start_hour:
        sdate = sdate - pd.Timedelta(hours=start_hour)
    else:
        sdate = sdate + pd.Timedelta(hours=start_hour)

    while sdate < df.index[-1]:
        day = np.logical_and(
            df.index >= sdate, df.index < sdate + pd.Timedelta(hours=24)
        )
        ldays.append(df[day])
        ldays_ref.append(
            pd.Timestamp(year=sdate.year, month=sdate.month, day=sdate.day)
        )
        sdate += pd.Timedelta(hours=24)

    return ldays, ldays_ref


def rescale(x, min_val, max_val):
    resc = (x - min_val) / (max_val - min_val)
    return resc


def actigraphy_double_plot_actogram(df, column):
    max_val = max(df[column]) * 1.1
    min_val = min(df[column])
    if min_val > 0:
        min_val = min_val * 0.9

    # First day is the start of the day of the first Epoch
    ldays, ldays_ref = actigraphy_split_by_day(df)
    fig = go.Figure()
    i = 0
    for i in range(len(ldays) - 1):
        d1 = ldays[i]
        d2 = ldays[i + 1]
        x1 = d1.index.hour + d1.index.minute / 60.0 + d1.index.second / 3600.0
        x2 = d2.index.hour + d2.index.minute / 60.0 + d2.index.second / 3600.0 + 24.0
        x = np.append(x1, x2)
        y = np.append(d1[column], d2[column])
        y = (i + 1) - rescale(y, min_val, max_val)
        fig.add_trace(go.Scatter(x=x, y=y, line=dict(color="royalblue")))
        i += 1

    if len(ldays) > 0:
        d1 = ldays[-1]
        x = d1.index.hour + d1.index.minute / 60.0 + d1.index.second / 3600.0
        y = d1[column]
        y = (i + 1) - rescale(y, min_val, max_val)
        fig.add_trace(go.Scatter(x=x, y=y, line=dict(color="royalblue")))

    fig.update_yaxes(range=[len(ldays), 0])
    fig.update_layout(showlegend=False)
    fig.update_xaxes(tick0=0, dtick=2)
    fig.update_layout(height=len(ldays) * 70)

    return fig


def create_plots(act_data: dict):
    df = create_dataframe(act_data)

    start_date = pd.Timestamp("2023-06-01 00:00:00")

    end_date: pd.Timestamp = df.index[-1]

    df = actigraphy_select_period(df, start_date, end_date)

    actogram_fig = actigraphy_double_plot_actogram(df, "pim")

    actogram_fig.show()
