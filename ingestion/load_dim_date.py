import sys
import pandas as pd
import requests
from datetime import datetime, timedelta
from connectors.postgres_connector import PostgresConnector

BRAZILIAN_HOLIDAYS_API_URL = "https://brasilapi.com.br/api/feriados/v1/{year}"

def main():
    start_date, end_date = parse_args()
    dim_date_df = generate_dim_date_dataframe(start_date, end_date)
    connector = PostgresConnector.create_from_dot_env_file()
    with connector:
        connector.save_dataframe(
            dim_date_df,
            'dim_date',
            schema='dw',
            if_exists='replace'
        )

def parse_args() -> tuple[datetime, datetime]:
    if len(sys.argv) < 3:
        end_date = datetime.now() + timedelta(days=3 * 365)
    else:
        end_date = datetime.strptime(sys.argv[2], '%Y-%m-%d')
    if len(sys.argv) == 1:
        start_date = datetime(2018, 1, 1)
    else:
        start_date = datetime.strptime(sys.argv[1], '%Y-%m-%d')

    return start_date, end_date

def generate_dim_date_dataframe(start_date: datetime, end_date: datetime) -> pd.DataFrame:
    pure_date_dataframe = generate_pure_date_dataframe(start_date, end_date)
    holidays_dataframe = generate_holidays_dataframe(start_date, end_date)

    joined_dfs = pure_date_dataframe.merge(holidays_dataframe, how='left', on='date')
    joined_dfs["date"] = joined_dfs["date"].dt.date
    with pd.option_context("future.no_silent_downcasting", True): 
        joined_dfs["is_holiday"] = joined_dfs["is_holiday"].fillna(False).astype(bool)
    joined_dfs["is_workday"] = joined_dfs["is_workday"] & ~joined_dfs["is_holiday"]
    columns = ["sk_date", "date", "day", "month", "year", "weekday", "weekday_name", "quarter", "is_weekend", "is_weekday", "is_workday", "is_holiday"]
    return joined_dfs[columns]

def generate_pure_date_dataframe(start_date: datetime, end_date: datetime) -> pd.DataFrame:
    date_range = pd.date_range(start_date, end_date, name='date')
    date_df = pd.DataFrame(date_range, columns=['date'])
    date_df['sk_date'] = date_df['date'].dt.strftime('%Y%m%d').astype(int)
    date_df['day'] = date_df['date'].dt.day
    date_df['month'] = date_df['date'].dt.month
    date_df['year'] = date_df['date'].dt.year
    date_df['weekday'] = date_df['date'].dt.weekday
    date_df['weekday_name'] = date_df['date'].dt.day_name()
    date_df['quarter'] = date_df['date'].dt.quarter
    date_df['is_weekend'] = date_df['weekday'].isin([5, 6])
    date_df['is_weekday'] = ~date_df['is_weekend']
    date_df['is_workday'] = ~date_df['is_weekend']
    return date_df

def generate_holidays_dataframe(start_date: datetime, end_date: datetime) -> pd.DataFrame:
    for year in range(start_date.year, end_date.year + 1):
        holidays_df = get_holidays_from(year)
        if year == start_date.year:
            all_holidays_df = holidays_df
        else:
            all_holidays_df = pd.concat([all_holidays_df, holidays_df])
    return all_holidays_df

def get_holidays_from(year: int) -> pd.DataFrame:
    holidays_response = requests.get(BRAZILIAN_HOLIDAYS_API_URL.format(year=year))
    holidays = holidays_response.json()
    holidays_df = pd.DataFrame(holidays)
    holidays_df['date'] = pd.to_datetime(holidays_df['date'])
    holidays_df['is_holiday'] = True
    holidays_df = holidays_df[['date', 'is_holiday']].drop_duplicates()
    return holidays_df

if __name__ == '__main__':
    main()