import pandas as pd
import requests
from connectors.postgres_connector import PostgresConnector
from datetime import datetime

INDICATORS = {
    "cdi": {
        "url": "https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados?formato=json&dataInicial={start_date_dd_mm_yyyy}&dataFinal={end_date_dd_mm_yyyy}",
        "granularity": "daily",
    },
    "selic_goal": {
        "url": "https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados?formato=json&dataInicial={start_date_dd_mm_yyyy}&dataFinal={end_date_dd_mm_yyyy}",
        "granularity": "daily",
    },
    "ipca": {
        "url": "https://api.bcb.gov.br/dados/serie/bcdata.sgs.433/dados?formato=json&dataInicial={start_date_dd_mm_yyyy}&dataFinal={end_date_dd_mm_yyyy}",
        "granularity": "monthly",
    },
}
DEFAULT_START_DATE = datetime(2018, 1, 1)
DEFAULT_END_DATE = datetime.now()

def main():
    for indicator_name, indicator_config in INDICATORS.items():
        load_indicator(
            indicator_name,
            indicator_config["url"],
            indicator_config["granularity"],
            DEFAULT_START_DATE,
            DEFAULT_END_DATE
        )

def load_indicator(
        indicator_name: str,
        indicator_url_template: str,
        granularity: str,
        start_date: datetime,
        end_date: datetime
    ) -> None:
    indicator_url = indicator_url_template.format(
        start_date_dd_mm_yyyy=start_date.strftime('%d/%m/%Y'),
        end_date_dd_mm_yyyy=end_date.strftime('%d/%m/%Y')
    )
    response = requests.get(indicator_url)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data from {indicator_url}")
    data = response.json()
    df = pd.DataFrame(data)
    connector = PostgresConnector.create_from_dot_env_file()
    with connector:
        connector.save_dataframe(df, f"central_bank_{indicator_name}_{granularity}", "raw", "replace")

if __name__ == "__main__":
    main()