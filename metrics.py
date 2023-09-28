import httpx
import logging

logging.basicConfig(level=logging.DEBUG)
logging.getLogger("httpx").setLevel(logging.WARNING)


class Metric:
    def __init__(self, base_url, endpoint, api_key, processor,
                 metric_name=None, params=None, headers=None,
                 asset_name=None, df_col_name=None):
        self.api_key = api_key
        self.df_col_name = df_col_name
        self.metric_name = f"{asset_name}_{metric_name}" if asset_name else metric_name
        self.url = f"{base_url}/{endpoint}/{metric_name}" if metric_name else f"{base_url}/{endpoint}"
        self.params = params
        self.processor = processor
        self.headers = headers

    async def fetch_data(self):
        """
        Fetches data from the API
        :return: dict with the metric name as key and the processed data as value
        """
        logging.info(f'Fetching data for {self.metric_name} from {self.url}')
        if self.headers:
            async with httpx.AsyncClient() as client:
                response = await client.get(self.url, headers=self.headers, params=self.params)

        else:
            async with httpx.AsyncClient() as client:
                response = await client.get(self.url, params=self.params)

        if response.status_code == 200:
            raw_data = response.json()
            processed_data = self.processor.process(data=raw_data, metric_name=self.df_col_name)
            return {self.df_col_name: processed_data}
        else:
            raise ValueError(f'Error fetching data from API: {response.status_code}')
