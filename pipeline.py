import asyncio
import aiohttp
import pandas as pd
import numpy as np
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Web3DataPipeline:
    def __init__(self, pairs: list):
        self.pairs = pairs
        self.base_url = "https://api.coingecko.com/api/v3"
        
    async def fetch_market_data(self, session: aiohttp.ClientSession, pair: str) -> dict:
        """Asynchroniczne pobieranie danych rynkowych dla wybranej pary"""
        url = f"{self.base_url}/coins/{pair}/market_chart?vs_currency=usd&days=1&interval=hourly"
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    logging.info(f"Successfully fetched data for {pair}")
                    return {pair: data}
                else:
                    logging.error(f"Error fetching {pair}: Status {response.status}")
                    return {pair: None}
        except Exception as e:
            logging.error(f"Exception during fetch for {pair}: {str(e)}")
            return {pair: None}

    async def run_pipeline(self) -> pd.DataFrame:
        """Uruchomienie asynchronicznego potoku zbierania danych"""
        async with aiohttp.ClientSession() as session:
            tasks = [self.fetch_market_data(session, pair) for pair in self.pairs]
            results = await asyncio.gather(*tasks)
            
            combined_data = {}
            for res in results:
                combined_data.update(res)
                
            return self.process_blockchain_data(combined_data)

    def process_blockchain_data(self, raw_data: dict) -> pd.DataFrame:
        """Przetwarzanie i walidacja danych przy użyciu Pandas i NumPy"""
        processed_records = []
        
        for pair, data in raw_data.items():
            if not data or 'prices' not in data:
                continue
                
            df_prices = pd.DataFrame(data['prices'], columns=['timestamp', 'price'])
            df_volumes = pd.DataFrame(data['total_volumes'], columns=['timestamp', 'volume'])
            
            
            df = pd.merge(df_prices, df_volumes, on='timestamp')
            df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
            df['token_pair'] = pair
            
            volume_mean = df['volume'].mean()
            volume_std = df['volume'].std()
            
            if volume_std > 0:
                df['volume_z_score'] = (df['volume'] - volume_mean) / volume_std
            else:
                df['volume_z_score'] = 0.0
                
            df['is_anomaly'] = np.where(df['volume_z_score'] > 2.0, True, False)
            
            processed_records.append(df)
            
        if processed_records:
            final_df = pd.concat(processed_records, ignore_index=True)
            return final_df
        return pd.DataFrame()

if __name__ == "__main__":
    target_tokens = ['ethereum', 'solana', 'wrapped-bitcoin']
    
    pipeline = Web3DataPipeline(pairs=target_tokens)
    
    logging.info("Starting Web3 Data Pipeline...")
    loop = asyncio.get_event_loop()
    result_df = loop.run_until_complete(pipeline.run_pipeline())
    
    if not result_df.empty:
        print("\n--- PROCESSED WEB3 DATA SNAPSHOT ---")
        print(result_df[['datetime', 'token_pair', 'price', 'volume_z_score', 'is_anomaly']].tail(10))
    else:
        print("Pipeline finished with empty dataset.")
